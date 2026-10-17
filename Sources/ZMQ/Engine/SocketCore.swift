// The socket engine.
//
// One `SocketCore` runs one socket. The socket picks an event loop when it is created; the core, its
// pipes and every connection live on that loop, so none of this code takes a lock. The public socket
// classes reach the core only through `SocketEngine`, which hops onto the loop for every call.
//
// How a socket type plugs in
// --------------------------
// A type is a subclass of `SocketBehavior` (SocketBehavior.swift). The core owns everything shared:
// listeners, connect/reconnect, handshakes (ZMTPConnection), pipes with their send and receive queues,
// high-water marks, write batching, the FIFO of blocked `send` and `receive` calls with cancellation,
// and close with linger. The behavior decides only what differs between types:
//   - `trySend(frames)`: pick the outgoing pipe(s) and `Pipe.enqueue` the message. One pipe in turn is
//     `core.nextLoadBalancedPipe()`; all matching pipes, or one chosen by key, is a loop over
//     `core.pipes`. Return `.wouldBlock` to make the caller wait (PUSH, DEALER, REQ) or `.accepted`
//     after dropping on purpose (PUB, ROUTER). The engine calls `trySend` again for parked senders,
//     in order, when a pipe gains room or a pipe appears.
//   - `received(frames, from:)`: turn a received message into what `receive()` returns, or drop it.
//     The queued items are read in fair-queue order (one pipe after the other). `delivered` runs when
//     the application takes a message; `beforeReceive` can refuse a `receive()` (REP state check).
//   - `pipeCreated`, `pipeAttached(pipe, peer:)`, `pipeDetached`, `pipeRemoved`: per-pipe hooks. After
//     every handshake `pipeAttached` runs (SUB re-sends its subscriptions with `Pipe.writeSubscription`,
//     ROUTER registers the peer Identity, PAIR refuses a second peer by returning false).
//   - `peerSubscribed` / `peerCancelled`: SUBSCRIBE and CANCEL received from the peer (PUB, XPUB).
//   - `queueInbound(_:)`: a type can put an item of its own into the receive queue, one that did not arrive
//     as a message (XPUB reports "first subscriber" and "last subscriber left" this way).
// Transports: a pipe talks to its peer through a `PipeConnection` (PipeConnection.swift). TCP and IPC
// connections are NIO channels driven by `ConnectionHandler`, which also keeps the handshake timeout and
// heartbeat clocks. An inproc link has no channel: `InprocConnection` passes messages to the other
// socket's event loop, and the core links two sockets through the context's `InprocRegistry`. The core
// reports what happens to connections as `SocketEvent`s (`events`).
// The public class of the type creates a `SocketEngine` with its behavior and exposes the typed
// `send` / `receive` calls on top of `engine.send(frames)` and `engine.receive()`.

import NIOCore
import NIOPosix
import ZMTP

/// Runs one socket on one event loop.
///
/// `@unchecked Sendable`: all mutable state is touched only on `loop`. The entry points that other
/// threads reach (through `loop.execute`) and every NIO callback start with
/// `loop.preconditionInEventLoop()` or run in a context NIO already pins to `loop`.
final class SocketCore: @unchecked Sendable {
    enum Phase {
        /// Normal operation.
        case open
        /// `close()` was called; waiting for queued messages to be written, up to `linger`.
        case lingering
        /// Closing channels.
        case tearingDown
        case closed
    }

    /// A bound endpoint: the listening channel and, for IPC, the socket file it created.
    ///
    /// `@unchecked Sendable`: the NIO child initializer captures it, but it is only touched on the loop.
    final class Listener: @unchecked Sendable {
        /// The endpoint actually bound (set when the bind has succeeded).
        var endpoint = ""
        var channel: (any Channel)?
        private var file: (path: String, identity: Endpoint.FileIdentity?)?

        /// Remembers the socket file just created at `path`, to remove it when the listener closes.
        func ownFile(at path: String) {
            file = (path, Endpoint.socketFileIdentity(at: path))
        }

        /// Stops listening and removes the socket file we created (not one another socket put there since).
        func close() {
            channel?.close(promise: nil)
            guard let file else { return }
            self.file = nil
            Endpoint.removeSocketFile(at: file.path, ifIdentity: file.identity)
        }
    }

    let loop: any EventLoop
    let options: SocketOptions
    let behavior: SocketBehavior
    /// `options.conflate` is set and the socket type honors it (PUSH, PULL, PUB, SUB, DEALER): every
    /// pipe keeps only the newest message in its send queue and in its receive queue, without a
    /// high-water mark.
    let conflates: Bool
    /// Where `SocketEvent`s go; `events()` of the public sockets makes streams from it.
    let events = SocketEventHub()

    private(set) var phase = Phase.open
    /// Pipes in creation order; the order is used for round-robin.
    private(set) var pipes: [Pipe] = []
    private var nextPipeID = 0
    private var listeners: [Listener] = []
    private var handlers: [ObjectIdentifier: ConnectionHandler] = [:]
    // Inproc state (SocketCore+Inproc.swift). Internal so the extension in that file can use it.
    /// The `inproc://` names of the context; shared with the other sockets of the context.
    let inproc: InprocRegistry
    /// Names this socket has bound (the registry holds them for us until `close()`).
    var inprocNames: [String] = []
    /// Inproc links that are up or being set up, until they end.
    var inprocLinks: [ObjectIdentifier: InprocConnection] = [:]
    private var receiveWaiters = CircularBuffer<Waiter<[ByteBuffer]>>()
    /// Items the behavior queued itself (`queueInbound`); read before the messages of the pipes.
    private var queuedInbound = CircularBuffer<SocketBehavior.InboundMessage>()
    private var sendWaiters = CircularBuffer<Waiter<Void>>()
    private var loadBalanceCursor = 0
    private var fairQueueCursor = 0
    private var closeContinuations: [CheckedContinuation<Void, Never>] = []
    private var lingerTimer: Scheduled<Void>?
    /// A task waiting in `close()` was cancelled: do not wait for linger.
    private var lingerAborted = false

    init(loop: any EventLoop, options: SocketOptions, behavior: SocketBehavior, inproc: InprocRegistry) {
        self.loop = loop
        self.options = options
        self.behavior = behavior
        self.inproc = inproc
        switch behavior.socketType {
        case .push, .pull, .pub, .sub, .dealer: self.conflates = options.conflate
        case .pair, .req, .rep, .router, .xpub, .xsub: self.conflates = false
        }
        behavior.core = self
    }

    var sendLimit: Int { options.sendHighWaterMark > 0 ? options.sendHighWaterMark : .max }
    var receiveLimit: Int { options.receiveHighWaterMark > 0 ? options.receiveHighWaterMark : .max }

    // MARK: - Helpers for socket types

    /// The next pipe in round-robin order that can take a message, or nil when all are full or absent.
    /// Connect-side pipes count from `connect()` on, before their handshake.
    func nextLoadBalancedPipe() -> Pipe? {
        let count = pipes.count
        guard count > 0 else { return nil }
        for step in 0..<count {
            let index = (loadBalanceCursor + step) % count
            if pipes[index].canAccept {
                loadBalanceCursor = index + 1
                return pipes[index]
            }
        }
        return nil
    }

    /// Puts an item that is not a received message into the receive queue (XPUB's subscription notices).
    /// It goes to a waiting `receive()` at once, else it waits in a queue of its own that is read before
    /// the pipes' messages. Such items do not count against `receiveHighWaterMark` (the connection must
    /// keep reading subscriptions even when the application is slow) and `delivered` is not called for
    /// them. Ignored once the socket is closing. Call on the loop.
    func queueInbound(_ item: SocketBehavior.InboundMessage) {
        loop.preconditionInEventLoop()
        guard phase == .open else { return }
        if let waiter = receiveWaiters.popFirst() {
            // A waiting receive() means every queue is empty, so this item is the next in line.
            waiter.succeed(item.frames)
        } else {
            queuedInbound.append(item)
        }
    }

    // MARK: - Calls from the application (arrive through loop.execute)

    func registerSend(_ waiter: Waiter<Void>, _ continuation: CheckedContinuation<Void, any Error>) {
        loop.preconditionInEventLoop()
        guard waiter.begin(continuation) else { return }
        guard phase == .open else {
            waiter.fail(ZMQError.socketClosed)
            return
        }
        // Senders are served in order: a new one does not overtake those already waiting.
        guard sendWaiters.isEmpty else {
            sendWaiters.append(waiter)
            return
        }
        do {
            switch try behavior.trySend(waiter.payload) {
            case .accepted: waiter.succeed(())
            case .wouldBlock: sendWaiters.append(waiter)
            }
        } catch {
            waiter.fail(error)
        }
    }

    func cancelSend(_ waiter: Waiter<Void>) {
        loop.preconditionInEventLoop()
        if waiter.cancel() {
            sendWaiters.removeAll { $0 === waiter }
        }
    }

    func registerReceive(
        _ waiter: Waiter<[ByteBuffer]>, _ continuation: CheckedContinuation<[ByteBuffer], any Error>
    ) {
        loop.preconditionInEventLoop()
        guard waiter.begin(continuation) else { return }
        guard phase == .open else {
            waiter.fail(ZMQError.socketClosed)
            return
        }
        do {
            try behavior.beforeReceive()
        } catch {
            waiter.fail(error)
            return
        }
        // If anything is queued there are no waiters, so serving this call first keeps the order.
        if let message = dequeueInbound() {
            waiter.succeed(message.frames)
        } else {
            receiveWaiters.append(waiter)
        }
    }

    func cancelReceive(_ waiter: Waiter<[ByteBuffer]>) {
        loop.preconditionInEventLoop()
        if waiter.cancel() {
            receiveWaiters.removeAll { $0 === waiter }
        }
    }

    /// Starts listening. The future holds the endpoint actually bound.
    func bind(_ endpoint: String) -> EventLoopFuture<String> {
        loop.preconditionInEventLoop()
        guard phase == .open else { return loop.makeFailedFuture(ZMQError.socketClosed) }
        let target: Endpoint.BindTarget
        do {
            target = try Endpoint.forBind(endpoint)
        } catch {
            return loop.makeFailedFuture(error)
        }
        if case .inproc(let name) = target { return bindInproc(name, endpoint: endpoint) }

        let core = self
        let listener = Listener()
        let bootstrap = ServerBootstrap(group: loop)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .childChannelOption(ChannelOptions.autoRead, value: false)
            .childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        ConnectionHandler(core: core, source: .accept(listener)))
                }
            }
        let binding: EventLoopFuture<any Channel>
        switch target {
        case .tcp(let address):
            var tcp =
                bootstrap
                .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .childChannelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            if let idle = options.tcpKeepAliveIdleSeconds {
                tcp =
                    tcp
                    .childChannelOption(ChannelOptions.socketOption(.so_keepalive), value: 1)
                    .childChannelOption(ChannelOptions.tcpOption(.tcpKeepAliveIdle), value: idle)
            }
            binding = tcp.bind(host: address.host, port: address.port)
        case .ipc(let path):
            // A file left by an earlier run would make bind fail with "address in use".
            Endpoint.removeStaleFile(at: path)
            binding = bootstrap.bind(unixDomainSocketPath: path, cleanupExistingSocketFile: false)
        case .inproc:
            preconditionFailure("an inproc name has no channel")
        }

        return binding
            .assumeIsolated()
            .flatMapThrowing { channel in
                listener.channel = channel
                if case .ipc(let path) = target { listener.ownFile(at: path) }
                guard self.phase == .open else {
                    listener.close()
                    throw ZMQError.socketClosed
                }
                switch target {
                case .tcp:
                    guard let local = channel.localAddress, let host = local.ipAddress, let port = local.port else {
                        listener.close()
                        throw ZMQError.bindFailed(endpoint: endpoint, reason: "no local address")
                    }
                    listener.endpoint = "tcp://\(host):\(port)"
                case .ipc(let path):
                    listener.endpoint = "ipc://\(path)"
                case .inproc:
                    preconditionFailure("an inproc name has no channel")
                }
                self.listeners.append(listener)
                self.events.emit(.listening(endpoint: listener.endpoint))
                return listener.endpoint
            }
            .flatMapErrorThrowing { error in
                if error is ZMQError { throw error }
                let reason = String(describing: error)
                self.events.emit(.bindFailed(endpoint: endpoint, reason: reason))
                throw ZMQError.bindFailed(endpoint: endpoint, reason: reason)
            }
            .nonisolated()
    }

    /// Creates the pipe for `endpoint` and starts connecting. Returns without waiting for the connection.
    func connect(_ endpoint: String) throws {
        loop.preconditionInEventLoop()
        guard phase == .open else { throw ZMQError.socketClosed }
        let target = try Endpoint.forConnect(endpoint)
        let pipe = Pipe(core: self, id: makePipeID(), origin: .connect(target, endpoint: endpoint))
        try behavior.pipeCreated(pipe)
        pipes.append(pipe)
        connectNow(pipe)
        retrySendWaiters()
    }

    /// `close()`: stop accepting, fail waiting calls, let queued messages out (up to linger), then close.
    func close(_ continuation: CheckedContinuation<Void, Never>) {
        loop.preconditionInEventLoop()
        switch phase {
        case .closed:
            continuation.resume()
        case .tearingDown:
            closeContinuations.append(continuation)
        case .lingering:
            closeContinuations.append(continuation)
            if lingerAborted { beginTeardown() }
        case .open:
            closeContinuations.append(continuation)
            phase = .lingering
            failWaiters()
            for listener in listeners { listener.close() }
            unbindInprocNames()
            if lingerAborted || options.linger.map({ $0 <= .zero }) == true {
                beginTeardown()
            } else if !hasPendingOutput {
                beginTeardown()
            } else if let linger = options.linger {
                lingerTimer = loop.assumeIsolated().scheduleTask(in: linger.timeAmount) { [self] in
                    lingerTimer = nil
                    beginTeardown()
                }
            }
        }
    }

    /// The task that called `close()` was cancelled: do not wait for linger any longer.
    func abortLinger() {
        loop.preconditionInEventLoop()
        lingerAborted = true
        if phase == .lingering { beginTeardown() }
    }

    /// The socket object was released without `close()`: close now, without linger.
    func shutdownNow() {
        loop.preconditionInEventLoop()
        guard phase == .open || phase == .lingering else { return }
        failWaiters()
        beginTeardown()
    }

    // MARK: - Calls from connection handlers (already on the loop)

    func handlerAdded(_ handler: ConnectionHandler) {
        handlers[ObjectIdentifier(handler)] = handler
    }

    func handlerRemoved(_ handler: ConnectionHandler) {
        handlers[ObjectIdentifier(handler)] = nil
    }

    /// The TCP connection is up. Returns false to have it closed.
    func connectionActive(_ handler: ConnectionHandler, connectPipeID: Int?) -> Bool {
        guard phase == .open || phase == .lingering else { return false }
        guard let id = connectPipeID else {
            guard phase == .open else { return false }
            events.emit(.accepted(endpoint: handler.endpoint))
            return true
        }
        // Only the attempt this pipe is waiting for may take it (not an extra Happy Eyeballs channel).
        guard let pipe = pipes.first(where: { $0.id == id }), case .connecting = pipe.link else { return false }
        pipe.link = .linked(handler)
        handler.attach(pipe)
        events.emit(.connected(endpoint: handler.endpoint))
        return true
    }

    /// The handshake finished (ZMTP, or the type check of an inproc link). Returns false to have the
    /// connection closed.
    func handshakeCompleted(_ handler: any PipeConnection, peer: Metadata) -> Bool {
        guard phase == .open || phase == .lingering else { return false }
        if let pipe = handler.pipe {
            // Connect side: the pipe has been here since connect().
            guard behavior.pipeAttached(pipe, peer: peer) else { return false }
            pipe.peer = peer
            pipe.reconnectDelay = options.reconnectInterval.timeAmount
            handler.markAttached()
            events.emit(.handshakeSucceeded(endpoint: handler.endpoint, peerSocketType: peer.socketType ?? ""))
            pipe.scheduleFlush()
            retrySendWaiters()
            return true
        }
        // Bind side: the connection becomes a pipe now.
        guard phase == .open else { return false }
        let pipe = Pipe(core: self, id: makePipeID(), origin: .bind)
        // Linked before the hook runs, so the hook can write to the connection (SUB sends its subscriptions).
        pipe.link = .linked(handler)
        guard behavior.pipeAttached(pipe, peer: peer) else {
            pipe.link = .stopped
            return false
        }
        pipe.peer = peer
        pipe.link = .linked(handler)
        handler.attach(pipe)
        handler.markAttached()
        pipes.append(pipe)
        events.emit(.handshakeSucceeded(endpoint: handler.endpoint, peerSocketType: peer.socketType ?? ""))
        retrySendWaiters()
        return true
    }

    /// A message arrived on `pipe`.
    func messageReceived(_ frames: [ByteBuffer], on pipe: Pipe) {
        guard phase == .open, !pipe.isTerminated else { return }
        guard let message = behavior.received(frames, from: pipe) else { return }
        if let waiter = receiveWaiters.popFirst() {
            // A waiting receive() means every pipe's queue is empty, so this message is the next in line.
            behavior.delivered(message, from: pipe)
            waiter.succeed(message.frames)
        } else {
            // A conflating socket keeps the newest message only.
            if conflates { pipe.inbound.removeAll(keepingCapacity: true) }
            pipe.inbound.append(message)
        }
    }

    /// The connection ended (or never came up after the TCP connect).
    func connectionClosed(_ handler: any PipeConnection) {
        if handler.isEstablished { events.emit(.disconnected(endpoint: handler.endpoint)) }
        if let link = handler as? InprocConnection { inprocLinks[ObjectIdentifier(link)] = nil }
        guard let pipe = handler.pipe else { return }
        if pipe.isConnectSide {
            guard case .linked(let current) = pipe.link, current === handler else { return }
            pipe.link = .idle
            // `immediate`: what was queued for this connection is not kept for the next one.
            if options.immediate { pipe.outbound.removeAll() }
            if handler.wasAttached {
                behavior.pipeDetached(pipe)
                // A parked send may be aimed at the peer that just went (ROUTER with `routerMandatory`).
                retrySendWaiters()
            }
            if let link = handler as? InprocConnection, link.waitsForBind {
                // The bound socket went away: wait for the next bind of the name (no timer).
                connectNow(pipe)
            } else {
                scheduleReconnect(pipe)
            }
        } else {
            terminate(pipe)
        }
        lingerCheck()
    }

    /// A write handed to a connection completed (or failed); there may be room again.
    func pipeWriteCompleted(_ pipe: Pipe) {
        retrySendWaiters()
        lingerCheck()
    }

    /// Whether a connection of `pipe` should read more from the network now.
    func mayRead(from pipe: Pipe) -> Bool {
        phase == .open && !pipe.isTerminated && pipe.hasInboundRoom
    }

    // MARK: - Pipes

    private func makePipeID() -> Int {
        defer { nextPipeID += 1 }
        return nextPipeID
    }

    /// A bind-side pipe is over: it takes no more sends, its unsent messages are dropped, and it goes
    /// away once the messages it already received have been read.
    private func terminate(_ pipe: Pipe) {
        guard !pipe.isTerminated else { return }
        pipe.isTerminated = true
        pipe.outbound.removeAll()
        pipe.link = .stopped
        behavior.pipeRemoved(pipe)
        if pipe.inbound.isEmpty { remove(pipe) }
        retrySendWaiters()
    }

    private func remove(_ pipe: Pipe) {
        pipes.removeAll { $0 === pipe }
    }

    /// The next received message in fair-queue order (round-robin over the pipes that have one).
    private func dequeueInbound() -> SocketBehavior.InboundMessage? {
        if let item = queuedInbound.popFirst() { return item }
        let count = pipes.count
        guard count > 0 else { return nil }
        for step in 0..<count {
            let index = (fairQueueCursor + step) % count
            let pipe = pipes[index]
            guard !pipe.inbound.isEmpty else { continue }
            let message = pipe.inbound.removeFirst()
            fairQueueCursor = index + 1
            behavior.delivered(message, from: pipe)
            if pipe.isTerminated && pipe.inbound.isEmpty {
                remove(pipe)
            } else {
                pipe.connection?.resumeReadIfNeeded()
            }
            return message
        }
        return nil
    }

    /// Throws away the messages received on `pipe` that nobody has read yet (REQ drops an unwanted reply).
    func discardInbound(of pipe: Pipe) {
        loop.preconditionInEventLoop()
        pipe.inbound.removeAll()
        if pipe.isTerminated {
            remove(pipe)
        } else {
            pipe.connection?.resumeReadIfNeeded()
        }
    }

    // MARK: - Blocked senders

    /// Serves parked senders in order for as long as the type accepts their messages. A type whose state
    /// changes without a pipe event (REQ going back to idle) calls this itself.
    func retrySendWaiters() {
        while phase == .open, let waiter = sendWaiters.first {
            do {
                switch try behavior.trySend(waiter.payload) {
                case .accepted:
                    sendWaiters.removeFirst()
                    waiter.succeed(())
                case .wouldBlock:
                    return
                }
            } catch {
                sendWaiters.removeFirst()
                waiter.fail(error)
            }
        }
    }

    private func failWaiters() {
        while let waiter = sendWaiters.popFirst() { waiter.fail(ZMQError.socketClosed) }
        while let waiter = receiveWaiters.popFirst() { waiter.fail(ZMQError.socketClosed) }
    }

    // MARK: - Connecting

    private func connectNow(_ pipe: Pipe) {
        guard case .connect(let target, let endpoint) = pipe.origin, phase == .open || phase == .lingering else { return }
        if case .inproc(let name) = target {
            connectInproc(pipe, name: name, endpoint: endpoint)
            return
        }
        pipe.link = .connecting
        let core = self
        let pipeID = pipe.id
        let bootstrap = ClientBootstrap(group: loop)
            .channelOption(ChannelOptions.autoRead, value: false)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        ConnectionHandler(core: core, source: .connect(pipeID: pipeID, endpoint: endpoint)))
                }
            }
        let connecting: EventLoopFuture<any Channel>
        switch target {
        case .tcp(let host, let port):
            var tcp = bootstrap.channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            if let idle = options.tcpKeepAliveIdleSeconds {
                tcp =
                    tcp
                    .channelOption(ChannelOptions.socketOption(.so_keepalive), value: 1)
                    .channelOption(ChannelOptions.tcpOption(.tcpKeepAliveIdle), value: idle)
            }
            connecting = tcp.connect(host: host, port: port)
        case .ipc(let path):
            connecting = bootstrap.connect(unixDomainSocketPath: path)
        case .inproc:
            preconditionFailure("an inproc name has no channel")
        }
        connecting
            .assumeIsolated()
            .whenComplete { result in
                switch result {
                case .success(let channel):
                    // If the socket was torn down while the attempt was running, do not keep the channel.
                    if self.phase == .tearingDown || self.phase == .closed { channel.close(promise: nil) }
                case .failure:
                    if case .connecting = pipe.link { self.scheduleReconnect(pipe) }
                }
            }
    }

    private func scheduleReconnect(_ pipe: Pipe) {
        guard phase == .open || phase == .lingering, !pipe.isTerminated else {
            pipe.link = .stopped
            return
        }
        let delay = pipe.reconnectDelay
        if let endpoint = pipe.connectEndpoint {
            events.emit(.connectRetried(endpoint: endpoint, after: .nanoseconds(delay.nanoseconds)))
        }
        if let maximum = options.reconnectIntervalMax?.timeAmount {
            pipe.reconnectDelay = .nanoseconds(min(delay.nanoseconds &* 2, max(maximum.nanoseconds, delay.nanoseconds)))
        }
        pipe.link = .backoff(
            loop.assumeIsolated().scheduleTask(in: delay) { [self] in
                guard case .backoff = pipe.link else { return }
                connectNow(pipe)
            })
    }

    // MARK: - Closing

    private var hasPendingOutput: Bool {
        pipes.contains { $0.pending > 0 }
    }

    /// While lingering, finish as soon as nothing is left to write.
    private func lingerCheck() {
        if phase == .lingering && !hasPendingOutput { beginTeardown() }
    }

    private func beginTeardown() {
        guard phase == .open || phase == .lingering else { return }
        phase = .tearingDown
        lingerTimer?.cancel()
        lingerTimer = nil
        failWaiters()

        var closing: [EventLoopFuture<Void>] = []
        unbindInprocNames()
        for pipe in pipes {
            if case .backoff(let timer) = pipe.link { timer.cancel() }
            if case .waitingForBind = pipe.link, case .connect(.inproc(let name), _) = pipe.origin {
                inproc.cancelWait(name, core: self, pipeID: pipe.id)
            }
            pipe.link = .stopped
        }
        // Inproc links end at once: the other sockets are told, and each link reports `disconnected`.
        for link in Array(inprocLinks.values) { link.close() }
        for listener in listeners {
            listener.close()
            if let channel = listener.channel { closing.append(channel.closeFuture) }
        }
        listeners.removeAll()
        for handler in Array(handlers.values) {
            guard let channel = handler.channel else { continue }
            channel.close(promise: nil)
            closing.append(channel.closeFuture)
        }
        EventLoopFuture.andAllComplete(closing, on: loop).assumeIsolated().whenComplete { [self] _ in
            finishTeardown()
        }
    }

    private func finishTeardown() {
        phase = .closed
        for pipe in pipes {
            // A bind-side pipe that ended earlier already had pipeRemoved; it only stayed for its inbound.
            let alreadyRemoved = pipe.isTerminated
            pipe.isTerminated = true
            pipe.outbound.removeAll()
            pipe.inbound.removeAll()
            if !alreadyRemoved { behavior.pipeRemoved(pipe) }
        }
        pipes.removeAll()
        queuedInbound.removeAll()
        handlers.removeAll()
        inprocLinks.removeAll()
        events.emit(.closed)
        events.finish()
        let continuations = closeContinuations
        closeContinuations.removeAll()
        for continuation in continuations { continuation.resume() }
    }
}
