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
//   - `peerSubscribed` / `peerCancelled`: SUBSCRIBE and CANCEL received from the peer (PUB).
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

    let loop: any EventLoop
    let options: SocketOptions
    let behavior: SocketBehavior

    private(set) var phase = Phase.open
    /// Pipes in creation order; the order is used for round-robin.
    private(set) var pipes: [Pipe] = []
    private var nextPipeID = 0
    private var listeners: [any Channel] = []
    private var handlers: [ObjectIdentifier: ConnectionHandler] = [:]
    private var receiveWaiters = CircularBuffer<Waiter<[ByteBuffer]>>()
    private var sendWaiters = CircularBuffer<Waiter<Void>>()
    private var loadBalanceCursor = 0
    private var fairQueueCursor = 0
    private var closeContinuations: [CheckedContinuation<Void, Never>] = []
    private var lingerTimer: Scheduled<Void>?
    /// A task waiting in `close()` was cancelled: do not wait for linger.
    private var lingerAborted = false

    init(loop: any EventLoop, options: SocketOptions, behavior: SocketBehavior) {
        self.loop = loop
        self.options = options
        self.behavior = behavior
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
        let address: Endpoint.TCP
        do {
            address = try Endpoint.tcpForBind(endpoint)
        } catch {
            return loop.makeFailedFuture(error)
        }

        let core = self
        return ServerBootstrap(group: loop)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .childChannelOption(ChannelOptions.autoRead, value: false)
            .childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        ConnectionHandler(core: core, connectPipeID: nil))
                }
            }
            .bind(host: address.host, port: address.port)
            .assumeIsolated()
            .flatMapThrowing { channel in
                guard self.phase == .open else {
                    channel.close(promise: nil)
                    throw ZMQError.socketClosed
                }
                self.listeners.append(channel)
                guard let local = channel.localAddress, let host = local.ipAddress, let port = local.port else {
                    throw ZMQError.bindFailed(endpoint: endpoint, reason: "no local address")
                }
                return "tcp://\(host):\(port)"
            }
            .flatMapErrorThrowing { error in
                if error is ZMQError { throw error }
                throw ZMQError.bindFailed(endpoint: endpoint, reason: String(describing: error))
            }
            .nonisolated()
    }

    /// Creates the pipe for `endpoint` and starts connecting. Returns without waiting for the connection.
    func connect(_ endpoint: String) throws {
        loop.preconditionInEventLoop()
        guard phase == .open else { throw ZMQError.socketClosed }
        let address = try Endpoint.tcpForConnect(endpoint)
        let pipe = Pipe(core: self, id: makePipeID(), origin: .connect(host: address.host, port: address.port))
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
            for listener in listeners { listener.close(promise: nil) }
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
        guard let id = connectPipeID else { return phase == .open }
        // Only the attempt this pipe is waiting for may take it (not an extra Happy Eyeballs channel).
        guard let pipe = pipes.first(where: { $0.id == id }), case .connecting = pipe.link else { return false }
        pipe.link = .linked(handler)
        handler.attach(pipe)
        return true
    }

    /// The ZMTP handshake finished. Returns false to have the connection closed.
    func handshakeCompleted(_ handler: ConnectionHandler, peer: Metadata) -> Bool {
        guard phase == .open || phase == .lingering else { return false }
        if let pipe = handler.pipe {
            // Connect side: the pipe has been here since connect().
            guard behavior.pipeAttached(pipe, peer: peer) else { return false }
            pipe.peer = peer
            pipe.reconnectDelay = options.reconnectInterval.timeAmount
            handler.markAttached()
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
            pipe.inbound.append(message)
        }
    }

    /// The connection ended (or never came up after the TCP connect).
    func connectionClosed(_ handler: ConnectionHandler) {
        guard let pipe = handler.pipe else { return }
        if pipe.isConnectSide {
            guard case .linked(let current) = pipe.link, current === handler else { return }
            pipe.link = .idle
            if handler.isAttached { behavior.pipeDetached(pipe) }
            scheduleReconnect(pipe)
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
                pipe.handler?.resumeReadIfNeeded()
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
            pipe.handler?.resumeReadIfNeeded()
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
        guard case .connect(let host, let port) = pipe.origin, phase == .open || phase == .lingering else { return }
        pipe.link = .connecting
        let core = self
        let pipeID = pipe.id
        ClientBootstrap(group: loop)
            .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .channelOption(ChannelOptions.autoRead, value: false)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(
                        ConnectionHandler(core: core, connectPipeID: pipeID))
                }
            }
            .connect(host: host, port: port)
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
        for pipe in pipes {
            if case .backoff(let timer) = pipe.link { timer.cancel() }
            pipe.link = .stopped
        }
        for listener in listeners {
            listener.close(promise: nil)
            closing.append(listener.closeFuture)
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
        handlers.removeAll()
        let continuations = closeContinuations
        closeContinuations.removeAll()
        for continuation in continuations { continuation.resume() }
    }
}
