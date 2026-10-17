import NIOCore
import ZMTP

/// Drives one stream connection (TCP or IPC): feeds received bytes to a `ZMTPConnection`, writes what
/// it produces, and reports the handshake, messages and the end of the connection to the `SocketCore`.
/// It also keeps the connection's clocks: the handshake timeout and the heartbeats.
///
/// Reading is manual (`autoRead` is off). The handler reads while the handshake runs, and afterwards
/// only while the pipe's received-message queue has room, so a slow `receive()` side pushes back on
/// the sender through the kernel's socket buffers.
final class ConnectionHandler: ChannelInboundHandler, PipeConnection {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    /// Where a connection came from, which gives its endpoint string for events.
    enum Source {
        /// A connect-side connection; `endpoint` is what the application passed to `connect`.
        case connect(pipeID: Int, endpoint: String)
        /// A connection accepted on `listener`.
        case accept(SocketCore.Listener)
    }

    private let core: SocketCore
    private let source: Source
    private var connection: ZMTPConnection

    private(set) var channel: (any Channel)?
    private(set) var pipe: Pipe?
    /// The ZMTP handshake finished.
    private(set) var isReady = false
    /// The core took this connection (`connectionActive` said yes); it reports its end.
    private(set) var isEstablished = false
    /// The socket accepted this connection after the handshake (stays true after the connection ends).
    private(set) var wasAttached = false
    private var started = false
    private var readInFlight = false
    /// `handshakeFailed` has been reported for this connection.
    private var handshakeFailureReported = false
    /// Set while the handler is in a pipeline; the answer of an authentication comes back through it.
    private var handlerContext: ChannelHandlerContext?
    /// Counts the authentications asked for, so an answer can be matched to its question.
    private var authenticationSerial: UInt64 = 0
    /// The authentication whose answer the connection is waiting for.
    private var pendingAuthentication: UInt64?
    private var authenticationTask: Task<Void, Never>?

    // Clocks. All run on the socket's loop and are cancelled when the connection ends.
    private var handshakeTimer: Scheduled<Void>?
    /// Fires when it is time to send the next PING.
    private var pingTimer: Scheduled<Void>?
    /// Armed after a PING; fires if the peer says nothing in `heartbeatTimeout`.
    private var pongTimer: Scheduled<Void>?
    /// Armed by a PING of the peer that carries a TTL; fires if the peer says nothing for that long.
    private var ttlTimer: Scheduled<Void>?

    init(core: SocketCore, source: Source) {
        self.core = core
        self.source = source
        self.connection = ZMTPConnection(
            configuration: .init(
                socketType: core.behavior.socketType,
                routingID: core.options.routingID?.frame.bytes ?? [],
                maxMessageSize: core.options.maxMessageSize,
                mechanism: core.options.security.mechanism
            ))
    }

    /// The pipe ID this connection was made for, if it is a connect-side connection.
    var connectPipeID: Int? {
        if case .connect(let pipeID, _) = source { return pipeID }
        return nil
    }

    // MARK: PipeConnection

    var endpoint: String {
        switch source {
        case .connect(_, let endpoint): endpoint
        case .accept(let listener): listener.endpoint
        }
    }

    var isAttached: Bool { wasAttached && channel?.isActive == true }

    func write(_ messages: [[ByteBuffer]], completed: @escaping () -> Void) {
        guard let channel, channel.isActive else {
            completed()
            return
        }
        var buffer = channel.allocator.buffer(capacity: 1024)
        for frames in messages { connection.encodeMessage(frames, into: &buffer) }
        let promise = channel.eventLoop.makePromise(of: Void.self)
        promise.futureResult.assumeIsolated().whenComplete { _ in completed() }
        channel.writeAndFlush(buffer, promise: promise)
    }

    func writeSubscription(_ topic: ByteBuffer, subscribe: Bool) {
        guard isReady, let channel, channel.isActive else { return }
        var buffer = channel.allocator.buffer(capacity: topic.readableBytes + 16)
        if subscribe {
            connection.encodeSubscribe(topic, into: &buffer)
        } else {
            connection.encodeCancel(topic, into: &buffer)
        }
        channel.writeAndFlush(buffer, promise: nil)
    }

    /// Asks the transport for more bytes if nothing is pending and the pipe can take more messages.
    func resumeReadIfNeeded() {
        guard started, !readInFlight, let channel, channel.isActive else { return }
        if isReady {
            guard let pipe, core.mayRead(from: pipe) else { return }
        }
        readInFlight = true
        channel.read()
    }

    func close() {
        channel?.close(promise: nil)
    }

    // MARK: Called by the core

    func attach(_ pipe: Pipe) {
        self.pipe = pipe
    }

    func markAttached() {
        wasAttached = true
    }

    // MARK: ChannelInboundHandler

    func handlerAdded(context: ChannelHandlerContext) {
        channel = context.channel
        handlerContext = context
        core.handlerAdded(self)
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        cancelTimers()
        abandonAuthentication()
        handlerContext = nil
        core.handlerRemoved(self)
    }

    func channelActive(context: ChannelHandlerContext) {
        guard core.connectionActive(self, connectPipeID: connectPipeID) else {
            context.close(promise: nil)
            return
        }
        isEstablished = true
        started = true
        startHandshakeTimer()
        apply(connection.start(), context: context)
        context.fireChannelActive()
        resumeReadIfNeeded()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        // Any traffic proves the peer is alive: it settles the open question of a PING of ours or a TTL of
        // the peer's.
        pongTimer?.cancel()
        pongTimer = nil
        ttlTimer?.cancel()
        ttlTimer = nil
        apply(connection.receive(unwrapInboundIn(data)), context: context)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        context.flush()
        readInFlight = false
        // The messages of this read go on to the application's receive queue, which frees room in the pipe.
        core.receiveBatchEnded()
        resumeReadIfNeeded()
        context.fireChannelReadComplete()
    }

    func channelInactive(context: ChannelHandlerContext) {
        cancelTimers()
        abandonAuthentication()
        if isEstablished && !isReady {
            reportHandshakeFailure("the connection closed before the handshake finished")
        }
        core.connectionClosed(self)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        context.close(promise: nil)
    }

    // MARK: Output

    private func apply(_ outputs: [ZMTPConnection.Output], context: ChannelHandlerContext) {
        var wrote = false
        for output in outputs {
            switch output {
            case .write(let buffer):
                context.write(wrapOutboundOut(buffer), promise: nil)
                wrote = true
            case .ready(let peer, let minorVersion):
                isReady = true
                handshakeTimer?.cancel()
                handshakeTimer = nil
                guard core.handshakeCompleted(self, peer: peer) else {
                    isReady = false
                    reportHandshakeFailure("the socket refused the connection")
                    context.flush()
                    context.close(promise: nil)
                    return
                }
                if minorVersion >= 1 { startHeartbeat() }
            case .authenticate(let request):
                startAuthentication(request)
            case .message(let frames):
                if let pipe { core.messageReceived(frames, on: pipe) }
            case .subscribe(let topic):
                if let pipe { core.behavior.peerSubscribed(topic, on: pipe) }
            case .cancel(let topic):
                if let pipe { core.behavior.peerCancelled(topic, on: pipe) }
            case .pingReceived(let ttl):
                // The PONG follows as a .write. A TTL promises the next PING (or any traffic) in time.
                if ttl > 0 { startTTLTimer(tenthsOfSecond: ttl) }
            case .closed(let error):
                if !isReady { reportHandshakeFailure(Self.reason(for: error)) }
                // Any ERROR command was emitted as a .write just before.
                context.flush()
                context.close(promise: nil)
                return
            }
        }
        if wrote { context.flush() }
    }

    /// The text of `handshakeFailed` for a ZMTP error.
    private static func reason(for error: ZMTPError) -> String {
        switch error {
        case .mechanismMismatch(let local, let peer):
            "security mechanism mismatch: this socket uses \(local), the peer uses \(peer)"
        case .authenticationFailed:
            "authentication failed"
        case .cryptoError(let detail):
            "CURVE check failed: \(detail)"
        case .peerError(let reason):
            "the peer sent ERROR \(reason)"
        default:
            String(describing: error)
        }
    }

    // MARK: Authentication

    /// A server-side mechanism wants the caller's `authenticate` function to decide about the peer. The
    /// function runs in a Task; its answer is fed back on the socket's loop, if the connection is still
    /// the same one and still open by then. The handshake timer keeps running meanwhile.
    private func startAuthentication(_ request: AuthenticationRequest) {
        switch request {
        case .plain(let username, let password):
            guard let authenticate = core.options.security.plainAuthenticator else {
                // The mechanism comes from the same options, so this does not happen.
                close()
                return
            }
            let credentials = PlainCredentials(username: username, password: password)
            beginAuthentication { await authenticate(credentials) }
        case .curve(let keyBytes):
            guard core.options.security.isCurveServer else {
                close()
                return
            }
            let key = CurvePublicKey(validated: keyBytes)
            // Without an authenticate function every client key is accepted; the question still goes
            // through the same path, so the handshake continues on the loop as for any answer.
            let authenticate = core.options.security.curveAuthenticator
            beginAuthentication {
                if let authenticate { return await authenticate(key) }
                return true
            }
        }
    }

    private func beginAuthentication(_ decide: @escaping @Sendable () async -> Bool) {
        authenticationSerial &+= 1
        let serial = authenticationSerial
        pendingAuthentication = serial
        let loop = core.loop
        let handler = NIOLoopBound(self, eventLoop: loop)
        authenticationTask = Task {
            let allowed = await decide()
            loop.execute { handler.value.authenticationAnswered(serial: serial, allowed: allowed) }
        }
    }

    private func authenticationAnswered(serial: UInt64, allowed: Bool) {
        // A different serial or none: the connection ended (or ended and began again) while the caller thought.
        guard pendingAuthentication == serial else { return }
        pendingAuthentication = nil
        authenticationTask = nil
        guard let context = handlerContext, context.channel.isActive else { return }
        apply(connection.completeAuthentication(allowed: allowed), context: context)
        // Messages the peer sent meanwhile are delivered by now; like after a read, hand them on.
        core.receiveBatchEnded()
        resumeReadIfNeeded()
    }

    /// The connection is over: forget the question, so a late answer is dropped, and cancel the Task.
    private func abandonAuthentication() {
        pendingAuthentication = nil
        authenticationTask?.cancel()
        authenticationTask = nil
    }

    // MARK: Clocks

    private func reportHandshakeFailure(_ reason: String) {
        guard !handshakeFailureReported else { return }
        handshakeFailureReported = true
        core.events.emit(.handshakeFailed(endpoint: endpoint, reason: reason))
    }

    private func cancelTimers() {
        handshakeTimer?.cancel()
        pingTimer?.cancel()
        pongTimer?.cancel()
        ttlTimer?.cancel()
        handshakeTimer = nil
        pingTimer = nil
        pongTimer = nil
        ttlTimer = nil
    }

    private func startHandshakeTimer() {
        guard let timeout = core.options.handshakeTimeout, timeout > .zero else { return }
        handshakeTimer = core.loop.assumeIsolated().scheduleTask(in: timeout.timeAmount) { [self] in
            handshakeTimer = nil
            guard !isReady else { return }
            reportHandshakeFailure("the handshake did not finish within \(timeout)")
            close()
        }
    }

    /// PINGs every `heartbeatInterval`, starting one interval after the handshake.
    private func startHeartbeat() {
        guard let interval = core.options.heartbeatInterval, interval > .zero else { return }
        schedulePing(after: interval)
    }

    private func schedulePing(after interval: Duration) {
        pingTimer = core.loop.assumeIsolated().scheduleTask(in: interval.timeAmount) { [self] in
            pingTimer = nil
            guard isReady, let channel, channel.isActive else { return }
            sendPing(on: channel)
            schedulePing(after: interval)
        }
    }

    private func sendPing(on channel: any Channel) {
        let options = core.options
        var buffer = channel.allocator.buffer(capacity: 16)
        connection.encodePing(
            ttl: options.heartbeatTTL?.tenthsOfSecond ?? 0, context: ByteBuffer(), into: &buffer)
        channel.writeAndFlush(buffer, promise: nil)

        // Wait for the peer, unless an earlier PING is still being waited for.
        guard pongTimer == nil, let timeout = options.heartbeatTimeout ?? options.heartbeatInterval,
            timeout > .zero
        else { return }
        armPongTimer(timeout)
    }

    private func armPongTimer(_ timeout: Duration) {
        pongTimer = core.loop.assumeIsolated().scheduleTask(in: timeout.timeAmount) { [self] in
            pongTimer = nil
            // While we are not reading (the receive queue is full), the peer's answer may be waiting in
            // the kernel; do not blame the peer for our own pause.
            guard !isReadingPaused else {
                armPongTimer(timeout)
                return
            }
            close()
        }
    }

    private func startTTLTimer(tenthsOfSecond: UInt16) {
        let ttl = Duration.milliseconds(Int(tenthsOfSecond) * 100)
        ttlTimer = core.loop.assumeIsolated().scheduleTask(in: ttl.timeAmount) { [self] in
            ttlTimer = nil
            guard !isReadingPaused else {
                startTTLTimer(tenthsOfSecond: tenthsOfSecond)
                return
            }
            close()
        }
    }

    /// True while the handler deliberately does not read because the pipe's receive queue is full.
    private var isReadingPaused: Bool {
        guard isReady, let pipe else { return false }
        return !pipe.hasInboundRoom
    }
}
