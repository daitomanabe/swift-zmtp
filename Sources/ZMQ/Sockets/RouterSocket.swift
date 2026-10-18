import NIOCore
import ZMTP

/// Talks to many peers at once and addresses each by a routing ID: `receive()` says which peer a
/// message came from, `send(_:to:)` picks the peer to send to.
public final class RouterSocket: ZMQSocket {
    private let engine: SocketEngine

    public var options: SocketOptions { engine.options }

    public init(context: ZMQContext = .default, options: SocketOptions = SocketOptions()) {
        engine = SocketEngine(context: context, options: options, behavior: RouterBehavior())
    }

    @discardableResult
    public func bind(_ endpoint: String) async throws -> String { try await engine.bind(endpoint) }

    public func connect(_ endpoint: String) async throws { try await engine.connect(endpoint) }

    public func close() async { await engine.close() }

    public func events() -> AsyncStream<SocketEvent> { engine.events() }

    /// Waits for the next message from any peer, taking the peers in turn. The routing ID is the
    /// peer's `routingID` option if it set one; otherwise the socket makes one up (5 bytes, the first
    /// is 0). `message` holds the frames as the peer sent them: a REQ peer's message therefore starts
    /// with an empty frame, and a reply to it must start with one as well. Cancelling the task ends the
    /// wait with `CancellationError` and loses no message.
    public func receive() async throws -> RoutedMessage {
        var frames = try await engine.receive()
        let routingID = RoutingID(Frame(buffer: frames.removeFirst()))
        return RoutedMessage(routingID: routingID, message: Message(wireFrames: frames))
    }

    /// Like `receive()`, but gives up after `timeout`: it throws `ZMQError.timedOut` and takes no
    /// message, so a later `receive` still gets what arrives next. A message that arrives at the deadline
    /// is returned, not lost. Cancelling the task ends the call with `CancellationError`.
    public func receive(timeout: Duration) async throws -> RoutedMessage {
        try await withTimeLimit(timeout) { try await self.receive() }
    }

    /// Sends `message` to the peer with `routingID`. A routing ID nobody has, or a peer at its send
    /// high-water mark, drops the message silently; this call never waits and does not throw for it.
    ///
    /// With `options.routerMandatory` it reports instead: an unknown routing ID throws
    /// `ZMQError.hostUnreachable`, and a peer at its send high-water mark makes the call wait until it
    /// has room (cancelling the task ends the wait with `CancellationError` and sends nothing). Calls are
    /// served in order, so a send that waits holds up the ones behind it, also those for other peers.
    public func send(_ message: Message, to routingID: RoutingID) async throws {
        try await engine.send([routingID.frame.buffer] + message.wireFrames())
    }

    /// Like `send(_:to:)`, but gives up after `timeout`: it throws `ZMQError.timedOut` and queues
    /// nothing. Only a send that has to wait can time out, which means a peer at its send high-water mark
    /// with `options.routerMandatory`; every other send returns at once. Cancelling the task ends the call
    /// with `CancellationError`.
    public func send(_ message: Message, to routingID: RoutingID, timeout: Duration) async throws {
        try await withTimeLimit(timeout) { try await self.send(message, to: routingID) }
    }

    /// Every message until the socket is closed.
    public var messages: MessageSequence<RoutedMessage> {
        MessageSequence { try await self.receive() }
    }
}

final class RouterBehavior: SocketBehavior {
    private var pipesByID: [ByteBuffer: Pipe] = [:]
    private var idsByPipe: [Int: ByteBuffer] = [:]
    /// Next number for a generated routing ID; starts at a random value.
    private var nextGenerated = UInt32.random(in: .min ... .max)

    init() { super.init(socketType: .router) }

    /// Registers the peer's routing ID: its Identity, or a generated one when that is empty. An Identity
    /// that another connection already uses refuses the new connection.
    override func pipeAttached(_ pipe: Pipe, peer: Metadata) -> Bool {
        unregister(pipe)
        var id: ByteBuffer
        if let identity = peer.identity, identity.readableBytes > 0 {
            id = ByteBuffer(bytes: identity.readableBytesView)
            if pipesByID[id] != nil { return false }
        } else {
            repeat { id = generatedID() } while pipesByID[id] != nil
        }
        pipesByID[id] = pipe
        idsByPipe[pipe.id] = id
        return true
    }

    override func pipeDetached(_ pipe: Pipe) { unregister(pipe) }

    override func pipeRemoved(_ pipe: Pipe) { unregister(pipe) }

    private func unregister(_ pipe: Pipe) {
        guard let id = idsByPipe.removeValue(forKey: pipe.id) else { return }
        if pipesByID[id] === pipe { pipesByID[id] = nil }
    }

    /// 0x00 followed by a big-endian counter.
    private func generatedID() -> ByteBuffer {
        var id = ByteBuffer()
        id.writeInteger(UInt8(0))
        id.writeInteger(nextGenerated)
        nextGenerated &+= 1
        return id
    }

    /// `frames[0]` is the routing ID, the rest is the message. Unknown ID or full pipe: dropped, or with
    /// `routerMandatory` an error for an unknown ID and a wait for a full pipe.
    override func trySend(_ frames: [ByteBuffer]) throws -> SendOutcome {
        guard frames.count >= 2 else { return .accepted }
        let mandatory = core.options.routerMandatory
        guard let pipe = pipesByID[frames[0]] else {
            if mandatory { throw ZMQError.hostUnreachable(RoutingID(Frame(buffer: frames[0]))) }
            return .accepted
        }
        guard pipe.canAccept else { return mandatory ? .wouldBlock : .accepted }
        pipe.enqueue(Array(frames.dropFirst()))
        return .accepted
    }

    /// Without `routerMandatory` a send never waits (an unknown ID or a full pipe drops the message), so
    /// credits are fine. With it, the answer depends on the pipe the message is for: slow path only.
    override func sendCapacity() -> Int { core.options.routerMandatory ? 0 : SocketCore.fixedSendCapacity }

    override var usesReceiveQueue: Bool { true }

    /// The routing ID of the source pipe goes in front of the frames.
    override func received(_ frames: [ByteBuffer], from pipe: Pipe) -> InboundMessage? {
        guard let id = idsByPipe[pipe.id] else { return nil }
        return InboundMessage(frames: [id] + frames)
    }
}
