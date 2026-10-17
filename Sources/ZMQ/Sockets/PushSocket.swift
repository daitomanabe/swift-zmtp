import NIOCore
import ZMTP

/// Sends messages to connected PULL sockets, taking the peers in turn (load balancing). `send` waits
/// while no peer can take a message.
public final class PushSocket: ZMQSocket {
    private let engine: SocketEngine

    public var options: SocketOptions { engine.options }

    public init(context: ZMQContext = .default, options: SocketOptions = SocketOptions()) {
        engine = SocketEngine(context: context, options: options, behavior: PushBehavior())
    }

    @discardableResult
    public func bind(_ endpoint: String) async throws -> String { try await engine.bind(endpoint) }

    public func connect(_ endpoint: String) async throws { try await engine.connect(endpoint) }

    public func close() async { await engine.close() }

    public func events() -> AsyncStream<SocketEvent> { engine.events() }

    /// Queues `message` for one peer. Waits while every peer is at the send high-water mark (or there
    /// is none); cancelling the task ends the wait with `CancellationError` and sends nothing.
    public func send(_ message: Message) async throws {
        try await engine.send(message.wireFrames())
    }
}

final class PushBehavior: SocketBehavior {
    init() { super.init(socketType: .push) }

    override func trySend(_ frames: [ByteBuffer]) throws -> SendOutcome {
        guard let pipe = core.nextLoadBalancedPipe() else { return .wouldBlock }
        pipe.enqueue(frames)
        return .accepted
    }

    override func sendCapacity() -> Int { core.loadBalancedCapacity() }

    /// PUSH never expects data from its peer.
    override func received(_ frames: [ByteBuffer], from pipe: Pipe) -> InboundMessage? { nil }
}
