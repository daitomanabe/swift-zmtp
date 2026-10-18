import NIOCore
import ZMTP

/// Asynchronous request socket: sends to the peers in turn and receives from all of them, with the
/// frames left as they are. Set `options.routingID` to give a ROUTER peer a name to address it by.
public final class DealerSocket: ZMQSocket {
    private let engine: SocketEngine

    public var options: SocketOptions { engine.options }

    public init(context: ZMQContext = .default, options: SocketOptions = SocketOptions()) {
        engine = SocketEngine(context: context, options: options, behavior: DealerBehavior())
    }

    @discardableResult
    public func bind(_ endpoint: String) async throws -> String { try await engine.bind(endpoint) }

    public func connect(_ endpoint: String) async throws { try await engine.connect(endpoint) }

    public func close() async { await engine.close() }

    public func events() -> AsyncStream<SocketEvent> { engine.events() }

    /// Queues `message` for one peer, taking the peers in turn. Waits while every peer is at the send
    /// high-water mark (or there is none); cancelling the task ends the wait and sends nothing.
    public func send(_ message: Message) async throws {
        try await engine.send(message.wireFrames())
    }

    /// Like `send(_:)`, but gives up after `timeout`: it throws `ZMQError.timedOut` and queues nothing.
    /// Only a send that has to wait can time out. Cancelling the task ends the call with
    /// `CancellationError`.
    public func send(_ message: Message, timeout: Duration) async throws {
        try await withTimeLimit(timeout) { try await self.send(message) }
    }

    /// Waits for the next message from any peer. Cancelling the task ends the wait with
    /// `CancellationError` and loses no message.
    public func receive() async throws -> Message {
        Message(wireFrames: try await engine.receive())
    }

    /// Like `receive()`, but gives up after `timeout`: it throws `ZMQError.timedOut` and takes no
    /// message, so a later `receive` still gets what arrives next. A message that arrives at the deadline
    /// is returned, not lost. Cancelling the task ends the call with `CancellationError`.
    public func receive(timeout: Duration) async throws -> Message {
        try await withTimeLimit(timeout) { try await self.receive() }
    }

    /// Every message until the socket is closed.
    public var messages: MessageSequence<Message> {
        MessageSequence { try await self.receive() }
    }
}

final class DealerBehavior: SocketBehavior {
    init() { super.init(socketType: .dealer) }

    override func trySend(_ frames: [ByteBuffer]) throws -> SendOutcome {
        guard let pipe = core.nextLoadBalancedPipe() else { return .wouldBlock }
        pipe.enqueue(frames)
        return .accepted
    }

    override func sendCapacity() -> Int { core.loadBalancedCapacity() }

    override var usesReceiveQueue: Bool { true }
}
