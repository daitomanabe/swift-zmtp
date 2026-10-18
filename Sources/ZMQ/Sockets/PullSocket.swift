import NIOCore
import ZMTP

/// Receives messages from connected PUSH sockets, taking the peers in turn (fair queueing).
public final class PullSocket: ZMQSocket {
    private let engine: SocketEngine

    public var options: SocketOptions { engine.options }

    public init(context: ZMQContext = .default, options: SocketOptions = SocketOptions()) {
        engine = SocketEngine(context: context, options: options, behavior: PullBehavior())
    }

    @discardableResult
    public func bind(_ endpoint: String) async throws -> String { try await engine.bind(endpoint) }

    public func connect(_ endpoint: String) async throws { try await engine.connect(endpoint) }

    public func close() async { await engine.close() }

    public func events() -> AsyncStream<SocketEvent> { engine.events() }

    /// Waits for the next message. Cancelling the task ends the wait with `CancellationError`
    /// and loses no message.
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

final class PullBehavior: SocketBehavior {
    init() { super.init(socketType: .pull) }

    override var usesReceiveQueue: Bool { true }
}
