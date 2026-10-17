import NIOCore
import ZMTP

/// Receives the messages of connected PUB sockets that match its subscriptions. A new SUB socket
/// receives nothing until it subscribes.
public final class SubSocket: ZMQSocket {
    private let engine: SocketEngine

    public var options: SocketOptions { engine.options }

    public init(context: ZMQContext = .default, options: SocketOptions = SocketOptions()) {
        engine = SocketEngine(context: context, options: options, behavior: SubBehavior())
    }

    @discardableResult
    public func bind(_ endpoint: String) async throws -> String { try await engine.bind(endpoint) }

    public func connect(_ endpoint: String) async throws { try await engine.connect(endpoint) }

    public func close() async { await engine.close() }

    /// Subscribes to messages whose first frame starts with `prefix` (the empty prefix: everything).
    /// Works before the socket is bound or connected, and again after a reconnect. Subscribing to the
    /// same prefix twice needs two `unsubscribe` calls.
    public func subscribe(_ prefix: Frame = Frame()) async throws {
        let prefix = prefix.buffer
        try await engine.perform { (behavior: SubBehavior) in behavior.subscribe(prefix) }
    }

    /// Takes one subscription to `prefix` back. Does nothing when the prefix is not subscribed.
    public func unsubscribe(_ prefix: Frame) async throws {
        let prefix = prefix.buffer
        try await engine.perform { (behavior: SubBehavior) in behavior.unsubscribe(prefix) }
    }

    /// Waits for the next matching message. Cancelling the task ends the wait with `CancellationError`
    /// and loses no message.
    public func receive() async throws -> Message {
        Message(wireFrames: try await engine.receive())
    }

    /// Every message until the socket is closed.
    public var messages: MessageSequence<Message> {
        MessageSequence { try await self.receive() }
    }
}

final class SubBehavior: SocketBehavior {
    private var subscriptions = SubscriptionSet()

    init() { super.init(socketType: .sub) }

    func subscribe(_ prefix: ByteBuffer) {
        guard subscriptions.add(prefix) else { return }
        for pipe in core.pipes { pipe.writeSubscription(prefix, subscribe: true) }
    }

    func unsubscribe(_ prefix: ByteBuffer) {
        guard subscriptions.remove(prefix) else { return }
        for pipe in core.pipes { pipe.writeSubscription(prefix, subscribe: false) }
    }

    /// Every new connection (also after a reconnect) gets the whole set again, once per prefix.
    override func pipeAttached(_ pipe: Pipe, peer: Metadata) -> Bool {
        for prefix in subscriptions.prefixes { pipe.writeSubscription(prefix, subscribe: true) }
        return true
    }

    /// The PUB side filters too; this drops what slipped through (a subscription not yet cancelled
    /// there, or a peer that does not filter).
    override func received(_ frames: [ByteBuffer], from pipe: Pipe) -> InboundMessage? {
        guard let topic = frames.first, subscriptions.matches(topic) else { return nil }
        return InboundMessage(frames: frames)
    }
}
