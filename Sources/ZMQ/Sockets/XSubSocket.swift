import NIOCore
import ZMTP

/// Like `SubSocket`, but the application sees the subscription traffic: it can also pass subscriptions
/// and messages upstream with `send`, and `receive` does not filter. Used to build brokers (see
/// `ZMQProxy`).
public final class XSubSocket: ZMQSocket {
    private let engine: SocketEngine

    public var options: SocketOptions { engine.options }

    public init(context: ZMQContext = .default, options: SocketOptions = SocketOptions()) {
        engine = SocketEngine(context: context, options: options, behavior: XSubBehavior())
    }

    @discardableResult
    public func bind(_ endpoint: String) async throws -> String { try await engine.bind(endpoint) }

    public func connect(_ endpoint: String) async throws { try await engine.connect(endpoint) }

    public func close() async { await engine.close() }

    public func events() -> AsyncStream<SocketEvent> { engine.events() }

    /// Subscribes to messages whose first frame starts with `prefix` (the empty prefix: everything).
    /// Works before the socket is bound or connected, and again after a reconnect. Subscribing to the
    /// same prefix twice needs two `unsubscribe` calls.
    public func subscribe(_ prefix: Frame = Frame()) async throws {
        let prefix = prefix.buffer
        try await engine.perform { (behavior: XSubBehavior) in behavior.subscribe(prefix) }
    }

    /// Takes one subscription to `prefix` back. Does nothing when the prefix is not subscribed.
    public func unsubscribe(_ prefix: Frame) async throws {
        let prefix = prefix.buffer
        try await engine.perform { (behavior: XSubBehavior) in behavior.unsubscribe(prefix) }
    }

    /// Sends `message` upstream. A message of one frame that starts with 0x01 subscribes to the rest of
    /// the frame, one that starts with 0x00 cancels such a subscription (the same as `subscribe` and
    /// `unsubscribe`). Any other message goes to every connected peer that has room, and is dropped for
    /// those that have none. Does not wait for any peer.
    public func send(_ message: Message) async throws {
        try await engine.send(message.wireFrames())
    }

    /// Waits for the next message from any publisher. Unlike `SubSocket`, nothing is filtered here:
    /// what the publisher sent is what you get. Cancelling the task ends the wait with
    /// `CancellationError` and loses no message.
    public func receive() async throws -> Message {
        Message(wireFrames: try await engine.receive())
    }

    /// Every message until the socket is closed.
    public var messages: MessageSequence<Message> {
        MessageSequence { try await self.receive() }
    }
}

final class XSubBehavior: SubBehavior {
    init() { super.init(socketType: .xsub) }

    /// A one-frame message with a first byte of 1 or 0 is a subscription change; anything else is a
    /// message for the publishers, sent to those pipes that are connected and have room.
    override func trySend(_ frames: [ByteBuffer]) throws -> SendOutcome {
        if frames.count == 1, let marker = frames[0].getInteger(at: frames[0].readerIndex, as: UInt8.self),
            marker <= 1
        {
            var prefix = frames[0]
            prefix.moveReaderIndex(forwardBy: 1)
            if marker == 1 {
                subscribe(prefix)
            } else {
                unsubscribe(prefix)
            }
            return .accepted
        }
        for pipe in core.pipes where pipe.canAccept && pipe.connection?.isAttached == true {
            pipe.enqueue(frames)
        }
        return .accepted
    }

    /// No local filtering: the application sees everything the publishers send.
    /// XSUB never waits (a subscription message is applied, the rest goes to every pipe or is dropped).
    override func sendCapacity() -> Int { SocketCore.fixedSendCapacity }

    override func received(_ frames: [ByteBuffer], from pipe: Pipe) -> InboundMessage? {
        InboundMessage(frames: frames)
    }
}
