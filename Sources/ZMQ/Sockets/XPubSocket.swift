import NIOCore
import ZMTP

/// What `XPubSocket.receive()` returns.
public enum XPubMessage: Sendable, Hashable {
    /// The first subscriber of this prefix (over all peers) appeared.
    case subscribe(prefix: Frame)
    /// The last subscriber of this prefix left: it cancelled, or its connection ended.
    case unsubscribe(prefix: Frame)
    /// Anything else an XSUB peer sent upstream.
    case message(Message)
}

/// Like `PubSocket`, and also tells the application about the subscriptions of its peers. Used to build
/// brokers: the subscriptions it reports are passed on to the publishers behind it (see `ZMQProxy`).
public final class XPubSocket: ZMQSocket {
    private let engine: SocketEngine

    public var options: SocketOptions { engine.options }

    public init(context: ZMQContext = .default, options: SocketOptions = SocketOptions()) {
        engine = SocketEngine(context: context, options: options, behavior: XPubBehavior())
    }

    @discardableResult
    public func bind(_ endpoint: String) async throws -> String { try await engine.bind(endpoint) }

    public func connect(_ endpoint: String) async throws { try await engine.connect(endpoint) }

    public func close() async { await engine.close() }

    public func events() -> AsyncStream<SocketEvent> { engine.events() }

    /// Queues `message` for every peer that subscribed to a prefix of its first frame. A peer whose
    /// queue is full misses the message; without a matching subscriber the message is dropped. Does not
    /// wait for any peer.
    public func send(_ message: Message) async throws {
        try await engine.send(message.wireFrames())
    }

    /// Waits for the next subscription change or upstream message. A change is reported when a prefix
    /// gets its first subscriber or loses its last one, counted over all peers; a peer that disconnects
    /// cancels everything it subscribed to. Notices come in the order they happened and before any
    /// upstream message. Cancelling the task ends the wait with `CancellationError` and loses nothing.
    public func receive() async throws -> XPubMessage {
        var frames = try await engine.receive()
        let tag = frames.removeFirst()
        switch XPubBehavior.Kind(tag: tag) {
        case .subscribe:
            return .subscribe(prefix: Frame(buffer: frames[0]))
        case .unsubscribe:
            return .unsubscribe(prefix: Frame(buffer: frames[0]))
        case .message:
            return .message(Message(wireFrames: frames))
        }
    }

    /// Like `receive()`, but gives up after `timeout`: it throws `ZMQError.timedOut` and takes no
    /// message, so a later `receive` still gets what arrives next. A message that arrives at the deadline
    /// is returned, not lost. Cancelling the task ends the call with `CancellationError`.
    public func receive(timeout: Duration) async throws -> XPubMessage {
        try await withTimeLimit(timeout) { try await self.receive() }
    }

    /// Every notice and upstream message until the socket is closed.
    public var messages: MessageSequence<XPubMessage> {
        MessageSequence { try await self.receive() }
    }
}

final class XPubBehavior: PubBehavior {
    /// What an item in the receive queue is. The engine queues plain frames, so the behavior puts one
    /// extra frame in front of the real ones to say what they are; `XPubSocket.receive()` takes it off.
    enum Kind: UInt8 {
        /// The frames are an upstream message.
        case message = 0
        /// One frame: the prefix.
        case subscribe = 1
        case unsubscribe = 2

        init(tag: ByteBuffer) {
            self = tag.getInteger(at: tag.readerIndex, as: UInt8.self).flatMap(Kind.init(rawValue:)) ?? .message
        }

        var tag: ByteBuffer { ByteBuffer(integer: rawValue) }
    }

    /// For each prefix, how many pipes hold it. A notice goes out when this changes between 0 and 1.
    private var holders: [ByteBuffer: Int] = [:]

    init() { super.init(socketType: .xpub) }

    override func prefixSubscribed(_ prefix: ByteBuffer, on pipe: Pipe) {
        let count = holders[prefix, default: 0]
        holders[prefix] = count + 1
        if count == 0 { notify(.subscribe, prefix) }
    }

    override func prefixCancelled(_ prefix: ByteBuffer, on pipe: Pipe) {
        guard let count = holders[prefix] else { return }
        if count > 1 {
            holders[prefix] = count - 1
            return
        }
        holders[prefix] = nil
        notify(.unsubscribe, prefix)
    }

    private func notify(_ kind: Kind, _ prefix: ByteBuffer) {
        // A copy: the prefix is a slice of a network read and would keep all of it alive.
        core.queueInbound(InboundMessage(frames: [kind.tag, ByteBuffer(bytes: prefix.readableBytesView)]))
    }

    /// Data from an XSUB peer, handed on as it is.
    /// The subscription notices (`queueInbound`) go through the receive queue too.
    override var usesReceiveQueue: Bool { true }

    override func received(_ frames: [ByteBuffer], from pipe: Pipe) -> InboundMessage? {
        InboundMessage(frames: [Kind.message.tag] + frames)
    }
}
