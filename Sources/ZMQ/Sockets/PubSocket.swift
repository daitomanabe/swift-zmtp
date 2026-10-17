import NIOCore
import ZMTP

/// Publishes messages to the SUB peers whose subscriptions match. Never waits for a peer.
public final class PubSocket: ZMQSocket {
    private let engine: SocketEngine

    public var options: SocketOptions { engine.options }

    public init(context: ZMQContext = .default, options: SocketOptions = SocketOptions()) {
        engine = SocketEngine(context: context, options: options, behavior: PubBehavior())
    }

    @discardableResult
    public func bind(_ endpoint: String) async throws -> String { try await engine.bind(endpoint) }

    public func connect(_ endpoint: String) async throws { try await engine.connect(endpoint) }

    public func close() async { await engine.close() }

    /// Queues `message` for every peer that subscribed to a prefix of its first frame. A peer whose
    /// queue is full misses the message; without a matching subscriber the message is dropped. Does not
    /// wait for any peer.
    public func send(_ message: Message) async throws {
        try await engine.send(message.wireFrames())
    }
}

final class PubBehavior: SocketBehavior {
    /// What each pipe subscribed to, by pipe ID. A pipe without an entry gets nothing.
    private var subscriptions: [Int: SubscriptionSet] = [:]

    init() { super.init(socketType: .pub) }

    override func pipeAttached(_ pipe: Pipe, peer: Metadata) -> Bool {
        // A new connection starts without subscriptions; the SUB side sends its set again.
        subscriptions[pipe.id] = nil
        return true
    }

    override func pipeDetached(_ pipe: Pipe) {
        subscriptions[pipe.id] = nil
        // Messages that were chosen for the old subscriber and never written go with it.
        pipe.outbound.removeAll()
    }

    override func pipeRemoved(_ pipe: Pipe) {
        subscriptions[pipe.id] = nil
    }

    override func peerSubscribed(_ topic: ByteBuffer, on pipe: Pipe) {
        _ = subscriptions[pipe.id, default: SubscriptionSet()].add(topic)
    }

    override func peerCancelled(_ topic: ByteBuffer, on pipe: Pipe) {
        guard var set = subscriptions[pipe.id] else { return }
        _ = set.remove(topic)
        subscriptions[pipe.id] = set.isEmpty ? nil : set
    }

    override func trySend(_ frames: [ByteBuffer]) throws -> SendOutcome {
        guard let topic = frames.first else { return .accepted }
        for pipe in core.pipes where pipe.canAccept {
            if let set = subscriptions[pipe.id], set.matches(topic) {
                pipe.enqueue(frames)
            }
        }
        return .accepted
    }

    /// PUB never expects data from its peers.
    override func received(_ frames: [ByteBuffer], from pipe: Pipe) -> InboundMessage? { nil }
}
