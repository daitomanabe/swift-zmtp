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

    public func events() -> AsyncStream<SocketEvent> { engine.events() }

    /// Queues `message` for every peer that subscribed to a prefix of its first frame. A peer whose
    /// queue is full misses the message; without a matching subscriber the message is dropped. Does not
    /// wait for any peer.
    public func send(_ message: Message) async throws {
        try await engine.send(message.wireFrames())
    }
}

/// The sending side of PUB. XPUB subclasses it and adds the reporting of subscriptions.
class PubBehavior: SocketBehavior {
    /// What each pipe subscribed to, by pipe ID. A pipe without an entry gets nothing.
    private var subscriptions: [Int: SubscriptionSet] = [:]

    override init(socketType: SocketType = .pub) { super.init(socketType: socketType) }

    /// `pipe` now holds a subscription to `prefix` (a repeated SUBSCRIBE of the same prefix changes nothing).
    func prefixSubscribed(_ prefix: ByteBuffer, on pipe: Pipe) {}

    /// `pipe` no longer holds `prefix`: it was cancelled, or the pipe lost all its subscriptions.
    func prefixCancelled(_ prefix: ByteBuffer, on pipe: Pipe) {}

    private func dropSubscriptions(of pipe: Pipe) {
        guard let set = subscriptions.removeValue(forKey: pipe.id) else { return }
        for prefix in set.prefixes { prefixCancelled(prefix, on: pipe) }
    }

    override func pipeAttached(_ pipe: Pipe, peer: Metadata) -> Bool {
        // A new connection starts without subscriptions; the SUB side sends its set again.
        dropSubscriptions(of: pipe)
        return true
    }

    override func pipeDetached(_ pipe: Pipe) {
        dropSubscriptions(of: pipe)
        // Messages that were chosen for the old subscriber and never written go with it.
        pipe.outbound.removeAll()
    }

    override func pipeRemoved(_ pipe: Pipe) {
        dropSubscriptions(of: pipe)
    }

    override func peerSubscribed(_ topic: ByteBuffer, on pipe: Pipe) {
        // A copy: the topic is a slice of a network read and would keep all of it alive.
        let topic = ByteBuffer(bytes: topic.readableBytesView)
        if subscriptions[pipe.id, default: SubscriptionSet(countsDuplicates: false)].add(topic) {
            prefixSubscribed(topic, on: pipe)
        }
    }

    override func peerCancelled(_ topic: ByteBuffer, on pipe: Pipe) {
        guard var set = subscriptions[pipe.id] else { return }
        let last = set.remove(topic)
        subscriptions[pipe.id] = set.isEmpty ? nil : set
        if last { prefixCancelled(topic, on: pipe) }
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
    /// PUB never waits, so any number of queued messages is fine; the loop routes or drops them.
    override func sendCapacity() -> Int { SocketCore.fixedSendCapacity }

    override func received(_ frames: [ByteBuffer], from pipe: Pipe) -> InboundMessage? { nil }
}
