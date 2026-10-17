import NIOCore

/// Subscription prefixes. Used on the socket's event loop only.
///
/// SUB and XSUB count their own subscriptions: the same prefix subscribed twice needs two
/// unsubscribes. The PUB side's view of one peer does not count: libzmq's SUB sends SUBSCRIBE for every
/// subscribe call but CANCEL only for the last unsubscribe, and libzmq's PUB keeps a plain set per peer,
/// so one CANCEL ends the subscription. Counting there would keep sending to a libzmq SUB that has left.
struct SubscriptionSet {
    private var counts: [ByteBuffer: Int] = [:]
    /// True for the subscriber's own set, false for the publisher's set of one peer.
    let countsDuplicates: Bool

    init(countsDuplicates: Bool = true) {
        self.countsDuplicates = countsDuplicates
    }

    var isEmpty: Bool { counts.isEmpty }

    /// The distinct prefixes held.
    var prefixes: [ByteBuffer] { Array(counts.keys) }

    /// Adds a subscription to `prefix`. True when it was not held before.
    mutating func add(_ prefix: ByteBuffer) -> Bool {
        let count = counts[prefix, default: 0]
        guard countsDuplicates || count == 0 else { return false }
        counts[prefix] = count + 1
        return count == 0
    }

    /// Takes a subscription to `prefix` off. True when the prefix is no longer held; false when
    /// counted subscriptions remain or when the prefix was not held (then nothing changes).
    mutating func remove(_ prefix: ByteBuffer) -> Bool {
        guard let count = counts[prefix] else { return false }
        if countsDuplicates && count > 1 {
            counts[prefix] = count - 1
            return false
        }
        counts[prefix] = nil
        return true
    }

    /// Whether `topic` starts with any held prefix. The empty prefix matches everything.
    func matches(_ topic: ByteBuffer) -> Bool {
        for prefix in counts.keys where prefix.readableBytes <= topic.readableBytes {
            if prefix.readableBytes == 0 || topic.readableBytesView.starts(with: prefix.readableBytesView) {
                return true
            }
        }
        return false
    }
}
