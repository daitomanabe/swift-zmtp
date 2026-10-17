import NIOCore

/// A set of subscription prefixes with a count per prefix: the same prefix subscribed twice needs two
/// cancels. SUB keeps one for itself, PUB keeps one per pipe. Used on the socket's event loop only.
struct SubscriptionSet {
    private var counts: [ByteBuffer: Int] = [:]

    var isEmpty: Bool { counts.isEmpty }

    /// The distinct prefixes held.
    var prefixes: [ByteBuffer] { Array(counts.keys) }

    /// Counts one more subscription to `prefix`. True when it was not held before.
    mutating func add(_ prefix: ByteBuffer) -> Bool {
        let count = counts[prefix, default: 0]
        counts[prefix] = count + 1
        return count == 0
    }

    /// Counts one subscription to `prefix` off. True when that was the last one; false when others
    /// remain or when the prefix was not held (then nothing changes).
    mutating func remove(_ prefix: ByteBuffer) -> Bool {
        guard let count = counts[prefix] else { return false }
        if count > 1 {
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
