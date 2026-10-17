import Synchronization

/// Fans the events of one socket out to any number of `events()` streams.
///
/// Streams may be created from any thread; events are emitted from the socket's event loop. The lock
/// only guards the table of streams, and it is never held while an event is yielded or a stream
/// finished (a stream's termination handler takes the lock again).
final class SocketEventHub: Sendable {
    private struct State {
        var streams: [Int: AsyncStream<SocketEvent>.Continuation] = [:]
        var nextID = 0
        var isFinished = false
    }

    /// How many events a slow listener may fall behind before the oldest ones are dropped.
    static let bufferSize = 256

    private let state = Mutex(State())

    /// A new stream that sees the events emitted from now on. After `finish()` it ends at once.
    func makeStream() -> AsyncStream<SocketEvent> {
        let (stream, continuation) = AsyncStream.makeStream(
            of: SocketEvent.self, bufferingPolicy: .bufferingNewest(Self.bufferSize))
        let id: Int? = state.withLock { state in
            guard !state.isFinished else { return nil }
            defer { state.nextID += 1 }
            state.streams[state.nextID] = continuation
            return state.nextID
        }
        guard let id else {
            continuation.finish()
            return stream
        }
        continuation.onTermination = { [weak self] _ in
            self?.state.withLock { _ = $0.streams.removeValue(forKey: id) }
        }
        return stream
    }

    func emit(_ event: SocketEvent) {
        let targets = state.withLock { Array($0.streams.values) }
        for continuation in targets { continuation.yield(event) }
    }

    /// Ends every stream and any stream created later. Call after emitting `.closed`.
    func finish() {
        let targets = state.withLock { state -> [AsyncStream<SocketEvent>.Continuation] in
            state.isFinished = true
            defer { state.streams.removeAll() }
            return Array(state.streams.values)
        }
        for continuation in targets { continuation.finish() }
    }
}
