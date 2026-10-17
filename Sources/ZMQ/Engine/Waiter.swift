import NIOCore

/// One suspended `receive` or blocked `send`.
///
/// A waiter is created by the calling task and then handed to the socket's event loop twice: once to
/// register it, once (from the cancellation handler) to cancel it. The two hops can arrive in either
/// order, and the cancel can arrive after the waiter was already fulfilled, so the waiter itself
/// remembers what happened: cancelling an idle waiter makes the later registration end at once with
/// `CancellationError`, and cancelling a finished waiter does nothing. No cancelled-ID list is needed.
///
/// `@unchecked Sendable`: the state is read and written only on the event loop of the owning socket
/// (every caller is an entry point of `SocketCore`, which asserts that); the other fields are immutable.
final class Waiter<Value: Sendable>: @unchecked Sendable {
    private enum State {
        case idle
        case waiting(CheckedContinuation<Value, any Error>)
        case cancelled
        case finished
    }

    private var state = State.idle
    /// The message of a blocked `send`; empty for a `receive`.
    let payload: [ByteBuffer]

    init(payload: [ByteBuffer] = []) {
        self.payload = payload
    }

    /// Registers the continuation. Returns false when the waiter was cancelled before it got here;
    /// the continuation has then already been resumed with `CancellationError`.
    func begin(_ continuation: CheckedContinuation<Value, any Error>) -> Bool {
        switch state {
        case .idle:
            state = .waiting(continuation)
            return true
        case .cancelled:
            state = .finished
            continuation.resume(throwing: CancellationError())
            return false
        case .waiting, .finished:
            preconditionFailure("a waiter is registered once")
        }
    }

    func succeed(_ value: Value) {
        guard case .waiting(let continuation) = state else { return }
        state = .finished
        continuation.resume(returning: value)
    }

    func fail(_ error: any Error) {
        guard case .waiting(let continuation) = state else { return }
        state = .finished
        continuation.resume(throwing: error)
    }

    /// Returns true when the waiter was registered and has now been resumed with `CancellationError`,
    /// so the caller must take it out of its queue.
    func cancel() -> Bool {
        switch state {
        case .idle:
            state = .cancelled
            return false
        case .waiting(let continuation):
            state = .finished
            continuation.resume(throwing: CancellationError())
            return true
        case .cancelled, .finished:
            return false
        }
    }
}
