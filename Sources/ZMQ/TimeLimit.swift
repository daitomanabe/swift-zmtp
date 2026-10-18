/// What the two children of `withTimeLimit` report.
private enum TimeLimitEvent<Value: Sendable>: Sendable {
    /// The operation ended, with a value or an error.
    case finished(Result<Value, any Error>)
    /// The limit passed.
    case deadline
    /// The timer was cancelled before the limit passed.
    case timerCancelled
}

/// Runs `operation` and gives up after `limit`: the shared core of every `receive(timeout:)`,
/// `send(_:timeout:)` and `request(_:timeout:)`.
///
/// The operation races a timer in a task group. When the timer wins, the operation is cancelled and then
/// still awaited, because a blocking call of this library that is cancelled may nevertheless return a
/// value (a message that arrived at the deadline is not lost): that value is returned. If the operation
/// ends with `CancellationError`, the call throws `ZMQError.timedOut`. Any other error of the operation
/// is thrown as it is. If the calling task itself is cancelled, the call throws `CancellationError`
/// (or returns the value the operation produced anyway). Both children are finished when this returns.
func withTimeLimit<Value: Sendable>(
    _ limit: Duration,
    _ operation: @escaping @Sendable () async throws -> Value
) async throws -> Value {
    try await withThrowingTaskGroup(of: TimeLimitEvent<Value>.self) { group in
        group.addTask {
            do {
                return .finished(.success(try await operation()))
            } catch {
                return .finished(.failure(error))
            }
        }
        group.addTask {
            do {
                try await Task.sleep(for: limit)
                return .deadline
            } catch {
                return .timerCancelled
            }
        }

        var deadlinePassed = false
        while let event = try await group.next() {
            switch event {
            case .finished(let result):
                group.cancelAll()   // stops the timer; the group waits for it before we return
                if deadlinePassed, case .failure(is CancellationError) = result {
                    // Our own cancellation, or the operation giving up because the limit passed?
                    throw Task.isCancelled ? CancellationError() : ZMQError.timedOut
                }
                return try result.get()
            case .deadline:
                deadlinePassed = true
                group.cancelAll()   // cancels the operation; its end is the next event
            case .timerCancelled:
                break   // the calling task was cancelled; the operation is cancelled too and ends next
            }
        }
        // Unreachable: the operation always reports `.finished`.
        throw CancellationError()
    }
}
