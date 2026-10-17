import ZMQ

struct TestTimeout: Error {}

/// Runs `operation` and fails with `TestTimeout` if it takes longer than `seconds`.
/// The losing task is cancelled; every blocking ZMQ call ends promptly on cancellation.
func within<T: Sendable>(
    _ seconds: Int = 10, _ operation: @escaping @Sendable () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask { try await operation() }
        group.addTask {
            try await Task.sleep(for: .seconds(seconds))
            throw TestTimeout()
        }
        defer { group.cancelAll() }
        guard let result = try await group.next() else { throw TestTimeout() }
        return result
    }
}

/// An endpoint on 127.0.0.1 where nothing listens: a port the system just handed out and took back.
func unusedEndpoint() async throws -> String {
    let probe = PullSocket()
    let endpoint = try await probe.bind("tcp://127.0.0.1:*")
    await probe.close()
    return endpoint
}

/// Options that make the tests quick: short reconnect interval and no waiting at close.
func fastOptions() -> SocketOptions {
    var options = SocketOptions()
    options.reconnectInterval = .milliseconds(20)
    options.linger = .seconds(1)
    return options
}

actor Flag {
    private(set) var isRaised = false
    func raise() { isRaised = true }
}
