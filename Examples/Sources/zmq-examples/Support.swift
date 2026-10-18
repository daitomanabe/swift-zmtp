import Foundation
import ZMQ

// Small helpers shared by the examples. Nothing here is about ZeroMQ except `unusedEndpoint()`.

/// The command line is wrong: the dispatcher prints the usage and exits with status 64.
struct UsageError: Error, CustomStringConvertible {
    let description: String
    init(_ description: String) { self.description = description }
}

/// An example could not do its job; the text says why. Exit status 1.
struct ExampleError: Error, CustomStringConvertible {
    let description: String
    init(_ description: String) { self.description = description }
}

/// Runs `operation`, but gives up after `limit`: returns nil then, and cancels the operation.
///
/// This is how a time limit goes on more than one call (here: on a whole subcommand). For a single call the
/// sockets have it built in: `receive(timeout:)`, `send(_:timeout:)` and `request(_:timeout:)` throw
/// `ZMQError.timedOut`. Every waiting call of this library ends with `CancellationError` when its task is
/// cancelled, and a cancelled `receive` loses no message. That is what makes a limit this short.
func timeout<T: Sendable>(
    after limit: Duration, _ operation: @escaping @Sendable () async throws -> T
) async throws -> T? {
    try await withThrowingTaskGroup(of: T?.self) { group in
        group.addTask { try await operation() }
        group.addTask {
            try await Task.sleep(for: limit)
            return nil
        }
        defer { group.cancelAll() }
        return try await group.next() ?? nil
    }
}

/// An endpoint on 127.0.0.1 where nothing listens: a port the system just handed out and took back.
func unusedEndpoint() async throws -> String {
    let probe = PullSocket()
    let endpoint = try await probe.bind("tcp://127.0.0.1:*")   // "*" asks the system for a free port
    await probe.close()
    return endpoint
}

func printError(_ text: String) {
    FileHandle.standardError.write(Data((text + "\n").utf8))
}
