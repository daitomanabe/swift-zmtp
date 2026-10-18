import ZMQ

/// Reading from several sockets in one loop: what `zmq_poll` does in C.
///
/// One child task per socket waits for its next message and passes it on; the loop below takes them one at a time, so
/// everything it does with them (counting, printing) happens in one place and needs no locking.
/// This is the Guide's "mspoller": it takes tasks from the ventilator and weather updates from the weather server.
///
///     zmq-examples wuserver
///     zmq-examples taskvent --workers 1        (waits for mspoller to connect)
///     zmq-examples mspoller
enum MultiSocketReader {
    enum Input: Sendable {
        case task(String)
        case weather(String)
    }

    /// `mspoller [--vent ENDPOINT] [--weather ENDPOINT] [--zipcode ZIP] [--count N]`: stops after N messages in all.
    static func run(_ arguments: Arguments) async throws {
        let ventEndpoint = arguments.string("vent") ?? Pipeline.ventEndpoint
        let weatherEndpoint = arguments.string("weather") ?? WeatherUpdates.defaultEndpoint
        let zipCode = arguments.string("zipcode") ?? "10001"
        let count = try arguments.int("count", default: 20)
        guard count > 0 else { return }

        let tasks = PullSocket()
        try await tasks.connect(ventEndpoint)
        let weather = SubSocket()
        try await weather.subscribe(Frame(zipCode))
        try await weather.connect(weatherEndpoint)

        // Both sockets feed one stream. `messages` ends when its socket is closed.
        let (inputs, continuation) = AsyncStream.makeStream(of: Input.self)
        await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                defer { continuation.finish() }   // if a reader ends, so does the loop below
                for try await message in tasks.messages {
                    continuation.yield(.task(message.frames[0].string ?? ""))
                }
            }
            group.addTask {
                defer { continuation.finish() }
                for try await message in weather.messages {
                    continuation.yield(.weather(message.frames.dropFirst().first?.string ?? ""))
                }
            }

            var seen = 0
            for await input in inputs {
                switch input {
                case .task(let milliseconds): print("mspoller: task of \(milliseconds) ms")
                case .weather(let data): print("mspoller: weather for \(zipCode): \(data)")
                }
                seen += 1
                if seen == count { break }
            }
            group.cancelAll()   // stops both readers
        }
        await tasks.close()
        await weather.close()
    }
}
