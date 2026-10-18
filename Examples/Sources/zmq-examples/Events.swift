import ZMQ

/// Socket events: what a socket tells about its connections.
///
/// A connecting socket does not fail when nobody listens: it keeps trying in the background (the delay between
/// tries starts at `reconnectInterval` and, when `reconnectIntervalMax` is set, doubles up to that), and sends
/// queue up meanwhile. `events()` shows all this. Every call to it makes a new stream, and a stream shows only what
/// happens after the call.
enum Events {
    /// `events [endpoint]`: connects a PULL socket to an endpoint where nothing listens, waits, then lets a PUSH
    /// socket bind there, sends it one message, and closes everything.
    static func run(_ arguments: Arguments) async throws {
        // A port that is free now, so that connecting to it finds nobody.
        let endpoint = if let given = arguments.positional.first { given } else { try await unusedEndpoint() }

        var options = SocketOptions()
        options.reconnectInterval = .milliseconds(100)
        options.reconnectIntervalMax = .seconds(1)

        let pull = PullSocket(options: options)
        // Print every event of the socket until the last one (.closed) ends the stream.
        let events = pull.events()
        let log = Task {
            for await event in events { print("      event: \(event)") }
        }

        print("1. PULL connects to \(endpoint), where nothing listens yet")
        try await pull.connect(endpoint)
        try await Task.sleep(for: .seconds(1.5))

        print("2. a PUSH socket binds \(endpoint) and sends a message")
        let push = PushSocket()
        try await push.bind(endpoint)
        try await push.send("hello")
        let message = try await pull.receive()
        print("3. PULL received \"\(message.frames[0].string ?? "(binary data)")\"")

        print("4. the PUSH socket closes")
        await push.close()
        try await Task.sleep(for: .milliseconds(500))

        print("5. PULL closes")
        await pull.close()
        await log.value
    }
}
