import Testing
import ZMQ

@Suite struct PushPullTests {
    @Test(.timeLimit(.minutes(1)))
    func pullBindsPushConnects() async throws {
        let pull = PullSocket()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")
        #expect(endpoint.hasPrefix("tcp://127.0.0.1:"))
        #expect(!endpoint.hasSuffix(":*"))

        let push = PushSocket()
        try await push.connect(endpoint)
        let single: Message = "hello"
        let multi = Message(["a", "", "ccc"])
        try await push.send(single)
        try await push.send(multi)

        let first = try await within { try await pull.receive() }
        let second = try await within { try await pull.receive() }
        #expect(first == single)
        #expect(second == multi)
        #expect(second.frames.map(\.string) == ["a", "", "ccc"])

        await push.close()
        await pull.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func pushBindsPullConnects() async throws {
        let push = PushSocket()
        let endpoint = try await push.bind("tcp://127.0.0.1:*")
        let pull = PullSocket()
        try await pull.connect(endpoint)

        // The send waits until the PULL side has connected and finished the handshake.
        try await within { try await push.send(Message(["x", "y"])) }
        let message = try await within { try await pull.receive() }
        #expect(message == Message(["x", "y"]))

        await pull.close()
        await push.close()
    }
}

@Suite struct PairTests {
    @Test(.timeLimit(.minutes(1)))
    func oneMessageEachWay() async throws {
        let left = PairSocket()
        let endpoint = try await left.bind("tcp://127.0.0.1:*")
        let right = PairSocket()
        try await right.connect(endpoint)

        try await within { try await right.send("ping") }
        #expect(try await within { try await left.receive() } == "ping")
        try await within { try await left.send("pong") }
        #expect(try await within { try await right.receive() } == "pong")

        await right.close()
        await left.close()
    }
}

@Suite struct ConnectionLifecycleTests {
    @Test(.timeLimit(.minutes(1)))
    func connectBeforeBind() async throws {
        let endpoint = try await unusedEndpoint()
        let push = PushSocket(options: fastOptions())
        try await push.connect(endpoint)
        // Nobody listens yet: the message waits in the pipe made by connect().
        try await within { try await push.send("early") }

        let pull = PullSocket()
        try await pull.bind(endpoint)
        #expect(try await within { try await pull.receive() } == "early")

        await push.close()
        await pull.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func reconnectAfterPeerRestart() async throws {
        let first = PullSocket()
        let endpoint = try await first.bind("tcp://127.0.0.1:*")
        let push = PushSocket(options: fastOptions())
        try await push.connect(endpoint)
        try await within { try await push.send("one") }
        #expect(try await within { try await first.receive() } == "one")

        await first.close()
        // Let the PUSH side see the connection end; the next message then waits for the reconnect.
        try await Task.sleep(for: .milliseconds(300))
        let second = PullSocket()
        try await second.bind(endpoint)
        try await within { try await push.send("two") }
        #expect(try await within { try await second.receive() } == "two")

        await push.close()
        await second.close()
    }
}

@Suite struct WaitingTests {
    @Test(.timeLimit(.minutes(1)))
    func sendHighWaterMarkBlocksAndCancels() async throws {
        let endpoint = try await unusedEndpoint()
        var options = fastOptions()
        options.sendHighWaterMark = 2
        options.linger = .zero
        let push = PushSocket(options: options)
        try await push.connect(endpoint)

        try await within { try await push.send("one") }
        try await within { try await push.send("two") }

        let sent = Flag()
        let third = Task {
            try await push.send("three")
            await sent.raise()
        }
        try await Task.sleep(for: .milliseconds(200))
        #expect(await sent.isRaised == false)

        third.cancel()
        await #expect(throws: CancellationError.self) { try await third.value }

        // The cancelled message was not queued: only the first two reach a late PULL.
        let pull = PullSocket()
        try await pull.bind(endpoint)
        #expect(try await within { try await pull.receive() } == "one")
        #expect(try await within { try await pull.receive() } == "two")
        let extra = Task { try await pull.receive() }
        try await Task.sleep(for: .milliseconds(300))
        extra.cancel()
        await #expect(throws: CancellationError.self) { _ = try await extra.value }

        await push.close()
        await pull.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func receiveCancellation() async throws {
        let pull = PullSocket()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")

        // Cancelled while waiting.
        let waiting = Task { try await pull.receive() }
        try await Task.sleep(for: .milliseconds(100))
        waiting.cancel()
        await #expect(throws: CancellationError.self) { _ = try await waiting.value }

        // Cancelled before receive() even starts: the cancel reaches the engine first.
        let early = Task { () -> Message in
            withUnsafeCurrentTask { $0?.cancel() }
            return try await pull.receive()
        }
        await #expect(throws: CancellationError.self) { _ = try await early.value }

        // The cancelled calls left nothing behind: a later message goes to a later receive().
        let push = PushSocket()
        try await push.connect(endpoint)
        try await within { try await push.send("later") }
        #expect(try await within { try await pull.receive() } == "later")

        await push.close()
        await pull.close()
    }
}
