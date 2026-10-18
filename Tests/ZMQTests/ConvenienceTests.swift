import Testing
import ZMQ

/// The small conveniences of DESIGN 8: string interpolation, `Message.string`, and the `timeout:` calls.
/// Each test has a time limit.
@Suite struct ConvenienceTests {
    @Test(.timeLimit(.minutes(1)))
    func stringInterpolationBuildsMessagesAndFrames() {
        let number = 7
        let message: Message = "Hello \(number)"
        #expect(message == Message("Hello 7"))
        #expect(message.frames.count == 1)
        #expect(message.frames[0].string == "Hello 7")

        let frame: Frame = "id-\(number)"
        #expect(frame == Frame("id-7"))
        #expect(Message([frame, "tail \(number)"]).frames.count == 2)
    }

    @Test(.timeLimit(.minutes(1)))
    func messageStringIsTheTextOfOneUTF8Frame() {
        #expect(Message("héllo").string == "héllo")
        #expect(Message(Frame()).string == "")
        #expect(Message(["one", "two"]).string == nil)
        #expect(Message([]).string == nil)
        #expect(Message(Frame([0xFF, 0xFE, 0x41])).string == nil)
    }

    /// Runs `body` and returns how long it took.
    private func elapsed(_ body: () async throws -> Void) async rethrows -> Duration {
        let start = ContinuousClock.now
        try await body()
        return ContinuousClock.now - start
    }

    @Test(.timeLimit(.minutes(1)))
    func receiveTimesOutOnAnEmptyPull() async throws {
        let pull = PullSocket()
        _ = try await pull.bind("tcp://127.0.0.1:*")

        let took = await elapsed {
            await #expect(throws: ZMQError.timedOut) { _ = try await pull.receive(timeout: .milliseconds(200)) }
        }
        #expect(took >= .milliseconds(190) && took < .seconds(5))

        // The task itself cancelled: CancellationError, not a time-out.
        let waiting = Task { try await pull.receive(timeout: .seconds(30)) }
        try await Task.sleep(for: .milliseconds(100))
        waiting.cancel()
        await #expect(throws: CancellationError.self) { _ = try await waiting.value }

        await pull.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func receiveWithTimeoutStillDeliversAndLosesNothing() async throws {
        let pull = PullSocket()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")
        let push = PushSocket()
        try await push.connect(endpoint)

        // Sent just before the call.
        try await push.send("early")
        #expect(try await pull.receive(timeout: .seconds(5)) == "early")

        // Arrives while the call waits.
        let waiting = Task { try await pull.receive(timeout: .seconds(5)) }
        try await Task.sleep(for: .milliseconds(100))
        try await push.send("while waiting")
        #expect(try await waiting.value == "while waiting")

        // A call that timed out took nothing: the next message goes to the next call.
        await #expect(throws: ZMQError.timedOut) { _ = try await pull.receive(timeout: .milliseconds(100)) }
        try await push.send("after")
        #expect(try await pull.receive(timeout: .seconds(5)) == "after")

        await push.close()
        await pull.close()
    }

    /// A REP that never answers: the request times out. Its late answer is dropped, and the next
    /// request gets its own reply.
    @Test(.timeLimit(.minutes(1)))
    func requestTimesOutAndTheNextRequestGetsItsOwnReply() async throws {
        let rep = RepSocket()
        let endpoint = try await rep.bind("tcp://127.0.0.1:*")
        let req = ReqSocket()
        try await req.connect(endpoint)

        let first = Task { try await req.request("first", timeout: .milliseconds(300)) }
        #expect(try await rep.receive(timeout: .seconds(5)) == "first")   // taken, and left unanswered
        await #expect(throws: ZMQError.timedOut) { _ = try await first.value }

        let second = Task { try await req.request("second", timeout: .seconds(5)) }
        try await rep.reply("late answer to first")
        #expect(try await rep.receive(timeout: .seconds(5)) == "second")
        try await rep.reply("answer to second")
        #expect(try await second.value == "answer to second")

        await req.close()
        await rep.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func sendTimesOutAtTheHighWaterMark() async throws {
        let endpoint = try await unusedEndpoint()
        var options = fastOptions()
        options.sendHighWaterMark = 2
        options.linger = .zero
        let push = PushSocket(options: options)
        try await push.connect(endpoint)

        try await push.send("one", timeout: .seconds(5))
        try await push.send("two", timeout: .seconds(5))
        let took = await elapsed {
            await #expect(throws: ZMQError.timedOut) { try await push.send("three", timeout: .milliseconds(200)) }
        }
        #expect(took >= .milliseconds(190) && took < .seconds(5))

        // The timed-out message was not queued: a late PULL gets the first two and nothing more.
        let pull = PullSocket()
        try await pull.bind(endpoint)
        #expect(try await pull.receive(timeout: .seconds(5)) == "one")
        #expect(try await pull.receive(timeout: .seconds(5)) == "two")
        await #expect(throws: ZMQError.timedOut) { _ = try await pull.receive(timeout: .milliseconds(300)) }

        await push.close()
        await pull.close()
    }
}
