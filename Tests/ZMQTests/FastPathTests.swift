import Testing
import ZMQ

/// The send and receive fast path (DESIGN 6): `send` and `receive` that return without hopping onto the
/// socket's event loop, next to the slow path they fall back to. Each test has a time limit.
@Suite struct FastPathTests {
    /// 10,000 messages from one sender arrive in order at a reader that is slow at first, so the
    /// sender runs out of credits and moves between the fast and the slow path many times.
    private func orderedDelivery(push: PushSocket, pull: PullSocket) async throws {
        let total = 10_000
        let sender = Task {
            for index in 0..<total { try await push.send(Message("m\(index)")) }
        }
        // Nothing is read for a moment: the sender fills its credits and the queues, then waits.
        try await Task.sleep(for: .milliseconds(300))
        for index in 0..<total {
            let message = try await within(10) { try await pull.receive() }
            guard message == Message("m\(index)") else {
                Issue.record("message \(index) was \(message.frames.map(\.string))")
                break
            }
        }
        try await within { try await sender.value }
    }

    @Test(.timeLimit(.minutes(1)))
    func tenThousandMessagesInOrderOverTCP() async throws {
        let pull = PullSocket()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")
        let push = PushSocket()
        try await push.connect(endpoint)
        try await orderedDelivery(push: push, pull: pull)
        await push.close()
        await pull.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func tenThousandMessagesInOrderOverInproc() async throws {
        let context = ZMQContext()
        let pull = PullSocket(context: context)
        try await pull.bind("inproc://fast-order")
        let push = PushSocket(context: context)
        try await push.connect("inproc://fast-order")
        try await orderedDelivery(push: push, pull: pull)
        await push.close()
        await pull.close()
    }

    /// Four tasks send on one PUSH at the same time: everything arrives, and the messages of each task
    /// arrive in the order that task sent them.
    @Test(.timeLimit(.minutes(1)))
    func concurrentSendersKeepTheirOwnOrder() async throws {
        let pull = PullSocket()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")
        let push = PushSocket()
        try await push.connect(endpoint)

        let senders = 4
        let perSender = 2_500
        let tasks = (0..<senders).map { sender in
            Task {
                for index in 0..<perSender { try await push.send(Message("\(sender):\(index)")) }
            }
        }

        var next = [Int](repeating: 0, count: senders)
        for _ in 0..<(senders * perSender) {
            let text = try await within(10) { try await pull.receive() }.frames[0].string ?? ""
            let parts = text.split(separator: ":").compactMap { Int($0) }
            guard parts.count == 2, parts[0] < senders else {
                Issue.record("unexpected message \(text)")
                break
            }
            if parts[1] != next[parts[0]] {
                let expected = next[parts[0]]
                Issue.record("sender \(parts[0]): expected \(expected), got \(parts[1])")
                break
            }
            next[parts[0]] += 1
        }
        #expect(next == [Int](repeating: perSender, count: senders))
        for task in tasks { try await within { try await task.value } }

        await push.close()
        await pull.close()
    }

    /// Messages the fast path has accepted are not lost when `close()` follows at once: they go out
    /// within the linger time.
    @Test(.timeLimit(.minutes(1)))
    func closeRightAfterFastSendsDeliversEverything() async throws {
        let pull = PullSocket()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")
        var options = fastOptions()
        options.linger = .seconds(1)
        let push = PushSocket(options: options)
        try await push.connect(endpoint)

        // Fewer than the default high-water mark (1000): every send is accepted without waiting, most
        // of them before the connection is even up.
        let total = 800
        for index in 0..<total { try await push.send(Message("c\(index)")) }
        await push.close()
        await #expect(throws: ZMQError.socketClosed) { try await push.send("after close") }

        for index in 0..<total {
            let message = try await within { try await pull.receive() }
            guard message == Message("c\(index)") else {
                Issue.record("message \(index) was \(message.frames.map(\.string))")
                break
            }
        }
        await pull.close()
    }

    /// A send that waits (no peer, high-water mark reached) can be cancelled, and the socket works
    /// afterwards: the cancelled message is not sent, and a peer that appears later gets the rest.
    @Test(.timeLimit(.minutes(1)))
    func cancelledSlowSendThrowsAndLaterSendsStillWork() async throws {
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

        let pull = PullSocket()
        try await pull.bind(endpoint)
        try await within { try await push.send("four") }
        #expect(try await within { try await pull.receive() } == "one")
        #expect(try await within { try await pull.receive() } == "two")
        #expect(try await within { try await pull.receive() } == "four")

        await push.close()
        await pull.close()
    }

    /// A `receive()` that waits can be cancelled, and the message that comes later is not lost.
    @Test(.timeLimit(.minutes(1)))
    func cancelledReceiveThrowsAndLaterMessageArrives() async throws {
        let pull = PullSocket()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")
        let push = PushSocket()
        try await push.connect(endpoint)

        let waiting = Task { try await pull.receive() }
        try await Task.sleep(for: .milliseconds(100))
        waiting.cancel()
        await #expect(throws: CancellationError.self) { _ = try await waiting.value }

        try await within { try await push.send("late") }
        #expect(try await within { try await pull.receive() } == "late")

        await push.close()
        await pull.close()
    }
}
