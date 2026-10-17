import Testing
import ZMQ

private func isInvalidState(_ error: any Error) -> Bool {
    if case ZMQError.invalidState = error { return true }
    return false
}

@Suite struct PubSubTests {
    /// SUB gets the messages whose first frame starts with its prefix, and none of the others.
    @Test(.timeLimit(.minutes(1)))
    func subscriptionFiltersByFirstFramePrefix() async throws {
        let pub = PubSocket()
        let endpoint = try await pub.bind("tcp://127.0.0.1:*")
        let sub = SubSocket()
        // Before any connect; subscribe twice and unsubscribe once leaves one subscription.
        try await sub.subscribe("a")
        try await sub.subscribe("a")
        try await sub.unsubscribe("a")
        try await sub.connect(endpoint)

        // The subscription reaches PUB some time after the connect; keep sending until a1 arrives.
        let sender = Task {
            while !Task.isCancelled {
                try await pub.send(Message(["b-skip", "a-in-the-second-frame"]))
                try await pub.send("a1")
                try await Task.sleep(for: .milliseconds(20))
            }
        }
        for _ in 0..<3 {
            #expect(try await within { try await sub.receive() } == "a1")
        }
        sender.cancel()
        _ = await sender.result

        await sub.close()
        await pub.close()
    }

    /// A subscriber that never reads cannot make `send` wait: the full pipe drops.
    @Test(.timeLimit(.minutes(1)))
    func publishNeverWaits() async throws {
        var options = fastOptions()
        options.sendHighWaterMark = 2
        options.receiveHighWaterMark = 2
        options.linger = .zero
        let pub = PubSocket(options: options)
        let endpoint = try await pub.bind("tcp://127.0.0.1:*")
        let sub = SubSocket(options: options)
        try await sub.subscribe()
        try await sub.connect(endpoint)

        // One probe message proves the subscription is active; after that SUB reads nothing.
        let probe = Task {
            while !Task.isCancelled {
                try await pub.send("probe")
                try await Task.sleep(for: .milliseconds(20))
            }
        }
        #expect(try await within { try await sub.receive() } == "probe")
        probe.cancel()
        _ = await probe.result

        let big = Message(Frame(Array(repeating: 0x61, count: 16 * 1024)))
        try await within(10) {
            for _ in 0..<5000 { try await pub.send(big) }
        }

        await sub.close()
        await pub.close()
    }
}

@Suite struct RequestReplyTests {
    /// Two request/reply rounds, so the second one shows the sockets went back to their idle states.
    private func roundTrips(req: ReqSocket, rep: RepSocket) async throws {
        for round in 1...2 {
            let request = Task { try await req.request(Message("ping \(round)")) }
            #expect(try await within { try await rep.receive() } == Message("ping \(round)"))
            try await rep.reply(Message("pong \(round)"))
            #expect(try await within { try await request.value } == Message("pong \(round)"))
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func repBinds() async throws {
        let rep = RepSocket()
        let endpoint = try await rep.bind("tcp://127.0.0.1:*")
        let req = ReqSocket()
        try await req.connect(endpoint)
        try await roundTrips(req: req, rep: rep)
        await req.close()
        await rep.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func reqBinds() async throws {
        let req = ReqSocket()
        let endpoint = try await req.bind("tcp://127.0.0.1:*")
        let rep = RepSocket()
        try await rep.connect(endpoint)
        try await roundTrips(req: req, rep: rep)
        await rep.close()
        await req.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func repChecksItsState() async throws {
        let rep = RepSocket()
        let endpoint = try await rep.bind("tcp://127.0.0.1:*")
        let req = ReqSocket()
        try await req.connect(endpoint)

        // reply without a request
        await #expect { try await rep.reply("early") } throws: { isInvalidState($0) }

        let request = Task { try await req.request("ping") }
        #expect(try await within { try await rep.receive() } == "ping")
        // receive again without a reply
        await #expect { try await rep.receive() } throws: { isInvalidState($0) }

        // The failed receive changed nothing: the reply still goes out.
        try await rep.reply("pong")
        #expect(try await within { try await request.value } == "pong")

        await req.close()
        await rep.close()
    }

    /// A request cancelled while its reply is outstanding must not hand that late reply to the next one.
    @Test(.timeLimit(.minutes(1)))
    func cancelledRequestDropsItsLateReply() async throws {
        let rep = RepSocket()
        let endpoint = try await rep.bind("tcp://127.0.0.1:*")
        let req = ReqSocket()
        try await req.connect(endpoint)

        let first = Task { try await req.request("first") }
        #expect(try await within { try await rep.receive() } == "first")
        first.cancel()
        await #expect(throws: CancellationError.self) { _ = try await first.value }
        // REP answers only now, after REQ gave up.
        try await rep.reply("old")

        let second = Task { try await req.request("second") }
        #expect(try await within { try await rep.receive() } == "second")
        try await rep.reply("new")
        #expect(try await within { try await second.value } == "new")

        await req.close()
        await rep.close()
    }
}

@Suite struct DealerRouterTests {
    @Test(.timeLimit(.minutes(1)))
    func routerSeesTheDealerIdentityAndAnswersIt() async throws {
        let router = RouterSocket()
        let endpoint = try await router.bind("tcp://127.0.0.1:*")
        var options = SocketOptions()
        options.routingID = RoutingID("d1")
        let dealer = DealerSocket(options: options)
        try await dealer.connect(endpoint)

        try await within { try await dealer.send("hello") }
        let routed = try await within { try await router.receive() }
        #expect(routed.routingID.frame.string == "d1")
        #expect(routed.message == "hello")

        try await router.send("welcome", to: routed.routingID)
        #expect(try await within { try await dealer.receive() } == "welcome")

        // An ID nobody has: dropped without an error.
        try await router.send("lost", to: RoutingID("nobody"))

        await dealer.close()
        await router.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func routerMakesUpAnIDForAnonymousPeers() async throws {
        let router = RouterSocket()
        let endpoint = try await router.bind("tcp://127.0.0.1:*")
        let dealer = DealerSocket()
        try await dealer.connect(endpoint)

        try await within { try await dealer.send("hello") }
        let routed = try await within { try await router.receive() }
        #expect(routed.routingID.frame.count == 5)
        #expect(routed.routingID.frame.bytes.first == 0)

        // The made-up ID works for the way back.
        try await router.send("welcome", to: routed.routingID)
        #expect(try await within { try await dealer.receive() } == "welcome")

        await dealer.close()
        await router.close()
    }
}

@Suite struct EnvelopeTests {
    /// REQ puts an empty frame in front; ROUTER shows it and a reply to it needs one too.
    @Test(.timeLimit(.minutes(1)))
    func reqToRouter() async throws {
        let router = RouterSocket()
        let endpoint = try await router.bind("tcp://127.0.0.1:*")
        let req = ReqSocket()
        try await req.connect(endpoint)

        let request = Task { try await req.request("body") }
        let routed = try await within { try await router.receive() }
        #expect(routed.message.frames.map(\.string) == ["", "body"])
        try await router.send(Message([Frame(), "reply"]), to: routed.routingID)
        #expect(try await within { try await request.value } == "reply")

        await req.close()
        await router.close()
    }

    /// DEALER does the envelope by hand: an empty frame, then the body, both ways.
    @Test(.timeLimit(.minutes(1)))
    func dealerToRep() async throws {
        let rep = RepSocket()
        let endpoint = try await rep.bind("tcp://127.0.0.1:*")
        let dealer = DealerSocket()
        try await dealer.connect(endpoint)

        try await within { try await dealer.send(Message([Frame(), "body"])) }
        #expect(try await within { try await rep.receive() } == "body")
        try await rep.reply("reply")
        let answer = try await within { try await dealer.receive() }
        #expect(answer.frames.map(\.string) == ["", "reply"])

        await dealer.close()
        await rep.close()
    }
}
