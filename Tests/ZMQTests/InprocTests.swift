import Testing
import ZMQ

private actor Counter {
    private(set) var value = 0
    func increment() { value += 1 }
}

/// Every test uses its own `ZMQContext`, so names cannot clash between tests.
@Suite struct InprocTests {
    // MARK: PUSH -> PULL

    @Test(.timeLimit(.minutes(1)))
    func pushToPullBindFirst() async throws {
        let context = ZMQContext()
        let pull = PullSocket(context: context)
        let pullEvents = pull.events()
        let bound = try await pull.bind("inproc://bind-first")
        #expect(bound == "inproc://bind-first")

        let push = PushSocket(context: context)
        let pushEvents = push.events()
        try await push.connect("inproc://bind-first")
        let single: Message = "hello"
        let multi = Message(["a", "", "ccc"])
        try await within { try await push.send(single) }
        try await within { try await push.send(multi) }
        #expect(try await within { try await pull.receive() } == single)
        #expect(try await within { try await pull.receive() } == multi)

        await push.close()
        await pull.close()
        // The same events as for a TCP link.
        let pullSeen = try await within { await pullEvents.collect() }
        let pushSeen = try await within { await pushEvents.collect() }
        #expect(pullSeen.first == .listening(endpoint: "inproc://bind-first"))
        #expect(pullSeen.contains(.accepted(endpoint: "inproc://bind-first")))
        #expect(pullSeen.contains(.handshakeSucceeded(endpoint: "inproc://bind-first", peerSocketType: "PUSH")))
        #expect(pullSeen.contains(.disconnected(endpoint: "inproc://bind-first")))
        #expect(pullSeen.last == .closed)
        #expect(pushSeen.first == .connected(endpoint: "inproc://bind-first"))
        #expect(pushSeen.contains(.handshakeSucceeded(endpoint: "inproc://bind-first", peerSocketType: "PULL")))
        #expect(pushSeen.contains(.disconnected(endpoint: "inproc://bind-first")))
        #expect(pushSeen.last == .closed)
    }

    @Test(.timeLimit(.minutes(1)))
    func pushToPullConnectFirst() async throws {
        let context = ZMQContext()
        let push = PushSocket(context: context)
        try await push.connect("inproc://connect-first")
        // Nobody is bound yet: the message waits in the pipe made by connect().
        try await within { try await push.send("early") }

        let pull = PullSocket(context: context)
        try await pull.bind("inproc://connect-first")
        #expect(try await within { try await pull.receive() } == "early")
        try await within { try await push.send("late") }
        #expect(try await within { try await pull.receive() } == "late")

        await push.close()
        await pull.close()
    }

    // MARK: REQ / REP and PUB / SUB

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
    func requestReply() async throws {
        let context = ZMQContext()
        let rep = RepSocket(context: context)
        try await rep.bind("inproc://rr-rep-binds")
        let req = ReqSocket(context: context)
        try await req.connect("inproc://rr-rep-binds")
        try await roundTrips(req: req, rep: rep)
        await req.close()
        await rep.close()

        // The other way round: REQ is the bound side.
        let req2 = ReqSocket(context: context)
        try await req2.bind("inproc://rr-req-binds")
        let rep2 = RepSocket(context: context)
        try await rep2.connect("inproc://rr-req-binds")
        try await roundTrips(req: req2, rep: rep2)
        await rep2.close()
        await req2.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func publishSubscribeWithFilter() async throws {
        let context = ZMQContext()
        let pub = PubSocket(context: context)
        try await pub.bind("inproc://pubsub")
        let sub = SubSocket(context: context)
        let events = sub.events()
        try await sub.subscribe("a")
        try await sub.connect("inproc://pubsub")
        // Once the SUB side has finished linking, its subscription is on its way to PUB's loop, ahead of
        // anything PUB is asked to send from now on.
        _ = await events.collect { event in
            if case .handshakeSucceeded = event { return true }
            return false
        }

        try await pub.send(Message(["b-skip", "a-in-the-second-frame"]))
        try await pub.send("a1")
        try await pub.send("c-skip")
        try await pub.send(Message(["a2", "tail"]))
        #expect(try await within { try await sub.receive() } == "a1")
        #expect(try await within { try await sub.receive() } == Message(["a2", "tail"]))

        // Cancelling takes the subscription back.
        try await sub.unsubscribe("a")
        try await sub.subscribe("c")
        try await pub.send("a3")
        try await pub.send("c1")
        #expect(try await within { try await sub.receive() } == "c1")

        await sub.close()
        await pub.close()
    }

    // MARK: Back-pressure

    @Test(.timeLimit(.minutes(1)))
    func sendWaitsWhileThePeerDoesNotRead() async throws {
        let context = ZMQContext()
        var options = SocketOptions()
        options.sendHighWaterMark = 2
        options.receiveHighWaterMark = 2
        options.linger = .zero
        let pull = PullSocket(context: context, options: options)
        try await pull.bind("inproc://pressure")
        let push = PushSocket(context: context, options: options)
        try await push.connect("inproc://pressure")

        let total = 100
        let sent = Counter()
        let sender = Task {
            for index in 0..<total {
                try await push.send(Message("m\(index)"))
                await sent.increment()
            }
        }

        // PULL reads nothing: the count of completed sends stops growing, far below `total`.
        var last = -1
        var stable = 0
        while stable < 3 {
            try await Task.sleep(for: .milliseconds(100))
            let now = await sent.value
            if now == last { stable += 1 } else { (last, stable) = (now, 0) }
        }
        #expect(last >= 1)
        #expect(last <= 8, "\(last) sends went through although nobody reads")

        // Reading lets the waiting sends continue, and every message arrives in order.
        for index in 0..<total {
            #expect(try await within { try await pull.receive() } == Message("m\(index)"))
        }
        try await within { try await sender.value }
        #expect(await sent.value == total)

        await push.close()
        await pull.close()
    }

    // MARK: Closing and re-binding

    @Test(.timeLimit(.minutes(1)))
    func connectSideFollowsANewBind() async throws {
        let context = ZMQContext()
        let first = PullSocket(context: context)
        try await first.bind("inproc://rebind")
        let push = PushSocket(context: context, options: fastOptions())
        try await push.connect("inproc://rebind")
        try await within { try await push.send("one") }
        #expect(try await within { try await first.receive() } == "one")

        // The bound side closes and gives the name back; a new socket takes it.
        await first.close()
        let second = PullSocket(context: context)
        try await second.bind("inproc://rebind")
        try await within { try await push.send("two") }
        #expect(try await within { try await second.receive() } == "two")

        await push.close()
        await second.close()
    }

    // MARK: Names

    @Test(.timeLimit(.minutes(1)))
    func namesBelongToOneContextAndOneSocket() async throws {
        let context = ZMQContext()
        let first = PullSocket(context: context)
        try await first.bind("inproc://taken")

        // A second bind of the same name fails; an empty name is not an endpoint.
        let second = PullSocket(context: context)
        await #expect {
            try await second.bind("inproc://taken")
        } throws: { error in
            if case ZMQError.bindFailed(let endpoint, _) = error { return endpoint == "inproc://taken" }
            return false
        }
        await #expect(throws: ZMQError.self) { try await second.bind("inproc://") }

        // A socket of another context does not see the name: its message never arrives.
        let other = ZMQContext()
        var options = SocketOptions()
        options.linger = .zero
        let stranger = PushSocket(context: other, options: options)
        try await stranger.connect("inproc://taken")
        try await within { try await stranger.send("lost") }
        await #expect(throws: TestTimeout.self) { _ = try await within(1) { try await first.receive() } }
        await stranger.close()

        // Closing gives the name back.
        await first.close()
        try await second.bind("inproc://taken")
        let friend = PushSocket(context: context)
        try await friend.connect("inproc://taken")
        try await within { try await friend.send("found") }
        #expect(try await within { try await second.receive() } == "found")

        await friend.close()
        await second.close()
    }
}
