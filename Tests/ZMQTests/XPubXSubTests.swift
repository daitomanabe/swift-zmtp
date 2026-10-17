import Testing
import ZMQ

/// Calls `send` every 20 ms until the returned task is cancelled (a subscription reaches the publisher
/// some time after the connect, so a test keeps publishing until its subscriber sees a message).
private func publishRepeatedly(
    _ send: @escaping @Sendable () async throws -> Void
) -> Task<Void, any Error> {
    Task {
        while !Task.isCancelled {
            try await send()
            try await Task.sleep(for: .milliseconds(20))
        }
    }
}

private func stop(_ task: Task<Void, any Error>) async {
    task.cancel()
    _ = await task.result
}

/// A subscription frame as an XSUB application writes it: 0x01 (subscribe) or 0x00 (cancel) and the prefix.
private func subscriptionMessage(_ marker: UInt8, _ prefix: String) -> Message {
    Message(Frame([marker] + Array(prefix.utf8)))
}

@Suite struct XPubTests {
    /// Notices are about prefixes counted over all peers; a peer that goes away cancels what it held.
    /// "Nothing came" is shown by order: a later sentinel notice must be the next one.
    @Test(.timeLimit(.minutes(1)))
    func reportsFirstSubscriberAndLastCancel() async throws {
        let xpub = XPubSocket(options: fastOptions())
        let endpoint = try await xpub.bind("tcp://127.0.0.1:*")

        let first = SubSocket(options: fastOptions())
        try await first.subscribe("a")
        try await first.connect(endpoint)
        #expect(try await within { try await xpub.receive() } == .subscribe(prefix: "a"))

        // The second SUB subscribes to "a" too: no notice. A message it receives proves XPUB has
        // processed its subscription.
        let second = SubSocket(options: fastOptions())
        try await second.subscribe("a")
        try await second.connect(endpoint)
        let publisher = publishRepeatedly { try await xpub.send("a1") }
        #expect(try await within { try await second.receive() } == "a1")
        await stop(publisher)

        // The first SUB cancels "a" (the second still holds it): no notice either. Its next
        // subscription is the sentinel, and it must be the next notice.
        try await first.unsubscribe("a")
        try await first.subscribe("z")
        #expect(try await within { try await xpub.receive() } == .subscribe(prefix: "z"))

        // The last holder of "a" goes away.
        await second.close()
        #expect(try await within { try await xpub.receive() } == .unsubscribe(prefix: "a"))

        // XPUB sends like PUB: to the subscribed SUB, and only to a matching one.
        let remaining = publishRepeatedly {
            try await xpub.send(Message(["a-no-subscriber"]))
            try await xpub.send("z1")
        }
        // (The "a1" it got while it still subscribed to "a" may be in its queue; nothing else may be.)
        try await within {
            while true {
                let message = try await first.receive()
                if message == "z1" { break }
                #expect(message == "a1")
            }
        }
        await stop(remaining)

        await first.close()
        await xpub.close()
    }
}

@Suite struct XSubTests {
    /// `subscribe` and a 0x01 message both subscribe at the PUB; the XSUB does not filter what it gets.
    @Test(.timeLimit(.minutes(1)))
    func subscribesAtPublisher() async throws {
        let pub = PubSocket(options: fastOptions())
        let endpoint = try await pub.bind("tcp://127.0.0.1:*")
        let xsub = XSubSocket(options: fastOptions())
        try await xsub.subscribe("t")
        try await xsub.connect(endpoint)

        let publisher = publishRepeatedly {
            try await pub.send("u1")
            try await pub.send("t1")
        }
        // Only "t" is subscribed so far, so "u1" cannot have come.
        for _ in 0..<3 {
            #expect(try await within { try await xsub.receive() } == "t1")
        }

        try await xsub.send(subscriptionMessage(1, "u"))
        try await within {
            while try await xsub.receive() != "u1" {}
        }
        await stop(publisher)

        await xsub.close()
        await pub.close()
    }
}

@Suite struct ProxyTests {
    /// PUB -> XSUB | proxy | XPUB -> SUB: the subscription of the SUB travels up to the PUB.
    @Test(.timeLimit(.minutes(1)))
    func pubSubBroker() async throws {
        let pub = PubSocket(options: fastOptions())
        let pubEndpoint = try await pub.bind("tcp://127.0.0.1:*")
        let xsub = XSubSocket(options: fastOptions())
        try await xsub.connect(pubEndpoint)
        let xpub = XPubSocket(options: fastOptions())
        let xpubEndpoint = try await xpub.bind("tcp://127.0.0.1:*")
        let proxy = Task { try await ZMQProxy.run(frontend: xsub, backend: xpub) }

        let sub = SubSocket(options: fastOptions())
        try await sub.subscribe("topic")
        try await sub.connect(xpubEndpoint)

        let publisher = publishRepeatedly {
            try await pub.send("other nobody wants")
            try await pub.send("topic hello")
        }
        for _ in 0..<2 {
            #expect(try await within { try await sub.receive() } == "topic hello")
        }
        await stop(publisher)

        // Closing a socket ends the proxy; it returns without an error.
        await xpub.close()
        try await within { try await proxy.value }

        await sub.close()
        await xsub.close()
        await pub.close()
    }

    /// REQ -> ROUTER | proxy | DEALER -> REP, and the reply finds its way back. Cancelling the task
    /// ends the proxy with CancellationError.
    @Test(.timeLimit(.minutes(1)))
    func routerDealerBroker() async throws {
        let router = RouterSocket(options: fastOptions())
        let routerEndpoint = try await router.bind("tcp://127.0.0.1:*")
        let dealer = DealerSocket(options: fastOptions())
        let dealerEndpoint = try await dealer.bind("tcp://127.0.0.1:*")
        let proxy = Task { try await ZMQProxy.run(frontend: router, backend: dealer) }

        let req = ReqSocket(options: fastOptions())
        try await req.connect(routerEndpoint)
        let rep = RepSocket(options: fastOptions())
        try await rep.connect(dealerEndpoint)

        for round in 1...2 {
            let request = Task { try await req.request(Message("ping \(round)")) }
            #expect(try await within { try await rep.receive() } == Message("ping \(round)"))
            try await rep.reply(Message("pong \(round)"))
            #expect(try await within { try await request.value } == Message("pong \(round)"))
        }

        proxy.cancel()
        await #expect {
            try await within { try await proxy.value }
        } throws: {
            $0 is CancellationError
        }

        await req.close()
        await rep.close()
        await router.close()
        await dealer.close()
    }
}
