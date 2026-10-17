import Testing
import ZMQ

private actor Counter {
    private(set) var value = 0
    func increment() { value += 1 }
}

/// routerMandatory, conflate, immediate and tcpKeepAlive. Every test uses its own `ZMQContext` for inproc,
/// so names cannot clash between tests; the TCP ones use ports the system hands out.
@Suite struct SocketOptionTests {
    // MARK: routerMandatory

    /// A ROUTER with a DEALER peer that is known by name; `router` has small high-water marks (inproc
    /// back-pressure is exact), and the dealer never reads.
    private func connectedPair(
        _ name: String, mandatory: Bool, context: ZMQContext
    ) async throws -> (router: RouterSocket, dealer: DealerSocket, id: RoutingID) {
        var routerOptions = SocketOptions()
        routerOptions.routerMandatory = mandatory
        routerOptions.sendHighWaterMark = 2
        routerOptions.linger = .zero
        let router = RouterSocket(context: context, options: routerOptions)
        try await router.bind("inproc://\(name)")

        var dealerOptions = SocketOptions()
        dealerOptions.routingID = RoutingID("dealer-1")
        dealerOptions.receiveHighWaterMark = 2
        dealerOptions.linger = .zero
        let dealer = DealerSocket(context: context, options: dealerOptions)
        try await dealer.connect("inproc://\(name)")

        // Once the router has received something, it knows the dealer's routing ID.
        try await dealer.send("hello")
        let hello = try await within { try await router.receive() }
        #expect(hello.routingID == RoutingID("dealer-1"))
        return (router, dealer, hello.routingID)
    }

    @Test(.timeLimit(.minutes(1)))
    func routerMandatoryReportsUnknownIDAndWaitsForAFullPeer() async throws {
        let context = ZMQContext()
        let (router, dealer, id) = try await connectedPair("mandatory", mandatory: true, context: context)

        // An unknown routing ID is an error, not a silent drop.
        let nobody = RoutingID("nobody")
        await #expect(throws: ZMQError.hostUnreachable(nobody)) {
            try await router.send("lost", to: nobody)
        }

        // The dealer does not read: sends go through until its queue and the router's high-water mark
        // are full, then the next one waits.
        let sent = Counter()
        let sender = Task {
            for index in 0..<100 {
                try await router.send(Message("m\(index)"), to: id)
                await sent.increment()
            }
        }
        var last = -1
        var stable = 0
        while stable < 3 {
            try await Task.sleep(for: .milliseconds(100))
            let now = await sent.value
            if now == last { stable += 1 } else { (last, stable) = (now, 0) }
        }
        #expect(last >= 1)
        #expect(last <= 8, "\(last) sends went through although nobody reads")

        // Cancelling the waiting send ends it with CancellationError.
        sender.cancel()
        await #expect(throws: CancellationError.self) { try await sender.value }

        // Nothing was dropped: the dealer gets every message that was sent, in order, and not the
        // cancelled one.
        for index in 0..<last {
            #expect(try await within { try await dealer.receive() } == Message("m\(index)"))
        }
        let extra = Task { try await dealer.receive() }
        try await Task.sleep(for: .milliseconds(300))
        extra.cancel()
        await #expect(throws: CancellationError.self) { _ = try await extra.value }

        await dealer.close()
        await router.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func routerWithoutMandatoryDropsSilently() async throws {
        let context = ZMQContext()
        let (router, dealer, id) = try await connectedPair("not-mandatory", mandatory: false, context: context)

        // Unknown routing ID: no error.
        try await within { try await router.send("lost", to: RoutingID("nobody")) }

        // A peer that does not read: every send returns at once, the surplus is dropped.
        for index in 0..<50 {
            try await within { try await router.send(Message("m\(index)"), to: id) }
        }
        await dealer.close()
        await router.close()
    }

    // MARK: conflate

    @Test(.timeLimit(.minutes(1)))
    func conflatingSubKeepsOnlyTheNewestMessage() async throws {
        let context = ZMQContext()
        let pub = PubSocket(context: context)
        try await pub.bind("inproc://conflate-sub")

        var options = SocketOptions()
        options.conflate = true
        let sub = SubSocket(context: context, options: options)
        let events = sub.events()
        try await sub.subscribe(Frame())
        try await sub.connect("inproc://conflate-sub")
        // Once the SUB side has finished linking, its subscription is on its way to PUB's loop, ahead of
        // anything PUB is asked to send from now on.
        _ = await events.collect { event in
            if case .handshakeSucceeded = event { return true }
            return false
        }

        // The SUB reads nothing while the PUB sends 100 messages.
        for index in 1...100 {
            try await pub.send(Message("\(index)"))
        }
        // Let the last of them cross to the SUB's event loop.
        try await Task.sleep(for: .milliseconds(300))

        #expect(try await within { try await sub.receive() } == "100")
        let extra = Task { try await sub.receive() }
        try await Task.sleep(for: .milliseconds(300))
        extra.cancel()
        await #expect(throws: CancellationError.self) { _ = try await extra.value }

        await sub.close()
        await pub.close()
    }

    // MARK: immediate

    @Test(.timeLimit(.minutes(1)))
    func immediateSendWaitsForTheHandshake() async throws {
        let endpoint = try await unusedEndpoint()
        var options = fastOptions()
        options.immediate = true
        options.linger = .zero
        let push = PushSocket(options: options)
        try await push.connect(endpoint)

        // No peer: the send does not return.
        let sent = Flag()
        let first = Task {
            try await push.send("first")
            await sent.raise()
        }
        try await Task.sleep(for: .milliseconds(300))
        #expect(await sent.isRaised == false)

        // Without the option the same send returns at once; the message waits in the pipe.
        var plain = fastOptions()
        plain.linger = .zero
        let control = PushSocket(options: plain)
        try await control.connect(try await unusedEndpoint())
        try await within(2) { try await control.send("queued") }

        // Once a PULL is bound and the handshake is done, the waiting send goes through, and later ones too.
        let pull = PullSocket()
        try await pull.bind(endpoint)
        try await within { try await first.value }
        #expect(try await within { try await pull.receive() } == "first")
        try await within { try await push.send("second") }
        #expect(try await within { try await pull.receive() } == "second")

        await control.close()
        await push.close()
        await pull.close()
    }

    // MARK: tcpKeepAlive

    @Test(.timeLimit(.minutes(1)))
    func tcpKeepAliveStillDelivers() async throws {
        var bindOptions = SocketOptions()
        bindOptions.tcpKeepAlive = .seconds(30)
        let pull = PullSocket(options: bindOptions)
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")

        // Less than a second is rounded up to the smallest idle time, 1 s.
        var connectOptions = SocketOptions()
        connectOptions.tcpKeepAlive = .milliseconds(100)
        let push = PushSocket(options: connectOptions)
        try await push.connect(endpoint)
        try await within { try await push.send("alive") }
        #expect(try await within { try await pull.receive() } == "alive")
        await push.close()
        await pull.close()

        // The option has no meaning for IPC and must not get in the way there.
        let ipcPull = PullSocket(options: bindOptions)
        let ipcEndpoint = try await ipcPull.bind("ipc://*")
        let ipcPush = PushSocket(options: connectOptions)
        try await ipcPush.connect(ipcEndpoint)
        try await within { try await ipcPush.send("alive") }
        #expect(try await within { try await ipcPull.receive() } == "alive")
        await ipcPush.close()
        await ipcPull.close()
    }
}
