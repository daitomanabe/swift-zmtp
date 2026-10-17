import Testing
import ZMQ

@Suite struct HeartbeatTests {
    @Test(.timeLimit(.minutes(1)))
    func silentPeerIsDisconnectedAfterTheHeartbeatTimeout() async throws {
        let server = try await RawServer.start(.silentAfterReady)
        var options = fastOptions()
        options.heartbeatInterval = .milliseconds(50)
        options.heartbeatTimeout = .milliseconds(150)
        options.heartbeatTTL = .seconds(1)
        let push = PushSocket(options: options)
        let events = push.events()

        let started = ContinuousClock.now
        try await push.connect(server.endpoint)
        let seen = try await within(10) {
            await events.collect { if case .disconnected = $0 { true } else { false } }
        }
        let elapsed = ContinuousClock.now - started
        await push.close()
        await server.stop()

        let handshake = seen.firstIndex { if case .handshakeSucceeded = $0 { true } else { false } }
        #expect(handshake != nil)
        #expect(elapsed < .milliseconds(1500), "took \(elapsed)")
        // The PINGs reached the peer and carried the TTL (1 s = 10 tenths).
        #expect(server.pings.all.first == 10)
    }

    @Test(.timeLimit(.minutes(1)))
    func peerPingTTLClosesAConnectionThatGoesQuiet() async throws {
        // The peer pings with a TTL of 0.2 s and then says nothing; we have no heartbeat options ourselves.
        let server = try await RawServer.start(.pingOnceThenSilent(ttl: 2))
        let push = PushSocket(options: fastOptions())
        let events = push.events()

        let started = ContinuousClock.now
        try await push.connect(server.endpoint)
        let seen = try await within(10) {
            await events.collect { if case .disconnected = $0 { true } else { false } }
        }
        let elapsed = ContinuousClock.now - started
        await push.close()
        await server.stop()

        #expect(seen.contains { if case .handshakeSucceeded = $0 { true } else { false } })
        #expect(elapsed < .milliseconds(1500), "took \(elapsed)")
    }

    @Test(.timeLimit(.minutes(1)))
    func pongsKeepTheConnectionUp() async throws {
        var options = fastOptions()
        options.heartbeatInterval = .milliseconds(50)
        options.heartbeatTimeout = .milliseconds(150)
        let pull = PullSocket(options: options)
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")
        let push = PushSocket(options: options)
        let events = push.events()
        let collector = Task { await events.collect() }
        try await push.connect(endpoint)

        try await within { try await push.send("before") }
        #expect(try await within { try await pull.receive() } == "before")
        try await Task.sleep(for: .milliseconds(500))
        try await within { try await push.send("after") }
        #expect(try await within { try await pull.receive() } == "after")

        await push.close()
        await pull.close()
        let seen = await collector.value
        // One connection the whole time: it was made once and ended only with our own close().
        func count(_ matches: (SocketEvent) -> Bool) -> Int { seen.filter(matches).count }
        #expect(count { if case .connected = $0 { true } else { false } } == 1)
        #expect(count { if case .handshakeSucceeded = $0 { true } else { false } } == 1)
        #expect(count { if case .disconnected = $0 { true } else { false } } == 1)
        #expect(count { if case .connectRetried = $0 { true } else { false } } == 0)
    }
}

@Suite struct HandshakeTimeoutTests {
    @Test(.timeLimit(.minutes(1)))
    func silentPeerFailsTheHandshakeAndTheClientRetries() async throws {
        let server = try await RawServer.start(.silent)
        var options = fastOptions()
        options.handshakeTimeout = .milliseconds(100)
        let push = PushSocket(options: options)
        let events = push.events()
        try await push.connect(server.endpoint)

        let seen = try await within(10) {
            await events.collect { if case .connectRetried = $0 { true } else { false } }
        }
        await push.close()
        await server.stop()

        #expect(seen.first == .connected(endpoint: server.endpoint))
        let failure = seen.firstIndex {
            switch $0 {
            case .handshakeFailed, .disconnected: true
            default: false
            }
        }
        #expect(failure != nil)
        guard case .connectRetried(let retried, _)? = seen.last else {
            Issue.record("no connectRetried after the failed handshake: \(seen)")
            return
        }
        #expect(retried == server.endpoint)
        #expect(!seen.contains { if case .handshakeSucceeded = $0 { true } else { false } })
    }
}
