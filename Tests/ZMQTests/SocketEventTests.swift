import Testing
import ZMQ

extension AsyncStream where Element == SocketEvent {
    /// Reads events until `stop` accepts one (that event is included) or the stream ends.
    func collect(until stop: @Sendable (SocketEvent) -> Bool = { _ in false }) async -> [SocketEvent] {
        var seen: [SocketEvent] = []
        for await event in self {
            seen.append(event)
            if stop(event) { break }
        }
        return seen
    }
}

@Suite struct SocketEventTests {
    @Test(.timeLimit(.minutes(1)))
    func pushPullPairReportsItsConnection() async throws {
        let pull = PullSocket()
        let pullEvents = pull.events()
        let secondListener = pull.events()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")
        let push = PushSocket()
        let pushEvents = push.events()
        try await push.connect(endpoint)
        try await within { try await push.send("hello") }
        #expect(try await within { try await pull.receive() } == "hello")

        await push.close()
        await pull.close()
        // Both sockets are closed: every stream ends with .closed and finishes.
        let pullSeen = try await within { await pullEvents.collect() }
        let secondSeen = try await within { await secondListener.collect() }
        let pushSeen = try await within { await pushEvents.collect() }

        #expect(pullSeen == secondSeen)
        #expect(pullSeen.first == .listening(endpoint: endpoint))
        #expect(pullSeen.last == .closed)
        let accepted = pullSeen.firstIndex(of: .accepted(endpoint: endpoint))
        let pullHandshake = pullSeen.firstIndex(of: .handshakeSucceeded(endpoint: endpoint, peerSocketType: "PUSH"))
        #expect(accepted != nil && pullHandshake != nil)
        #expect(accepted.map { $0 < (pullHandshake ?? 0) } == true)

        #expect(pushSeen.first == .connected(endpoint: endpoint))
        #expect(pushSeen.contains(.handshakeSucceeded(endpoint: endpoint, peerSocketType: "PULL")))
        #expect(pushSeen.last == .closed)

        // A stream made after the close has nothing to say and ends at once.
        let late = try await within { await pull.events().collect() }
        #expect(late == [])
    }

    @Test(.timeLimit(.minutes(1)))
    func failedBindIsReported() async throws {
        let first = PullSocket()
        let endpoint = try await first.bind("tcp://127.0.0.1:*")
        let second = PullSocket()
        let events = second.events()
        await #expect(throws: ZMQError.self) { try await second.bind(endpoint) }
        await second.close()
        await first.close()

        let seen = try await within { await events.collect() }
        guard case .bindFailed(let failed, _)? = seen.first else {
            Issue.record("expected bindFailed first, got \(seen)")
            return
        }
        #expect(failed == endpoint)
        #expect(seen.last == .closed)
    }
}
