import Testing
import ZMQ

/// Records the credentials a PLAIN server was asked about.
private actor CredentialLog {
    private(set) var seen: [PlainCredentials] = []
    func add(_ credentials: PlainCredentials) { seen.append(credentials) }
}

@Suite struct PlainTests {
    @Test(.timeLimit(.minutes(1)))
    func acceptedLoginDeliversMessages() async throws {
        let log = CredentialLog()
        var serverOptions = fastOptions()
        // An authenticate function that takes a moment: the handshake waits for it.
        serverOptions.security = .plainServer { credentials in
            await log.add(credentials)
            try? await Task.sleep(for: .milliseconds(30))
            return credentials.username == "admin" && credentials.password == "secret"
        }
        let pull = PullSocket(options: serverOptions)
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")

        var clientOptions = fastOptions()
        clientOptions.security = .plainClient(username: "admin", password: "secret")
        let push = PushSocket(options: clientOptions)
        let events = push.events()
        try await push.connect(endpoint)
        try await within { try await push.send("hello") }

        #expect(try await within { try await pull.receive() } == "hello")
        #expect(await log.seen == [PlainCredentials(username: "admin", password: "secret")])
        let seen = try await within { await events.collect { if case .handshakeSucceeded = $0 { true } else { false } } }
        #expect(seen.last == .handshakeSucceeded(endpoint: endpoint, peerSocketType: "PULL"))
        await push.close()
        await pull.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func wrongPasswordIsRefusedAndNothingIsDelivered() async throws {
        var serverOptions = fastOptions()
        serverOptions.security = .plainServer(accepting: ["admin": "secret"])
        let pull = PullSocket(options: serverOptions)
        let pullEvents = pull.events()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")

        var clientOptions = fastOptions()
        clientOptions.security = .plainClient(username: "admin", password: "wrong")
        let push = PushSocket(options: clientOptions)
        let pushEvents = push.events()
        try await push.connect(endpoint)
        try await within { try await push.send("hello") }

        // The client sees the server's ERROR "400"; the server says why it refused.
        let clientSeen = try await within { await pushEvents.collect { Self.isHandshakeFailed($0) } }
        guard case .handshakeFailed(_, let clientReason)? = clientSeen.last else {
            Issue.record("expected handshakeFailed on the client, got \(clientSeen)")
            return
        }
        #expect(clientReason.contains("400"), "\(clientReason)")
        let serverSeen = try await within { await pullEvents.collect { Self.isHandshakeFailed($0) } }
        guard case .handshakeFailed(_, let serverReason)? = serverSeen.last else {
            Issue.record("expected handshakeFailed on the server, got \(serverSeen)")
            return
        }
        #expect(serverReason.contains("authentication failed"), "\(serverReason)")
        #expect(!serverSeen.contains { if case .handshakeSucceeded = $0 { true } else { false } })

        await #expect(throws: TestTimeout.self) { try await within(1) { try await pull.receive() } }
        await push.close()
        await pull.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func nullClientFailsTheHandshakeWithAPlainServer() async throws {
        var serverOptions = fastOptions()
        serverOptions.security = .plainServer(accepting: ["admin": "secret"])
        let pull = PullSocket(options: serverOptions)
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")

        let push = PushSocket(options: fastOptions())
        let events = push.events()
        try await push.connect(endpoint)
        try await within { try await push.send("hello") }

        let seen = try await within { await events.collect { Self.isHandshakeFailed($0) } }
        guard case .handshakeFailed(_, let reason)? = seen.last else {
            Issue.record("expected handshakeFailed, got \(seen)")
            return
        }
        #expect(reason.contains("mechanism"), "\(reason)")
        #expect(!seen.contains { if case .handshakeSucceeded = $0 { true } else { false } })
        await #expect(throws: TestTimeout.self) { try await within(1) { try await pull.receive() } }
        await push.close()
        await pull.close()
    }

    @Test func credentialsOver255BytesAreRejectedAtConnect() async throws {
        var options = fastOptions()
        options.security = .plainClient(username: String(repeating: "u", count: 256), password: "p")
        let push = PushSocket(options: options)
        await #expect(throws: ZMQError.self) { try await push.connect("tcp://127.0.0.1:1") }
        await push.close()
    }

    private static func isHandshakeFailed(_ event: SocketEvent) -> Bool {
        if case .handshakeFailed = event { true } else { false }
    }
}
