import Foundation
import Testing
import ZMQ

/// Records the client keys a CURVE server was asked about.
private actor KeyLog {
    private(set) var seen: [CurvePublicKey] = []
    func add(_ key: CurvePublicKey) { seen.append(key) }
}

@Suite struct CurveTests {
    private static func isHandshakeFailed(_ event: SocketEvent) -> Bool {
        if case .handshakeFailed = event { true } else { false }
    }

    private static func isHandshakeSucceeded(_ event: SocketEvent) -> Bool {
        if case .handshakeSucceeded = event { true } else { false }
    }

    @Test(.timeLimit(.minutes(1)))
    func pushPullDeliversOverCurve() async throws {
        let serverKeys = CurveKeyPair.generate()
        let clientKeys = CurveKeyPair.generate()
        let log = KeyLog()

        var serverOptions = fastOptions()
        serverOptions.security = .curveServer(keyPair: serverKeys) { key in
            await log.add(key)
            return true
        }
        let pull = PullSocket(options: serverOptions)
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")

        var clientOptions = fastOptions()
        clientOptions.security = .curveClient(serverKey: serverKeys.publicKey, keyPair: clientKeys)
        let push = PushSocket(options: clientOptions)
        let events = push.events()
        try await push.connect(endpoint)

        // A short one, a message of two frames, and one that needs the long frame form.
        let large = Message(Data(repeating: 0x5a, count: 70_000))
        try await within { try await push.send("hello") }
        try await within { try await push.send(["part1", "part2"]) }
        try await within { try await push.send(large) }
        #expect(try await within { try await pull.receive() } == "hello")
        #expect(try await within { try await pull.receive() } == ["part1", "part2"])
        #expect(try await within { try await pull.receive() } == large)

        // The server was asked about the client's own public key, once.
        #expect(await log.seen == [clientKeys.publicKey])
        let seen = try await within { await events.collect(until: Self.isHandshakeSucceeded) }
        #expect(seen.last == .handshakeSucceeded(endpoint: endpoint, peerSocketType: "PULL"))
        await push.close()
        await pull.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func serverWithoutAnAuthenticateFunctionAcceptsAnyClientKey() async throws {
        let serverKeys = CurveKeyPair.generate()
        var serverOptions = fastOptions()
        serverOptions.security = .curveServer(keyPair: serverKeys)
        let pull = PullSocket(options: serverOptions)
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")

        var clientOptions = fastOptions()
        clientOptions.security = .curveClient(serverKey: serverKeys.publicKey, keyPair: .generate())
        let push = PushSocket(options: clientOptions)
        try await push.connect(endpoint)
        try await within { try await push.send("hello") }
        #expect(try await within { try await pull.receive() } == "hello")
        await push.close()
        await pull.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func refusedClientKeyGetsError400AndNothingIsDelivered() async throws {
        let serverKeys = CurveKeyPair.generate()
        let allowed = CurveKeyPair.generate()
        var serverOptions = fastOptions()
        serverOptions.security = .curveServer(keyPair: serverKeys) { key in key == allowed.publicKey }
        let pull = PullSocket(options: serverOptions)
        let pullEvents = pull.events()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")

        var clientOptions = fastOptions()
        clientOptions.security = .curveClient(serverKey: serverKeys.publicKey, keyPair: .generate())
        let push = PushSocket(options: clientOptions)
        let pushEvents = push.events()
        try await push.connect(endpoint)
        try await within { try await push.send("hello") }

        let clientSeen = try await within { await pushEvents.collect(until: Self.isHandshakeFailed) }
        guard case .handshakeFailed(_, let clientReason)? = clientSeen.last else {
            Issue.record("expected handshakeFailed on the client, got \(clientSeen)")
            return
        }
        #expect(clientReason.contains("400"), "\(clientReason)")
        let serverSeen = try await within { await pullEvents.collect(until: Self.isHandshakeFailed) }
        guard case .handshakeFailed(_, let serverReason)? = serverSeen.last else {
            Issue.record("expected handshakeFailed on the server, got \(serverSeen)")
            return
        }
        #expect(serverReason.contains("authentication failed"), "\(serverReason)")
        await #expect(throws: TestTimeout.self) { try await within(1) { try await pull.receive() } }
        await push.close()
        await pull.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func wrongServerKeyNeverDeliversAndFailsTheHandshake() async throws {
        let serverKeys = CurveKeyPair.generate()
        var serverOptions = fastOptions()
        serverOptions.security = .curveServer(keyPair: serverKeys)
        let pull = PullSocket(options: serverOptions)
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")

        var clientOptions = fastOptions()
        clientOptions.security = .curveClient(serverKey: CurveKeyPair.generate().publicKey, keyPair: .generate())
        let push = PushSocket(options: clientOptions)
        let events = push.events()
        try await push.connect(endpoint)
        try await within { try await push.send("hello") }

        // The server cannot open the HELLO and closes without a word; the client reports the failed handshake.
        let seen = try await within { await events.collect(until: Self.isHandshakeFailed) }
        guard case .handshakeFailed? = seen.last else {
            Issue.record("expected handshakeFailed, got \(seen)")
            return
        }
        #expect(!seen.contains { Self.isHandshakeSucceeded($0) })
        await #expect(throws: TestTimeout.self) { try await within(1) { try await pull.receive() } }
        await push.close()
        await pull.close()
    }

    @Test func keyPairsRoundTripThroughZ85() throws {
        let pair = CurveKeyPair.generate()
        #expect(pair.publicKey.z85.utf8.count == 40 && pair.secretKey.z85.utf8.count == 40)
        #expect(try CurvePublicKey(z85: pair.publicKey.z85) == pair.publicKey)
        #expect(try CurvePublicKey(bytes: pair.publicKey.bytes) == pair.publicKey)

        // The public key follows from the secret key.
        let restored = CurveKeyPair(secretKey: try CurveSecretKey(z85: pair.secretKey.z85))
        #expect(restored.publicKey == pair.publicKey)

        // A key pair of libzmq's own tests (the recorded fixture): public from secret.
        let secret = try CurveSecretKey(z85: "{?9QGN7^VF^Msxzuo*kxNS{!nZ{}nu(erFAar&Gn")
        #expect(CurveKeyPair(secretKey: secret).publicKey.z85 == "3#^JWo2sgZYcd?SD/PN:C*Qxh&jNQrt{3s6lbhbq")

        // Bad keys are refused without echoing the key, and a secret key does not print its bytes.
        #expect(throws: ZMQError.self) { try CurvePublicKey(z85: "tooShort") }
        #expect(throws: ZMQError.self) { try CurvePublicKey(bytes: [UInt8](repeating: 1, count: 31)) }
        #expect(throws: ZMQError.self) { try CurveSecretKey(z85: String(repeating: "\u{7f}", count: 40)) }
        #expect(!String(describing: secret).contains(secret.z85))
        #expect(!String(reflecting: secret).contains(secret.z85))
        #expect(!String(describing: CurveKeyPair(secretKey: secret)).contains(secret.z85))
    }
}
