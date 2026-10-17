#if os(macOS)
import Foundation
import Testing
import ZMQ

// Stage 3 interop (docs/DESIGN.md 7.5 item 5): PLAIN and CURVE against libzmq 4.3.5 (pyzmq), with the
// libzmq side as server (checked by pyzmq's ZAP authenticator, `--zap-plain` / `--zap-curve`) and as client.
// Shares the helpers of InteropTests.swift (`interop`, `within`, `Role`, `PeerProcess`) and `EventLog`
// of InteropStage2Tests.swift. Which side is the server does not depend on which side binds, so every
// case runs in both roles.

// MARK: Helpers

private func isHandshakeFailed(_ event: SocketEvent) -> Bool {
    if case .handshakeFailed = event { return true }
    return false
}

private func isHandshakeSucceeded(_ event: SocketEvent) -> Bool {
    if case .handshakeSucceeded = event { return true }
    return false
}

private func isDisconnected(_ event: SocketEvent) -> Bool {
    if case .disconnected = event { return true }
    return false
}

private func isConnectRetried(_ event: SocketEvent) -> Bool {
    if case .connectRetried = event { return true }
    return false
}

/// The Socket-Type of the peer in each `handshakeSucceeded` of `events`.
private func handshakePeerTypes(_ events: [SocketEvent]) -> [String] {
    events.compactMap { event in
        if case .handshakeSucceeded(_, let peerSocketType) = event { return peerSocketType }
        return nil
    }
}

/// The reason of the last `handshakeFailed` in `events`, or nil when there is none.
private func handshakeFailure(in events: [SocketEvent]) -> String? {
    for event in events.reversed() {
        if case .handshakeFailed(_, let reason) = event { return reason }
    }
    return nil
}

private let z85Alphabet = Array(
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#".utf8)

/// Z85 (32/Z85) to bytes: libzmq takes keys as raw bytes (`--sockopt-hex`), the public API gives Z85 text.
private func z85Decode(_ text: String) -> [UInt8] {
    var value = [UInt8: UInt32]()
    for (index, character) in z85Alphabet.enumerated() { value[character] = UInt32(index) }
    let characters = Array(text.utf8)
    var bytes: [UInt8] = []
    var start = 0
    while start + 5 <= characters.count {
        var word: UInt32 = 0
        for offset in 0..<5 { word = word &* 85 &+ (value[characters[start + offset]] ?? 0) }
        bytes += [UInt8(word >> 24), UInt8((word >> 16) & 0xFF), UInt8((word >> 8) & 0xFF), UInt8(word & 0xFF)]
        start += 5
    }
    return bytes
}

private func publicKeyHex(_ pair: CurveKeyPair) -> String { hexString(pair.publicKey.bytes) }
private func secretKeyHex(_ pair: CurveKeyPair) -> String { hexString(z85Decode(pair.secretKey.z85)) }

/// Peer options for a libzmq CURVE server that owns `keys`.
private func curveServerArguments(_ keys: CurveKeyPair) -> [String] {
    ["--sockopt", "CURVE_SERVER=1", "--sockopt-hex", "CURVE_SECRETKEY=" + secretKeyHex(keys)]
}

/// Peer options for a libzmq CURVE client with `keys` that connects to the server owning `serverKey`.
private func curveClientArguments(serverKey: CurvePublicKey, keys: CurveKeyPair) -> [String] {
    [
        "--sockopt-hex", "CURVE_SERVERKEY=" + hexString(serverKey.bytes),
        "--sockopt-hex", "CURVE_PUBLICKEY=" + publicKeyHex(keys),
        "--sockopt-hex", "CURVE_SECRETKEY=" + secretKeyHex(keys),
    ]
}

/// Peer options for a libzmq PLAIN server that accepts only `admin` / `secret`.
private let plainServerArguments = ["--sockopt", "PLAIN_SERVER=1", "--zap-plain", "admin:secret"]

private func plainClientArguments(password: String) -> [String] {
    ["--sockopt", "PLAIN_USERNAME=admin", "--sockopt", "PLAIN_PASSWORD=" + password]
}

/// Tries to send without waiting for the result: a bound socket waits for a peer, and a refused login
/// never gives it one. Cancelling the task ends the wait.
private func trySend(_ socket: PushSocket, _ message: Message) -> Task<Void, Never> {
    Task { try? await socket.send(message) }
}

/// What a Swift server's `authenticate` function was asked.
private actor Asked<Value: Sendable> {
    private(set) var values: [Value] = []
    func add(_ value: Value) { values.append(value) }
}

@Suite(.serialized, .enabled(if: interopEnabled), .timeLimit(.minutes(2)))
struct LibzmqInteropStage3 {
    // MARK: PLAIN, Swift client -> libzmq server

    @Test(arguments: Role.allCases)
    func swiftPlainClientIsAcceptedByLibzmqServer(_ role: Role) async throws {
        var options = interopOptions()
        options.security = .plainClient(username: "admin", password: "secret")
        let push = PushSocket(options: options)
        let log = EventLog(push.events())
        defer { log.stop() }
        let message: Message = ["hello", "plain"]
        let peer = try await interop(
            push, peer: "PULL", role, peerArguments: plainServerArguments + ["--recv", "1"]
        ) { _ in
            try await within { try await push.send(message) }
        }
        #expect(peer.received == [bytes(message)], "\(peer.log)")
        #expect(handshakePeerTypes(log.all) == ["PULL"], "\(log.all)")
        #expect(log.all.contains(where: isHandshakeFailed) == false, "\(log.all)")
    }

    /// libzmq answers the wrong password with ERROR "400" (the ZAP status code) and closes.
    @Test(arguments: Role.allCases)
    func swiftPlainClientWithWrongPasswordIsRefusedByLibzmqServer(_ role: Role) async throws {
        var options = interopOptions()
        options.security = .plainClient(username: "admin", password: "wrong")
        let push = PushSocket(options: options)
        let log = EventLog(push.events())
        defer { log.stop() }
        var sender: Task<Void, Never>?
        defer { sender?.cancel() }
        let peer = try await interop(
            push, peer: "PULL", role,
            peerArguments: plainServerArguments + ["--linger-ms", "100", "--then", "drain:1.5"]
        ) { _ in
            sender = trySend(push, ["never", "delivered"])
            try await log.wait("handshakeFailed") { $0.contains(where: isHandshakeFailed) }
        }
        let reason = handshakeFailure(in: log.all)
        #expect(reason?.contains("400") == true, "reason: \(reason ?? "none"); \(log.all)")
        #expect(log.all.contains(where: isHandshakeSucceeded) == false, "\(log.all)")
        #expect(peer.received.isEmpty, "\(peer.log)")
    }

    // MARK: PLAIN, libzmq client -> Swift server

    @Test(arguments: Role.allCases)
    func libzmqPlainClientIsAcceptedBySwiftServer(_ role: Role) async throws {
        let asked = Asked<PlainCredentials>()
        var options = interopOptions()
        options.security = .plainServer { credentials in
            await asked.add(credentials)
            return credentials.username == "admin" && credentials.password == "secret"
        }
        let pull = PullSocket(options: options)
        let log = EventLog(pull.events())
        defer { log.stop() }
        let message: Message = ["hello", "plain"]
        var received: Message?
        let peer = try await interop(
            pull, peer: "PUSH", role,
            peerArguments: plainClientArguments(password: "secret") + ["--send", peerMessage(bytes(message))]
        ) { _ in
            received = try await within { try await pull.receive() }
        }
        #expect(received == message, "\(peer.log)")
        #expect(await asked.values == [PlainCredentials(username: "admin", password: "secret")], "\(peer.log)")
        #expect(handshakePeerTypes(log.all) == ["PUSH"], "\(log.all)")
        #expect(log.all.contains(where: isHandshakeFailed) == false, "\(log.all)")
    }

    @Test(arguments: Role.allCases)
    func libzmqPlainClientWithWrongPasswordIsRefusedBySwiftServer(_ role: Role) async throws {
        let asked = Asked<PlainCredentials>()
        var options = interopOptions()
        options.security = .plainServer { credentials in
            await asked.add(credentials)
            return credentials.username == "admin" && credentials.password == "secret"
        }
        let pull = PullSocket(options: options)
        let log = EventLog(pull.events())
        defer { log.stop() }
        let peer = try await interop(
            pull, peer: "PUSH", role,
            peerArguments: plainClientArguments(password: "wrong") + [
                "--linger-ms", "100", "--then", "offer:2.5:" + peerMessage(bytes("never")),
            ]
        ) { _ in
            try await log.wait("handshakeFailed") { $0.contains(where: isHandshakeFailed) }
            // The peer keeps offering messages during this second; none may arrive.
            await #expect(throws: Timeout.self) { try await within(1) { try await pull.receive() } }
        }
        let reason = handshakeFailure(in: log.all)
        #expect(reason?.contains("authentication failed") == true, "reason: \(reason ?? "none"); \(log.all)")
        #expect(await asked.values.first == PlainCredentials(username: "admin", password: "wrong"), "\(peer.log)")
        #expect(log.all.contains(where: isHandshakeSucceeded) == false, "\(log.all)")
        #expect(peer.events.contains { $0.name == "offered" }, "\(peer.log)")
    }

    // MARK: CURVE, Swift client -> libzmq server

    @Test(arguments: Role.allCases)
    func swiftCurveClientIsAcceptedByLibzmqServer(_ role: Role) async throws {
        let serverKeys = CurveKeyPair.generate()
        var options = interopOptions()
        options.security = .curveClient(serverKey: serverKeys.publicKey, keyPair: .generate())
        let push = PushSocket(options: options)
        let log = EventLog(push.events())
        defer { log.stop() }
        let message: Message = ["frame one", "frame two"]
        let peer = try await interop(
            push, peer: "PULL", role,
            peerArguments: curveServerArguments(serverKeys) + ["--zap-curve", "any", "--recv", "1"]
        ) { _ in
            try await within { try await push.send(message) }
        }
        #expect(peer.received == [bytes(message)], "\(peer.log)")
        #expect(handshakePeerTypes(log.all) == ["PULL"], "\(log.all)")
        #expect(log.all.contains(where: isHandshakeFailed) == false, "\(log.all)")
    }

    /// A server that does not own the key the client expects cannot open HELLO; libzmq closes without a word.
    @Test(arguments: Role.allCases)
    func swiftCurveClientWithWrongServerKeyIsRefusedByLibzmqServer(_ role: Role) async throws {
        let serverKeys = CurveKeyPair.generate()
        var options = interopOptions()
        options.security = .curveClient(serverKey: CurveKeyPair.generate().publicKey, keyPair: .generate())
        let push = PushSocket(options: options)
        let log = EventLog(push.events())
        defer { log.stop() }
        var sender: Task<Void, Never>?
        defer { sender?.cancel() }
        let peer = try await interop(
            push, peer: "PULL", role,
            peerArguments: curveServerArguments(serverKeys) + [
                "--zap-curve", "any", "--linger-ms", "100", "--then", "drain:1.5",
            ]
        ) { _ in
            sender = trySend(push, ["never", "delivered"])
            try await log.wait("handshakeFailed") { $0.contains(where: isHandshakeFailed) }
        }
        #expect(log.all.contains(where: isHandshakeSucceeded) == false, "\(log.all)")
        #expect(peer.received.isEmpty, "\(peer.log)")
    }

    /// The libzmq authenticator allows one client key (a callback provider, `--zap-curve HEXKEY`). The Swift
    /// client with that key gets in; one with another key is refused with ERROR "400" and nothing arrives.
    @Test(arguments: Role.allCases, [true, false])
    func libzmqZapAllowsOnlyTheListedCurveClientKey(_ role: Role, listed: Bool) async throws {
        let serverKeys = CurveKeyPair.generate()
        let clientKeys = CurveKeyPair.generate()
        var options = interopOptions()
        options.security = .curveClient(serverKey: serverKeys.publicKey, keyPair: clientKeys)
        let push = PushSocket(options: options)
        let log = EventLog(push.events())
        defer { log.stop() }
        let message: Message = ["frame one", "frame two"]
        let allowedKey = listed ? clientKeys : CurveKeyPair.generate()
        var sender: Task<Void, Never>?
        defer { sender?.cancel() }
        let peer = try await interop(
            push, peer: "PULL", role,
            peerArguments: curveServerArguments(serverKeys) + ["--zap-curve", publicKeyHex(allowedKey)]
                + (listed ? ["--recv", "1"] : ["--linger-ms", "100", "--then", "drain:1.5"])
        ) { _ in
            if listed {
                try await within { try await push.send(message) }
            } else {
                sender = trySend(push, message)
                try await log.wait("handshakeFailed") { $0.contains(where: isHandshakeFailed) }
            }
        }
        if listed {
            #expect(peer.received == [bytes(message)], "\(peer.log)")
            #expect(handshakePeerTypes(log.all) == ["PULL"], "\(log.all)")
            #expect(log.all.contains(where: isHandshakeFailed) == false, "\(log.all)")
        } else {
            let reason = handshakeFailure(in: log.all)
            #expect(reason?.contains("400") == true, "reason: \(reason ?? "none"); \(log.all)")
            #expect(log.all.contains(where: isHandshakeSucceeded) == false, "\(log.all)")
            #expect(peer.received.isEmpty, "\(peer.log)")
        }
    }

    // MARK: CURVE, libzmq client -> Swift server

    @Test(arguments: Role.allCases)
    func libzmqCurveClientIsAcceptedBySwiftServer(_ role: Role) async throws {
        let serverKeys = CurveKeyPair.generate()
        let clientKeys = CurveKeyPair.generate()
        let asked = Asked<CurvePublicKey>()
        var options = interopOptions()
        options.security = .curveServer(keyPair: serverKeys) { key in
            await asked.add(key)
            return key == clientKeys.publicKey
        }
        let pull = PullSocket(options: options)
        let log = EventLog(pull.events())
        defer { log.stop() }
        let message: Message = ["frame one", "frame two"]
        var received: Message?
        let peer = try await interop(
            pull, peer: "PUSH", role,
            peerArguments: curveClientArguments(serverKey: serverKeys.publicKey, keys: clientKeys) + [
                "--send", peerMessage(bytes(message)),
            ]
        ) { _ in
            received = try await within { try await pull.receive() }
        }
        #expect(received == message, "\(peer.log)")
        #expect(await asked.values == [clientKeys.publicKey], "\(peer.log)")
        #expect(handshakePeerTypes(log.all) == ["PUSH"], "\(log.all)")
        #expect(log.all.contains(where: isHandshakeFailed) == false, "\(log.all)")
    }

    /// The server allows only one client key; a libzmq client with another pair gets ERROR "400".
    @Test(arguments: Role.allCases)
    func libzmqCurveClientWithAnotherKeyIsRefusedBySwiftServer(_ role: Role) async throws {
        let serverKeys = CurveKeyPair.generate()
        let allowed = CurveKeyPair.generate()
        let intruder = CurveKeyPair.generate()
        let asked = Asked<CurvePublicKey>()
        var options = interopOptions()
        options.security = .curveServer(keyPair: serverKeys) { key in
            await asked.add(key)
            return key == allowed.publicKey
        }
        let pull = PullSocket(options: options)
        let log = EventLog(pull.events())
        defer { log.stop() }
        let peer = try await interop(
            pull, peer: "PUSH", role,
            peerArguments: curveClientArguments(serverKey: serverKeys.publicKey, keys: intruder) + [
                "--linger-ms", "100", "--then", "offer:2.5:" + peerMessage(bytes("never")),
            ]
        ) { _ in
            try await log.wait("handshakeFailed") { $0.contains(where: isHandshakeFailed) }
            await #expect(throws: Timeout.self) { try await within(1) { try await pull.receive() } }
        }
        let reason = handshakeFailure(in: log.all)
        #expect(reason?.contains("authentication failed") == true, "reason: \(reason ?? "none"); \(log.all)")
        #expect(await asked.values.first == intruder.publicKey, "\(peer.log)")
        #expect(log.all.contains(where: isHandshakeSucceeded) == false, "\(log.all)")
        #expect(peer.events.contains { $0.name == "offered" }, "\(peer.log)")
    }

    // MARK: CURVE + PUB / SUB: encrypted SUBSCRIBE commands

    /// Swift SUB (CURVE client) subscribes through the encrypted connection; the libzmq XPUB (CURVE server)
    /// sees `0x01 topic.` and then publishes a message that does not match and one that does.
    @Test(arguments: Role.allCases)
    func swiftCurveSubFromLibzmqXpub(_ role: Role) async throws {
        let serverKeys = CurveKeyPair.generate()
        var options = interopOptions()
        options.security = .curveClient(serverKey: serverKeys.publicKey, keyPair: .generate())
        let sub = SubSocket(options: options)
        try await sub.subscribe("topic.")
        let skipped: Message = ["other", "x"]
        let wanted: Message = ["topic.a", "payload"]
        var received: Message?
        let peer = try await interop(
            sub, peer: "XPUB", role,
            peerArguments: curveServerArguments(serverKeys) + [
                "--wait-sub", "1", "--send", peerMessage(bytes(skipped)), "--send", peerMessage(bytes(wanted)),
            ]
        ) { _ in
            received = try await within { try await sub.receive() }
        }
        #expect(received == wanted, "\(peer.log)")
        #expect(peer.subscriptions == [[[0x01] + bytes("topic.")]], "\(peer.log)")
    }

    /// The libzmq SUB (CURVE client) subscribes to `topic.`; Swift PUB (CURVE server) publishes a message that
    /// does not match and one that does, every 20 ms, until the peer reports a message. Only the matching
    /// one may arrive.
    @Test(arguments: Role.allCases)
    func libzmqCurveSubFromSwiftPub(_ role: Role) async throws {
        let serverKeys = CurveKeyPair.generate()
        var options = interopOptions()
        options.security = .curveServer(keyPair: serverKeys)
        let pub = PubSocket(options: options)
        let skipped: Message = ["other", "x"]
        let wanted: Message = ["topic.a", "payload"]
        let peer = try await interop(
            pub, peer: "SUB", role,
            peerArguments: curveClientArguments(serverKey: serverKeys.publicKey, keys: .generate()) + [
                "--subscribe", hexString(bytes("topic.")), "--recv", "1",
            ]
        ) { peer in
            let publisher = Task {
                while !Task.isCancelled {
                    try await pub.send(skipped)
                    try await pub.send(wanted)
                    try await Task.sleep(for: .milliseconds(20))
                }
            }
            defer { publisher.cancel() }
            try await peer.waitForEvent("recv")
            publisher.cancel()
            _ = await publisher.result
        }
        #expect(peer.received == [bytes(wanted)], "\(peer.log)")
    }

    // MARK: CURVE + heartbeats

    /// The libzmq CURVE client pings every 50 ms and drops the connection after 600 ms without an answer
    /// (its PINGs carry a 1 s TTL). The PINGs arrive in encrypted MESSAGEs and Swift must answer with
    /// PONGs the same way: no `disconnected` for 2 s, and a message still flows afterwards.
    @Test(arguments: Role.allCases)
    func curveHeartbeatsKeepTheConnectionUp(_ role: Role) async throws {
        let serverKeys = CurveKeyPair.generate()
        var options = interopOptions()
        options.security = .curveServer(keyPair: serverKeys)
        let pull = PullSocket(options: options)
        let log = EventLog(pull.events())
        defer { log.stop() }
        let message: Message = ["still", "here"]
        var received: Message?
        // Taken before `interop` closes the socket, which ends the connection with its own `disconnected`.
        var eventsAtTheEnd: [SocketEvent] = []
        let peer = try await interop(
            pull, peer: "PUSH", role,
            peerArguments: curveClientArguments(serverKey: serverKeys.publicKey, keys: .generate()) + [
                // Without our PONGs libzmq would drop the connection about 0.65 s in, well inside the
                // 2 s pause. The pause after the send keeps the peer from closing (and so disconnecting
                // us) before the events are read.
                "--sockopt", "HEARTBEAT_IVL=50", "--sockopt", "HEARTBEAT_TIMEOUT=600",
                "--sockopt", "HEARTBEAT_TTL=1000",
                "--then", "pause:2.0", "--then", "send:" + peerMessage(bytes(message)),
                "--then", "pause:0.5",
            ]
        ) { _ in
            received = try await within { try await pull.receive() }
            eventsAtTheEnd = log.all
        }
        #expect(received == message, "\(peer.log)")
        #expect(eventsAtTheEnd.filter(isDisconnected).isEmpty, "the connection dropped: \(eventsAtTheEnd)")
        #expect(eventsAtTheEnd.filter(isConnectRetried).isEmpty, "\(eventsAtTheEnd)")
        #expect(eventsAtTheEnd.filter(isHandshakeSucceeded).count == 1, "\(eventsAtTheEnd)")
    }
}
#endif
