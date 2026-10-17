import Foundation
import NIOCore
import Testing
import ZMTP
import ZMTPCrypto

// CURVE (25/ZMTP-CURVE, 26/CurveZMQ). The recorded libzmq exchange (fixture curve-push-pull) pins the
// handshake bytes that do not depend on random values; the rest runs a client and a server against each other.

// MARK: - Helpers

private struct CurveFixtureKeys: Decodable {
    struct Keys: Decodable {
        let serverPublic: String
        let serverSecret: String
        let clientPublic: String
        let clientSecret: String
    }
    let keys: Keys

    static func load() throws -> Keys {
        guard let directory = Bundle.module.url(forResource: "Fixtures", withExtension: nil) else {
            throw CocoaError(.fileNoSuchFile)
        }
        let data = try Data(contentsOf: directory.appendingPathComponent("curve-push-pull.json"))
        return try JSONDecoder().decode(CurveFixtureKeys.self, from: data).keys
    }
}

private func raw(_ z85: String) -> [UInt8] { CurveKeys.bytes(z85: z85) ?? [] }

private struct Pair {
    var client: ZMTPConnection
    var server: ZMTPConnection
    /// Everything either side produced, in order, for `events`.
    var clientOutputs: [Output] = []
    var serverOutputs: [Output] = []
    /// Everything each side wrote.
    var clientWrote: [UInt8] = []
    var serverWrote: [UInt8] = []

    /// A client of `clientType` and a server of `serverType`. `serverKeyForClient` is the server public key
    /// the client believes in (the right one unless a test says otherwise).
    init(
        _ clientType: SocketType, _ serverType: SocketType, clientRoutingID: [UInt8] = [],
        serverKeyForClient: [UInt8]? = nil
    ) {
        let serverKeys = CurveKeys.generate()
        let clientKeys = CurveKeys.generate()
        client = ZMTPConnection(
            configuration: .init(
                socketType: clientType, routingID: clientRoutingID,
                mechanism: .curveClient(
                    serverPublicKey: serverKeyForClient ?? serverKeys.publicKey, publicKey: clientKeys.publicKey,
                    secretKey: clientKeys.secretKey)))
        server = ZMTPConnection(
            configuration: .init(
                socketType: serverType,
                mechanism: .curveServer(publicKey: serverKeys.publicKey, secretKey: serverKeys.secretKey)))
        self.clientPublicKey = clientKeys.publicKey
    }

    let clientPublicKey: [UInt8]

    /// Starts both sides and delivers writes back and forth until nothing is left. The server's question
    /// about the client key is answered with `allow`.
    mutating func handshake(allow: Bool = true) {
        var toServer = ByteBuffer()
        var toClient = ByteBuffer()
        absorbClient(client.start(), &toServer)
        absorbServer(server.start(), &toClient)
        run(&toServer, &toClient, allow: allow)
    }

    /// Delivers `bytes` to the server (or the client) and everything that follows from it.
    mutating func deliverToServer(_ bytes: [UInt8], allow: Bool = true) {
        var toServer = ByteBuffer(bytes: bytes)
        var toClient = ByteBuffer()
        run(&toServer, &toClient, allow: allow)
    }

    mutating func deliverToClient(_ bytes: [UInt8], allow: Bool = true) {
        var toServer = ByteBuffer()
        var toClient = ByteBuffer(bytes: bytes)
        run(&toServer, &toClient, allow: allow)
    }

    private mutating func run(_ toServer: inout ByteBuffer, _ toClient: inout ByteBuffer, allow: Bool) {
        while toServer.readableBytes > 0 || toClient.readableBytes > 0 {
            if toServer.readableBytes > 0 {
                let bytes = toServer
                toServer = ByteBuffer()
                absorbServer(server.receive(bytes), &toClient, allow: allow)
            }
            if toClient.readableBytes > 0 {
                let bytes = toClient
                toClient = ByteBuffer()
                absorbClient(client.receive(bytes), &toServer)
            }
        }
    }

    private mutating func absorbClient(_ outputs: [Output], _ toServer: inout ByteBuffer) {
        clientOutputs += outputs
        for case .write(let buffer) in outputs {
            clientWrote += Array(buffer.readableBytesView)
            toServer.writeImmutableBuffer(buffer)
        }
    }

    private mutating func absorbServer(_ outputs: [Output], _ toClient: inout ByteBuffer, allow: Bool = true) {
        serverOutputs += outputs
        for case .write(let buffer) in outputs {
            serverWrote += Array(buffer.readableBytesView)
            toClient.writeImmutableBuffer(buffer)
        }
        if outputs.contains(where: { if case .authenticate = $0 { true } else { false } }) {
            absorbServer(server.completeAuthentication(allowed: allow), &toClient, allow: allow)
        }
    }

    /// The bytes `connection` writes for one message, as a plain array.
    static func wire(_ connection: inout ZMTPConnection, _ frames: [String]) -> [UInt8] {
        encoded { connection.encodeMessage(frames.map(buffer), into: &$0) }
    }
}

private func contains(_ haystack: [UInt8], _ needle: String) -> Bool {
    let needle = Array(needle.utf8)
    guard haystack.count >= needle.count else { return false }
    return (0...(haystack.count - needle.count)).contains { Array(haystack[$0..<($0 + needle.count)]) == needle }
}

// MARK: - The recorded libzmq exchange

@Test func fixtureHelloOpensWithTheRecordedServerKeyAndHoldsZeros() throws {
    let keys = try CurveFixtureKeys.load()
    let stream = try Fixture.named("curve-push-pull").clientStream
    // greeting (64), frame header (2), name (6: 05 HELLO), version (2), padding (72), C' (32), nonce (8), box (80).
    let hello = 64
    #expect(Array(stream[hello..<(hello + 2)]) == [0x04, 0xc8])
    #expect(Array(stream[(hello + 2)..<(hello + 8)]) == [0x05] + Array("HELLO".utf8))
    let shortTermKey = Array(stream[(hello + 82)..<(hello + 114)])
    let nonce = Array(stream[(hello + 114)..<(hello + 122)])
    let box = Array(stream[(hello + 122)..<(hello + 202)])
    #expect(nonce == [0, 0, 0, 0, 0, 0, 0, 1])

    let plain = CryptoBox.open(
        box, nonce: SecretBoxNonce(prefix: Array("CurveZMQHELLO---".utf8), counter: 1)!,
        senderPublicKey: shortTermKey, recipientSecretKey: raw(keys.serverSecret))
    #expect(plain == [UInt8](repeating: 0, count: 64))
}

@Test func serverAcceptsTheRecordedHelloAndRefusesItsCookieCopy() throws {
    let keys = try CurveFixtureKeys.load()
    let stream = try Fixture.named("curve-push-pull").clientStream
    /// A PULL server with the given pair, its own greeting already sent.
    func server(secret: String, public publicKey: String) -> ZMTPConnection {
        var connection = ZMTPConnection(
            configuration: .init(
                socketType: .pull,
                mechanism: .curveServer(publicKey: raw(publicKey), secretKey: raw(secret))))
        _ = connection.start()
        return connection
    }

    // Our server opens the recorded HELLO and answers with a WELCOME of the size libzmq's has (168 bytes).
    var pull = server(secret: keys.serverSecret, public: keys.serverPublic)
    let hello = Array(stream.prefix(64 + 202))
    let welcome = written(feed(&pull, hello))
    #expect(welcome.count == 2 + 168)
    #expect(Array(welcome.prefix(10)) == [0x04, 0xa8, 0x07] + Array("WELCOME".utf8))
    // The recorded INITIATE carries a cookie that another server made: it is refused, without an ERROR.
    let rest = feed(&pull, Array(stream.dropFirst(64 + 202)))
    #expect(written(rest).isEmpty)
    #expect(events(rest).count == 1 && events(rest).first?.hasPrefix("closed:cryptoError") == true)

    // Another secret key cannot open the HELLO: closed at once, nothing written back.
    var stranger = server(secret: keys.clientSecret, public: keys.clientPublic)
    let refused = feed(&stranger, hello)
    #expect(written(refused).isEmpty)
    #expect(events(refused).first?.hasPrefix("closed:cryptoError") == true)
}

@Test func clientHelloHasTheShapeOfLibzmqsHello() throws {
    let keys = try CurveFixtureKeys.load()
    let stream = try Fixture.named("curve-push-pull").clientStream
    var push = ZMTPConnection(
        configuration: .init(
            socketType: .push,
            mechanism: .curveClient(
                serverPublicKey: raw(keys.serverPublic), publicKey: raw(keys.clientPublic),
                secretKey: raw(keys.clientSecret))))
    var sent = written(push.start())
    sent += written(feed(&push, Array(try Fixture.named("curve-push-pull").serverStream.prefix(64))))
    // The greeting and the HELLO up to C' (version, padding) match libzmq's; the rest is random or boxed.
    #expect(sent.count == stream.prefix(64 + 202).count)
    #expect(Array(sent.prefix(64)) == Array(stream.prefix(64)))
    #expect(Array(sent[64..<(64 + 82)]) == Array(stream[64..<(64 + 82)]))
    // The nonce of the first command is 1.
    #expect(Array(sent[(64 + 114)..<(64 + 122)]) == [0, 0, 0, 0, 0, 0, 0, 1])
}

// MARK: - Client and server together

@Test func handshakeExchangesMetadataAndMessagesTravelEncrypted() {
    var pair = Pair(.dealer, .router, clientRoutingID: Array("dealer-1".utf8))
    pair.handshake()
    #expect(events(pair.clientOutputs) == ["ready:ROUTER"])
    #expect(events(pair.serverOutputs) == ["authenticate:curve:32", "ready:DEALER"])
    #expect(pair.client.isReady && pair.server.isReady)
    let identity = pair.serverOutputs.compactMap { output -> ByteBuffer? in
        if case .ready(let peer, _) = output { return peer.identity }
        return nil
    }
    #expect(identity.map(text) == ["dealer-1"])
    // The server was asked about the client key it proved, and only then said READY.
    #expect(pair.serverOutputs.contains(.authenticate(.curve(publicKey: pair.clientPublicKey))))

    // Two frames from the client, two from the server; neither the text nor a plain frame flag is on the wire.
    let toServer = Pair.wire(&pair.client, ["alpha", "beta"])
    #expect(!contains(toServer, "alpha") && !contains(toServer, "beta"))
    // Two MESSAGE frames of one kind: outer flags 0, body "\x07MESSAGE" + nonce 3 and 4 (HELLO and INITIATE took 1 and 2).
    let message = [UInt8(0x07)] + Array("MESSAGE".utf8)
    #expect(toServer[0] == 0 && Array(toServer[2..<10]) == message)
    #expect(Array(toServer[10..<18]) == [0, 0, 0, 0, 0, 0, 0, 3])
    let second = 2 + Int(toServer[1])
    #expect(toServer[second] == 0 && Array(toServer[(second + 10)..<(second + 18)]) == [0, 0, 0, 0, 0, 0, 0, 4])
    pair.serverOutputs = []
    pair.deliverToServer(toServer)
    #expect(events(pair.serverOutputs) == ["message:alpha|beta"])

    // An empty frame (a MESSAGE with only the flags byte in its box) goes through as well.
    let toClient = Pair.wire(&pair.server, ["gamma", ""])
    #expect(!contains(toClient, "gamma"))
    // The server's READY took nonce 1, so its MESSAGEs start at 2.
    #expect(Array(toClient[10..<18]) == [0, 0, 0, 0, 0, 0, 0, 2])
    pair.clientOutputs = []
    pair.deliverToClient(toClient)
    #expect(events(pair.clientOutputs) == ["message:gamma|"])
}

@Test func subscriptionsAndPingsAreEncryptedCommands() {
    var pair = Pair(.sub, .pub)
    pair.handshake()
    #expect(events(pair.clientOutputs) == ["ready:PUB"])

    let subscribe = encoded { pair.client.encodeSubscribe(buffer("topic"), into: &$0) }
    // A SUBSCRIBE command travels as a MESSAGE in a frame without the COMMAND flag.
    #expect(subscribe[0] == 0 && !contains(subscribe, "SUBSCRIBE") && !contains(subscribe, "topic"))
    pair.serverOutputs = []
    pair.deliverToServer(subscribe)
    #expect(events(pair.serverOutputs) == ["subscribe:topic"])

    let ping = encoded { pair.client.encodePing(ttl: 5, context: buffer("ctx"), into: &$0) }
    pair.serverOutputs = []
    pair.clientOutputs = []
    pair.deliverToServer(ping)
    // The server answered the PING with an encrypted PONG that the client accepted without an event.
    #expect(events(pair.serverOutputs) == ["ping:5"])
    #expect(events(pair.clientOutputs).isEmpty)
    #expect(pair.client.isReady)

    // PUB publishes; SUB receives.
    let published = Pair.wire(&pair.server, ["topic data"])
    pair.deliverToClient(published)
    #expect(events(pair.clientOutputs) == ["message:topic data"])
}

// MARK: - Failures close the connection quietly

@Test func tamperedReplayedAndWrongKeyMessagesCloseWithoutAnError() {
    // A flipped bit in the box.
    var tampered = Pair(.pair, .pair)
    tampered.handshake()
    var wire = Pair.wire(&tampered.client, ["secret"])
    wire[wire.count - 1] ^= 0x01
    let flipped = tampered.server.receive(ByteBuffer(bytes: wire))
    #expect(written(flipped).isEmpty)
    #expect(events(flipped).count == 1 && events(flipped).first?.hasPrefix("closed:cryptoError") == true)
    #expect(tampered.server.receive(ByteBuffer(bytes: wire)).isEmpty)

    // The same MESSAGE twice: the nonce did not increase.
    var replayed = Pair(.pair, .pair)
    replayed.handshake()
    let once = Pair.wire(&replayed.client, ["one"])
    #expect(events(replayed.server.receive(ByteBuffer(bytes: once))) == ["message:one"])
    let again = replayed.server.receive(ByteBuffer(bytes: once))
    #expect(written(again).isEmpty)
    #expect(events(again).first?.hasPrefix("closed:cryptoError") == true)

    // A client that believes in another server key: the server cannot open its HELLO. It says nothing back.
    var wrongKey = Pair(.pair, .pair, serverKeyForClient: CurveKeys.generate().publicKey)
    wrongKey.handshake()
    #expect(wrongKey.serverWrote.count == ZMTP.greetingLength)
    #expect(wrongKey.clientWrote.count == ZMTP.greetingLength + 202)
    #expect(events(wrongKey.serverOutputs).first?.hasPrefix("closed:cryptoError") == true)
    #expect(!wrongKey.client.isReady && !wrongKey.server.isReady)
}

@Test func refusedClientKeyGetsError400() {
    var pair = Pair(.push, .pull)
    pair.handshake(allow: false)
    // After the greeting and WELCOME, the server wrote exactly one more command: ERROR "400".
    let error: [UInt8] = [0x04, 0x0a, 0x05] + Array("ERROR".utf8) + [0x03] + Array("400".utf8)
    #expect(Array(pair.serverWrote.suffix(error.count)) == error)
    #expect(events(pair.serverOutputs) == ["authenticate:curve:32", "closed:authenticationFailed"])
    #expect(events(pair.clientOutputs) == ["closed:peerError(reason: \"400\")"])
    #expect(!pair.server.isReady && !pair.client.isReady)
}
