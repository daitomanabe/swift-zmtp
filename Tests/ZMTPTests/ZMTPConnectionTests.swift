import NIOCore
import Testing
import ZMTP

// MARK: - Helpers

typealias Output = ZMTPConnection.Output

private func connection(
    _ type: SocketType, routingID: [UInt8] = [], maxMessageSize: Int? = nil
) -> ZMTPConnection {
    var connection = ZMTPConnection(
        configuration: .init(socketType: type, routingID: routingID, maxMessageSize: maxMessageSize))
    _ = connection.start()
    return connection
}

/// Feeds a stream to the connection, whole or in chunks of `chunkSize` bytes.
func feed(_ connection: inout ZMTPConnection, _ stream: [UInt8], chunkSize: Int? = nil) -> [Output] {
    var outputs: [Output] = []
    let size = chunkSize ?? max(stream.count, 1)
    var offset = 0
    while offset < stream.count {
        let end = min(offset + size, stream.count)
        outputs += connection.receive(ByteBuffer(bytes: stream[offset..<end]))
        offset = end
    }
    return outputs
}

func text(_ buffer: ByteBuffer) -> String {
    String(decoding: buffer.readableBytesView, as: UTF8.self)
}

/// Everything except `.write`, as short strings: "ready:PULL", "message:a|b", "subscribe:w", "closed:...".
func events(_ outputs: [Output]) -> [String] {
    outputs.compactMap { output in
        switch output {
        case .write: nil
        case .ready(let peer, _): "ready:\(peer.socketType ?? "?")"
        case .authenticate(.plain(let username, let password)): "authenticate:\(username):\(password)"
        case .message(let frames): "message:" + frames.map(text).joined(separator: "|")
        case .subscribe(let topic): "subscribe:\(text(topic))"
        case .cancel(let topic): "cancel:\(text(topic))"
        case .pingReceived(let ttl): "ping:\(ttl)"
        case .closed(let error): "closed:\(error)"
        }
    }
}

func written(_ outputs: [Output]) -> [UInt8] {
    outputs.flatMap { output -> [UInt8] in
        if case .write(let buffer) = output { return Array(buffer.readableBytesView) }
        return []
    }
}

func encoded(_ body: (inout ByteBuffer) -> Void) -> [UInt8] {
    var buffer = ByteBuffer()
    body(&buffer)
    return Array(buffer.readableBytesView)
}

func buffer(_ string: String) -> ByteBuffer { ByteBuffer(string: string) }

struct HandshakeCase: Sendable, CustomTestStringConvertible {
    var fixture: String
    var type: SocketType
    var routingID: [UInt8] = []
    var testDescription: String { "\(fixture) as \(type.rawValue)" }
}

// MARK: - Handshake bytes

@Test(arguments: [
    HandshakeCase(fixture: "null-push-pull", type: .push),
    HandshakeCase(fixture: "null-req-rep", type: .req),
    HandshakeCase(fixture: "null-dealer-router", type: .dealer, routingID: Array("dealer-1".utf8)),
])
func ourGreetingAndReadyMatchLibzmq(_ testCase: HandshakeCase) throws {
    let fixture = try Fixture.named(testCase.fixture)
    var connection = ZMTPConnection(
        configuration: .init(socketType: testCase.type, routingID: testCase.routingID))
    var sent = written(connection.start())
    sent += written(connection.receive(ByteBuffer(bytes: fixture.serverStream)))

    // The client stream opens with greeting + READY (a short command frame), then messages.
    let readyLength = 2 + Int(fixture.clientStream[ZMTP.greetingLength + 1])
    #expect(sent.count == ZMTP.greetingLength + readyLength)
    #expect(sent == Array(fixture.clientStream.prefix(sent.count)))
}

@Test func pushSeesPullReady() throws {
    var push = connection(.push)
    let outputs = feed(&push, try Fixture.named("null-push-pull").serverStream)
    let ready = outputs.compactMap { output -> (peer: Metadata, minor: UInt8)? in
        if case .ready(let peer, let minor) = output { return (peer, minor) }
        return nil
    }
    #expect(ready.count == 1)
    #expect(ready.first?.peer.socketType == "PULL")
    #expect(ready.first?.peer.identity == nil)
    #expect(ready.first?.minor == 1)
    #expect(push.isReady)
}

// MARK: - Receiving messages

@Test(arguments: [nil, 1] as [Int?])
func pullReadsMessagesFromPush(chunkSize: Int?) throws {
    var pull = connection(.pull)
    let outputs = feed(&pull, try Fixture.named("null-push-pull").clientStream, chunkSize: chunkSize)
    #expect(
        events(outputs) == [
            "ready:PUSH",
            "message:hello",
            "message:part1|part2",
            "message:" + String(repeating: "x", count: 300),
        ])
}

@Test func pushEncodesMessagesLikeLibzmq() throws {
    let fixture = try Fixture.named("null-push-pull")
    var push = connection(.push)
    _ = feed(&push, fixture.serverStream)

    let sent = encoded { buffer in
        push.encodeMessage([ByteBuffer(string: "hello")], into: &buffer)
        push.encodeMessage([ByteBuffer(string: "part1"), ByteBuffer(string: "part2")], into: &buffer)
        push.encodeMessage([ByteBuffer(string: String(repeating: "x", count: 300))], into: &buffer)
    }
    #expect(sent == Array(fixture.clientStream.suffix(sent.count)))
}

@Test func oversizedFrameClosesTheConnection() throws {
    var pull = connection(.pull, maxMessageSize: 100)
    let outputs = feed(&pull, try Fixture.named("null-push-pull").clientStream)
    #expect(
        events(outputs) == [
            "ready:PUSH", "message:hello", "message:part1|part2", "closed:messageTooLarge",
        ])
}

// MARK: - Subscriptions

@Test(arguments: ["null-pub-sub", "zmtp30-sub"])
func pubSeesSubscribeAndCancel(_ name: String) throws {
    // null-pub-sub: SUBSCRIBE/CANCEL commands. zmtp30-sub: data frames starting with 0x01 / 0x00.
    var pub = connection(.pub)
    let outputs = feed(&pub, try Fixture.named(name).clientStream)
    #expect(events(outputs) == ["ready:SUB", "subscribe:w", "cancel:w"])
}

@Test func subscriptionFormFollowsThePeerVersion() throws {
    // A ZMTP 3.0 peer gets a data frame starting with 0x01 / 0x00.
    var oldPeer = connection(.sub)
    let oldOutputs = feed(&oldPeer, try Fixture.named("zmtp30-sub").serverStream)
    #expect(events(oldOutputs) == ["ready:PUB", "message:w hello"])
    #expect(oldPeer.peerMinorVersion == 0)
    #expect(encoded { oldPeer.encodeSubscribe(buffer("w"), into: &$0) } == [0x00, 0x02, 0x01, 0x77])
    #expect(encoded { oldPeer.encodeCancel(buffer("w"), into: &$0) } == [0x00, 0x02, 0x00, 0x77])

    // A ZMTP 3.1 peer gets commands. SUBSCRIBE: size = 1 + 9 + 1 (DESIGN.md says 0x0a; the fixture has 0x0b).
    let fixture = try Fixture.named("null-pub-sub")
    var newPeer = connection(.sub)
    _ = feed(&newPeer, fixture.serverStream)
    #expect(newPeer.peerMinorVersion == 1)
    let subscribe = encoded { newPeer.encodeSubscribe(buffer("w"), into: &$0) }
    let cancel = encoded { newPeer.encodeCancel(buffer("w"), into: &$0) }
    #expect(subscribe == [0x04, 0x0b, 0x09] + Array("SUBSCRIBE".utf8) + [0x77])
    #expect(cancel == [0x04, 0x08, 0x06] + Array("CANCEL".utf8) + [0x77])
    #expect(subscribe + cancel == Array(fixture.clientStream.suffix(subscribe.count + cancel.count)))
}

// MARK: - Heartbeat

@Test func pingIsAnsweredWithPong() throws {
    var pull = connection(.pull)
    let outputs = feed(&pull, try Fixture.named("null-heartbeat").clientStream)
    #expect(events(outputs) == ["ready:PUSH", "message:hello", "ping:5"])
    guard case .write(let pong)? = outputs.last else {
        Issue.record("the last output is not a write: \(outputs)")
        return
    }
    #expect(Array(pong.readableBytesView) == [0x04, 0x05, 0x04] + Array("PONG".utf8))
}

// MARK: - Errors

@Test func greetingStartingWithZeroIsABadSignature() throws {
    var stream = try Fixture.named("null-push-pull").clientStream
    stream[0] = 0x00
    var pull = connection(.pull)
    #expect(feed(&pull, stream) == [.closed(.badSignature)])
    // Nothing follows `.closed`.
    #expect(pull.receive(buffer("more")) == [])
}

@Test func plainMechanismIsRefusedByNull() throws {
    var push = connection(.push)
    let outputs = feed(&push, try Fixture.named("plain-push-pull").serverStream)
    #expect(outputs == [.closed(.mechanismMismatch(local: "NULL", peer: "PLAIN"))])
}

@Test func incompatibleSocketTypeGetsAnErrorCommand() throws {
    // Another PUSH's READY arrives at a PUSH.
    var push = connection(.push)
    let outputs = feed(&push, try Fixture.named("null-push-pull").clientStream)
    let error = [0x04, 0x1a, 0x05] + Array("ERROR".utf8) + [0x13] + Array("Invalid socket type".utf8)
    #expect(outputs.count == 3)  // our READY, the ERROR, closed
    #expect(written([outputs[1]]) == error)
    #expect(outputs.last == .closed(.incompatibleSocketType(local: .push, peer: "PUSH")))
    #expect(!push.isReady)
}
