import NIOCore
import Testing
import ZMTP

// PLAIN against the bytes of a recorded libzmq exchange (fixture plain-push-pull): a PUSH client with
// admin / secret and a PULL server whose ZAP handler accepts it.

private func plainClient(_ type: SocketType, username: String, password: String) -> ZMTPConnection {
    ZMTPConnection(
        configuration: .init(socketType: type, mechanism: .plainClient(username: username, password: password)))
}

private func plainServer(_ type: SocketType) -> ZMTPConnection {
    ZMTPConnection(configuration: .init(socketType: type, mechanism: .plainServer))
}

@Test func plainClientWritesTheBytesOfTheLibzmqClient() throws {
    let fixture = try Fixture.named("plain-push-pull")
    var push = plainClient(.push, username: "admin", password: "secret")
    var sent = written(push.start())
    let outputs = feed(&push, fixture.serverStream)
    sent += written(outputs)
    #expect(events(outputs) == ["ready:PULL"])
    #expect(push.isReady)

    // Greeting, HELLO and INITIATE so far; then the message.
    sent += encoded { push.encodeMessage([ByteBuffer(string: "hello")], into: &$0) }
    #expect(sent == fixture.clientStream)
}

@Test func plainServerAsksForAuthenticationAndWritesTheBytesOfTheLibzmqServer() throws {
    let fixture = try Fixture.named("plain-push-pull")
    var pull = plainServer(.pull)
    var sent = written(pull.start())
    let asked = feed(&pull, fixture.clientStream)
    sent += written(asked)
    // The greeting and HELLO are in; INITIATE and the message wait behind the question.
    #expect(events(asked) == ["authenticate:admin:secret"])
    #expect(!pull.isReady)

    let answered = pull.completeAuthentication(allowed: true)
    sent += written(answered)
    #expect(events(answered) == ["ready:PUSH", "message:hello"])
    #expect(sent == fixture.serverStream)
    #expect(pull.isReady)
}

@Test func plainServerRefusesWithError400() throws {
    var pull = plainServer(.pull)
    _ = pull.start()
    let asked = feed(&pull, try Fixture.named("plain-push-pull").clientStream)
    #expect(events(asked) == ["authenticate:admin:secret"])

    let refused = pull.completeAuthentication(allowed: false)
    #expect(written(refused) == [0x04, 0x0a, 0x05] + Array("ERROR".utf8) + [0x03] + Array("400".utf8))
    #expect(events(refused) == ["closed:authenticationFailed"])
    #expect(!pull.isReady)
    // Nothing more happens after the close, and a second answer is ignored.
    #expect(pull.completeAuthentication(allowed: true) == [])
    #expect(pull.receive(ByteBuffer(string: "x")) == [])
}

@Test func plainCredentialsOver255BytesAreRejected() {
    var connection = plainClient(.push, username: String(repeating: "u", count: 256), password: "p")
    let outputs = connection.start()
    #expect(events(outputs).count == 1)
    #expect(events(outputs).first?.hasPrefix("closed:invalidConfiguration") == true)
    #expect(written(outputs).isEmpty)
}

/// libzmq 4.3.5 names PLAIN's ERROR command with the C string "\x05ERROR", which is the five bytes
/// 5E "RROR". A refusal from a libzmq server arrives like this (seen with a real libzmq: `04 09 5E 52 52 4F
/// 52 03 34 30 30`); the client must report the reason and not a parse error.
@Test func plainClientReadsTheErrorCommandOfLibzmq() throws {
    let greeting = Array(try Fixture.named("plain-push-pull").serverStream.prefix(64))
    var push = plainClient(.push, username: "admin", password: "wrong")
    _ = push.start()
    let refused = feed(&push, greeting + [0x04, 0x09, 0x5E, 0x52, 0x52, 0x4F, 0x52, 0x03] + Array("400".utf8))
    #expect(events(refused) == ["closed:peerError(reason: \"400\")"])
    #expect(!push.isReady)
}

@Test func plainClientRejectsATruncatedErrorCommandOfLibzmq() throws {
    let greeting = Array(try Fixture.named("plain-push-pull").serverStream.prefix(64))
    var push = plainClient(.push, username: "admin", password: "wrong")
    _ = push.start()
    // The reason is announced as 3 bytes but only 2 follow.
    let refused = feed(&push, greeting + [0x04, 0x08, 0x5E, 0x52, 0x52, 0x4F, 0x52, 0x03] + Array("40".utf8))
    #expect(events(refused).first?.hasPrefix("closed:malformedCommand") == true)
}
