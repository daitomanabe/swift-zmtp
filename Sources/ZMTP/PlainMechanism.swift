import NIOCore

// PLAIN (24/ZMTP-PLAIN). The handshake, client side then server side:
//
//   client                              server
//   HELLO(username, password)  ------>
//                                       (the caller checks the credentials)
//                              <------  WELCOME            or   ERROR "400" and close
//   INITIATE(metadata)         ------>
//                              <------  READY(metadata)
//
// The commands are carried as `Command.unknown`: HELLO, WELCOME and INITIATE have a body that depends
// on the mechanism, so `Command` cannot parse them by name. After READY frames travel unchanged.

private enum PlainCommand {
    static let hello = "HELLO"
    static let welcome = "WELCOME"
    static let initiate = "INITIATE"
}

/// Longest user name or password: each is sent with a one-byte length.
let plainCredentialLimit = 255

/// The ERROR reason a PLAIN server sends when the credentials are refused (like libzmq's ZAP status).
let plainDeniedReason = "400"

/// Parses the body of a command received during the PLAIN handshake.
///
/// One difference from 24/ZMTP-PLAIN is accepted here: libzmq 4.3.5 declares the name of PLAIN's ERROR
/// command as the C string "\x05ERROR". A hexadecimal escape takes every hex digit after it, and `E` is
/// one, so the string is the five bytes 0x5E "RROR" and not 0x05 "ERROR". A libzmq server that refuses a
/// login therefore sends `5E 52 52 4F 52`, a reason length and the reason. Read as an ordinary command
/// that is a name longer than the frame. We read it as ERROR, so the application sees the reason (for
/// example "400") and not a parse error.
///
/// We still send the form of the specification. A libzmq PLAIN client expects its own form and reports
/// ours as an unexpected command; it closes the connection either way.
func plainHandshakeCommand(_ body: ByteBuffer) throws(ZMTPError) -> Command {
    let libzmqErrorName: [UInt8] = [0x5E, 0x52, 0x52, 0x4F, 0x52]
    guard body.readableBytesView.starts(with: libzmqErrorName) else { return try Command(body: body) }
    var data = body
    data.moveReaderIndex(forwardBy: libzmqErrorName.count)
    guard let reasonLength: UInt8 = data.readInteger(), let reason = data.readString(length: Int(reasonLength)) else {
        throw .malformedCommand("truncated ERROR reason")
    }
    return .error(reason: reason)
}

/// The connecting side of PLAIN.
struct PlainClient: Sendable {
    private enum State: Sendable {
        case idle, awaitingWelcome, awaitingReady, done
    }

    private let username: [UInt8]
    private let password: [UInt8]
    private var state = State.idle
    /// Our Socket-Type (and Identity), sent in INITIATE.
    private var local = Metadata()

    init(username: [UInt8], password: [UInt8]) {
        self.username = username
        self.password = password
    }

    func validate() throws(ZMTPError) {
        if username.count > plainCredentialLimit {
            throw .invalidConfiguration("the PLAIN user name is longer than \(plainCredentialLimit) bytes")
        }
        if password.count > plainCredentialLimit {
            throw .invalidConfiguration("the PLAIN password is longer than \(plainCredentialLimit) bytes")
        }
    }

    mutating func begin(local: Metadata) -> [Command] {
        self.local = local
        state = .awaitingWelcome
        var body = ByteBuffer()
        body.writeInteger(UInt8(username.count))
        body.writeBytes(username)
        body.writeInteger(UInt8(password.count))
        body.writeBytes(password)
        return [.unknown(name: PlainCommand.hello, body: body)]
    }

    mutating func receive(_ command: Command) throws(ZMTPError) -> Mechanism.HandshakeStep {
        if case .error(let reason) = command { throw .peerError(reason: reason) }
        switch state {
        case .awaitingWelcome:
            // libzmq ignores whatever follows the name; so do we.
            guard case .unknown(let name, _) = command, name == PlainCommand.welcome else {
                throw .unexpected("expected WELCOME, got \(command.name)")
            }
            state = .awaitingReady
            var body = ByteBuffer()
            local.write(into: &body)
            return .send([.unknown(name: PlainCommand.initiate, body: body)])
        case .awaitingReady:
            guard case .ready(let peer) = command else {
                throw .unexpected("expected READY, got \(command.name)")
            }
            state = .done
            return .complete(peer: peer, send: [])
        case .idle, .done:
            throw .unexpected("unexpected \(command.name)")
        }
    }
}

/// The accepting side of PLAIN. It does not check credentials itself: it hands them out as an
/// `AuthenticationRequest` and waits in `.authenticating` for the answer.
struct PlainServer: Sendable {
    private enum State: Sendable {
        case idle, awaitingHello, authenticating, awaitingInitiate, done
    }

    private var state = State.idle
    /// Our Socket-Type (and Identity), sent in READY.
    private var local = Metadata()

    mutating func begin(local: Metadata) -> [Command] {
        self.local = local
        state = .awaitingHello
        return []
    }

    mutating func receive(_ command: Command) throws(ZMTPError) -> Mechanism.HandshakeStep {
        if case .error(let reason) = command { throw .peerError(reason: reason) }
        switch state {
        case .awaitingHello:
            guard case .unknown(let name, let body) = command, name == PlainCommand.hello else {
                throw .unexpected("expected HELLO, got \(command.name)")
            }
            var data = body
            guard let usernameLength: UInt8 = data.readInteger(),
                let username = data.readSlice(length: Int(usernameLength)),
                let passwordLength: UInt8 = data.readInteger(),
                let password = data.readSlice(length: Int(passwordLength)),
                data.readableBytes == 0
            else {
                throw .malformedCommand("malformed HELLO")
            }
            state = .authenticating
            return .authenticate(
                .plain(
                    username: String(decoding: username.readableBytesView, as: UTF8.self),
                    password: String(decoding: password.readableBytesView, as: UTF8.self)))
        case .awaitingInitiate:
            guard case .unknown(let name, let body) = command, name == PlainCommand.initiate else {
                throw .unexpected("expected INITIATE, got \(command.name)")
            }
            var data = body
            let peer = try Metadata.parse(&data)
            state = .done
            return .complete(peer: peer, send: [.ready(local)])
        case .idle, .authenticating, .done:
            throw .unexpected("unexpected \(command.name)")
        }
    }

    /// The caller's answer to the `.plain` request: WELCOME to go on, or ERROR "400" and close.
    mutating func completeAuthentication(allowed: Bool) throws(ZMTPError) -> Mechanism.HandshakeStep {
        guard state == .authenticating else { throw .unexpected("no authentication is pending") }
        if allowed {
            state = .awaitingInitiate
            return .send([.unknown(name: PlainCommand.welcome, body: ByteBuffer())])
        }
        state = .done
        return .reject(send: [.error(reason: plainDeniedReason)], error: .authenticationFailed)
    }
}
