import NIOCore
import ZMTPCrypto

// CURVE (25/ZMTP-CURVE over 26/CurveZMQ). The handshake, client side then server side:
//
//   client                                      server
//   HELLO(C', box[64 zeros](C'->S))   ------>
//                                     <------   WELCOME(box[S', cookie](S->C'))
//   INITIATE(cookie, box[C, vouch,
//            metadata](C'->S'))       ------>   (the caller decides about C)
//                                     <------   READY(box[metadata](S'->C'))   or   ERROR "400" and close
//   MESSAGE ...                       <----->   MESSAGE ...
//
// C, S are the long-term public keys, C', S' short-term keys made for this connection only. Every box is a
// NaCl crypto_box in the "easy" layout (16-byte MAC, then the ciphertext). The boxes C'->S, S->C' and the
// WELCOME / HELLO use one shared key (X25519 of c' and S, which is also that of s and C'); the boxes
// C'->S' and S'->C' (INITIATE, READY, MESSAGE) use another one (X25519 of c' and S'), computed once.
//
// A command body is the command name (1-byte length + name) and then the data, laid out as in 26/CurveZMQ.
// The commands are carried as `Command.unknown` because READY here is a box, not metadata.
//
// After READY every frame, data or command, is one MESSAGE command in a normal frame (outer flags 0):
//   "\x07MESSAGE" | short nonce (8, big-endian) | box[ flags (1) | payload ]
// with inner flags bit 0 = MORE and bit 1 = COMMAND, as libzmq writes it. A failure of any check closes the
// connection without an ERROR command; only a refusal of the client key sends ERROR "400".

private enum CurveCommand {
    static let hello = "HELLO"
    static let welcome = "WELCOME"
    static let initiate = "INITIATE"
    static let ready = "READY"
    static let error = "ERROR"
}

/// The 24-byte nonces are a prefix and a counter or a random value (26/CurveZMQ).
private enum CurveNonce {
    // Short-nonce commands: 16-byte prefix + 8-byte big-endian counter.
    static let hello = Array("CurveZMQHELLO---".utf8)
    static let initiate = Array("CurveZMQINITIATE".utf8)
    static let ready = Array("CurveZMQREADY---".utf8)
    static let messageFromClient = Array("CurveZMQMESSAGEC".utf8)
    static let messageFromServer = Array("CurveZMQMESSAGES".utf8)
    // Long-nonce boxes: 8-byte prefix + 16 random bytes sent in the clear next to the box.
    static let welcome = Array("WELCOME-".utf8)
    static let cookie = Array("COOKIE--".utf8)
    static let vouch = Array("VOUCH---".utf8)
}

private enum CurveSize {
    static let key = 32
    static let shortNonce = 8
    static let longNonce = 16
    static let mac = 16
    /// Long nonce + secretbox of (C' + s'), or of (C' + S) for the vouch.
    static let cookie = longNonce + mac + 2 * key
    static let vouch = longNonce + mac + 2 * key
    /// HELLO after the name: version (2), padding (72), C' (32), nonce (8), box of 64 zeros (80).
    static let helloData = 2 + 72 + key + shortNonce + mac + 64
    /// WELCOME after the name: long nonce (16), box of S' + cookie (16 + 32 + 96).
    static let welcomeData = longNonce + mac + key + cookie
    /// INITIATE after the name, at least: cookie, nonce, box of C + vouch (and empty metadata).
    static let initiateDataMinimum = cookie + shortNonce + mac + key + vouch
}

/// The ZMTP error for a failed cryptographic check.
private func cryptoFailure(_ message: String) -> ZMTPError { .cryptoError(message) }

private func longNonce(prefix: [UInt8], random: [UInt8]) -> SecretBoxNonce {
    guard let nonce = SecretBoxNonce(prefix: prefix, suffix: random) else {
        preconditionFailure("a CURVE long nonce is an 8-byte prefix and 16 bytes")
    }
    return nonce
}

private func shortNonce(prefix: [UInt8], counter: UInt64) -> SecretBoxNonce {
    guard let nonce = SecretBoxNonce(prefix: prefix, counter: counter) else {
        preconditionFailure("a CURVE short nonce is a 16-byte prefix and a counter")
    }
    return nonce
}

/// Splits a handshake command body into its name and the data after it. The data is not parsed: it is a box.
private func splitHandshake(_ body: ByteBuffer) throws(ZMTPError) -> (name: String, data: ByteBuffer) {
    var data = body
    guard let nameLength: UInt8 = data.readInteger(), let name = data.readString(length: Int(nameLength)) else {
        throw .malformedCommand("truncated command name")
    }
    return (name, data)
}

/// The error for an ERROR command received during the handshake (the data is reason length + reason).
private func peerError(_ data: ByteBuffer) -> ZMTPError {
    var data = data
    guard let length: UInt8 = data.readInteger(), let reason = data.readString(length: Int(length)) else {
        return .malformedCommand("truncated ERROR reason")
    }
    return .peerError(reason: reason)
}

/// What is wrong with a long-term key pair, or nil.
private func longTermKeyProblem(publicKey: [UInt8], secretKey: [UInt8]) -> String? {
    guard publicKey.count == CurveSize.key, secretKey.count == CurveSize.key else {
        return "a CURVE key must be 32 bytes"
    }
    guard X25519.publicKey(secretKey: secretKey) == publicKey else {
        return "the CURVE public key does not belong to the secret key"
    }
    return nil
}

private func metadataBytes(_ metadata: Metadata) -> [UInt8] {
    var buffer = ByteBuffer()
    metadata.write(into: &buffer)
    return Array(buffer.readableBytesView)
}

// MARK: - After the handshake

/// The state of an established CURVE connection: the shared key and the two nonce counters. Every frame
/// is one MESSAGE.
struct CurveSession: Sendable {
    /// `"\x07MESSAGE"`: the command name of every frame.
    private static let messageName: [UInt8] = [0x07] + Array("MESSAGE".utf8)
    /// Name (8) + short nonce (8) + MAC (16) + flags (1): the smallest valid MESSAGE body.
    private static let minimumBody = 8 + CurveSize.shortNonce + CurveSize.mac + 1

    /// X25519(c', S') = X25519(s', C') through HSalsa20 (crypto_box_beforenm), computed once.
    private let key: SecretBoxKey
    private let isClient: Bool
    /// The nonce of the next MESSAGE we send. The handshake used the first ones (client: HELLO 1, INITIATE 2;
    /// server: READY 1).
    private var nextSendNonce: UInt64
    /// The last nonce received; the next one must be larger (so a replayed or reordered MESSAGE is refused).
    private var lastReceivedNonce: UInt64

    init(key: SecretBoxKey, isClient: Bool, nextSendNonce: UInt64, lastReceivedNonce: UInt64) {
        self.key = key
        self.isClient = isClient
        self.nextSendNonce = nextSendNonce
        self.lastReceivedNonce = lastReceivedNonce
    }

    /// Writes `frame` as a MESSAGE: the frame header, the command name and nonce, then the box built in
    /// the output buffer itself. The payload is copied once (into the space after the 16 bytes the MAC
    /// will fill) and encrypted in place, so there is no intermediate buffer.
    mutating func encode(_ frame: WireFrame, into buffer: inout ByteBuffer) {
        // 2^64 messages are out of reach (centuries at 10^9 per second); stop rather than reuse a nonce.
        precondition(nextSendNonce < UInt64.max, "the CURVE nonce space is used up")
        let counter = nextSendNonce
        nextSendNonce += 1

        let payloadLength = frame.body.readableBytes
        let boxLength = CurveSize.mac + 1 + payloadLength
        FrameCodec.writeHeader(
            flags: [], length: Self.messageName.count + CurveSize.shortNonce + boxLength, into: &buffer)
        buffer.writeBytes(Self.messageName)
        buffer.writeInteger(counter)

        var innerFlags: UInt8 = 0
        if frame.flags.contains(.more) { innerFlags |= 0x01 }
        if frame.flags.contains(.command) { innerFlags |= 0x02 }
        let nonce = shortNonce(
            prefix: isClient ? CurveNonce.messageFromClient : CurveNonce.messageFromServer, counter: counter)
        let key = self.key
        buffer.writeWithUnsafeMutableBytes(minimumWritableBytes: boxLength) { writable in
            let box = UnsafeMutableRawBufferPointer(rebasing: writable[..<boxLength])
            box[CurveSize.mac] = innerFlags
            frame.body.withUnsafeReadableBytes { payload in
                if !payload.isEmpty {
                    UnsafeMutableRawBufferPointer(rebasing: box[(CurveSize.mac + 1)...]).copyMemory(from: payload)
                }
            }
            SecretBox.sealInPlace(box, nonce: nonce, key: key)
            return boxLength
        }
    }

    /// Opens a MESSAGE. The outer flags are not looked at (libzmq sends 0 but a peer may set COMMAND); the
    /// inner flags decide. The plaintext is made in a new buffer in one pass (the received bytes are
    /// shared with the connection's input, so decrypting them in place would copy them anyway), and the
    /// frame body is a slice of it.
    mutating func decode(_ frame: WireFrame) throws(ZMTPError) -> WireFrame {
        let body = frame.body
        guard body.readableBytes >= Self.minimumBody else {
            throw .malformedFrame("a CURVE MESSAGE is shorter than \(Self.minimumBody) bytes")
        }
        let isMessage = body.withUnsafeReadableBytes { raw in
            raw.prefix(Self.messageName.count).elementsEqual(Self.messageName)
        }
        guard isMessage else { throw .unexpected("expected MESSAGE after the CURVE handshake") }
        guard let counter: UInt64 = body.getInteger(at: body.readerIndex + Self.messageName.count) else {
            throw .malformedFrame("truncated MESSAGE nonce")
        }
        // Checked before the box is opened: a replay costs nothing. Equal or smaller closes the connection.
        guard counter > lastReceivedNonce else {
            throw cryptoFailure("the nonce of a MESSAGE did not increase")
        }

        let headerLength = Self.messageName.count + CurveSize.shortNonce
        let plainLength = body.readableBytes - headerLength - CurveSize.mac
        let nonce = shortNonce(
            prefix: isClient ? CurveNonce.messageFromServer : CurveNonce.messageFromClient, counter: counter)
        var plain = ByteBufferAllocator().buffer(capacity: plainLength)
        var opened = false
        let key = self.key
        plain.writeWithUnsafeMutableBytes(minimumWritableBytes: plainLength) { writable in
            body.withUnsafeReadableBytes { all in
                opened = SecretBox.open(
                    UnsafeRawBufferPointer(rebasing: all[headerLength...]), nonce: nonce, key: key,
                    into: UnsafeMutableRawBufferPointer(rebasing: writable[..<plainLength]))
            }
            return opened ? plainLength : 0
        }
        guard opened else { throw cryptoFailure("a MESSAGE did not open") }
        lastReceivedNonce = counter

        // plainLength >= 1 by the size check above.
        guard let innerFlags: UInt8 = plain.readInteger() else { throw cryptoFailure("a MESSAGE has no flags") }
        var flags: FrameFlags = []
        if innerFlags & 0x01 != 0 { flags.insert(.more) }
        if innerFlags & 0x02 != 0 { flags.insert(.command) }
        if flags.contains(.more) && flags.contains(.command) {
            throw .malformedFrame("a command cannot have MORE set")
        }
        return WireFrame(flags: flags, body: plain)
    }
}

// MARK: - Client

/// The connecting side of CURVE (the one that knows the server's public key).
struct CurveClient: Sendable {
    private enum State: Sendable {
        case idle, awaitingWelcome, awaitingReady, done
    }

    /// S: the server's long-term public key.
    private let serverKey: [UInt8]
    /// C and c: our long-term pair.
    private let publicKey: [UInt8]
    private let secretKey: [UInt8]
    /// Set by `init` when the keys cannot be used; `validate()` reports it before anything is sent.
    private let configurationError: String?
    /// C' and c': new for this connection.
    private let shortTerm: X25519KeyPair?
    /// crypto_box_beforenm(S, c'), which is also the key of the server's WELCOME box (C' with s).
    private let helloKey: SecretBoxKey?

    private var state = State.idle
    /// Our Socket-Type (and Identity), sent in INITIATE.
    private var local = Metadata()
    /// The nonce of the next short-nonce command we send.
    private var nextNonce: UInt64 = 1
    /// crypto_box_beforenm(S', c'), known from WELCOME on.
    private var sessionKey: SecretBoxKey?
    private var finished: CurveSession?

    init(serverPublicKey: [UInt8], publicKey: [UInt8], secretKey: [UInt8]) {
        self.serverKey = serverPublicKey
        self.publicKey = publicKey
        self.secretKey = secretKey
        var problem: String?
        var pair: X25519KeyPair?
        var key: SecretBoxKey?
        if serverPublicKey.count != CurveSize.key {
            problem = "a CURVE key must be 32 bytes"
        } else if let keyProblem = longTermKeyProblem(publicKey: publicKey, secretKey: secretKey) {
            problem = keyProblem
        } else {
            let generated = X25519KeyPair.generate()
            pair = generated
            key = CryptoBox.precompute(publicKey: serverPublicKey, secretKey: generated.secretKey)
            if key == nil { problem = "the CURVE server key cannot be used for a key agreement" }
        }
        self.configurationError = problem
        self.shortTerm = pair
        self.helloKey = key
    }

    func validate() throws(ZMTPError) {
        if let configurationError { throw .invalidConfiguration(configurationError) }
    }

    /// The session, once, when the handshake is over.
    mutating func takeSession() -> CurveSession? {
        defer { finished = nil }
        return finished
    }

    private mutating func takeNonce() -> UInt64 {
        defer { nextNonce += 1 }
        return nextNonce
    }

    /// HELLO: version 1.0, 72 bytes of padding, C', the nonce, and a box of 64 zeros from C' to S.
    mutating func begin(local: Metadata) -> [Command] {
        self.local = local
        guard let shortTerm, let helloKey else { return [] }
        state = .awaitingWelcome
        let counter = takeNonce()
        let box = SecretBox.seal(
            [UInt8](repeating: 0, count: 64), nonce: shortNonce(prefix: CurveNonce.hello, counter: counter),
            key: helloKey)
        var body = ByteBuffer()
        body.writeBytes([1, 0])
        body.writeRepeatingByte(0, count: 72)
        body.writeBytes(shortTerm.publicKey)
        body.writeInteger(counter)
        body.writeBytes(box)
        return [.unknown(name: CurveCommand.hello, body: body)]
    }

    mutating func receive(handshake body: ByteBuffer) throws(ZMTPError) -> Mechanism.HandshakeStep {
        let (name, data) = try splitHandshake(body)
        if name == CurveCommand.error { throw peerError(data) }
        switch state {
        case .awaitingWelcome:
            guard name == CurveCommand.welcome else { throw .unexpected("expected WELCOME, got \(name)") }
            return try receiveWelcome(data)
        case .awaitingReady:
            guard name == CurveCommand.ready else { throw .unexpected("expected READY, got \(name)") }
            return try receiveReady(data)
        case .idle, .done:
            throw .unexpected("unexpected \(name)")
        }
    }

    /// WELCOME: opens with S (so the server has our HELLO key and is the one we meant) and gives S' and the
    /// cookie. We answer with INITIATE, which carries the cookie back, our long-term key C, and a vouch
    /// (C' and S in a box from C to S') that proves C' belongs to the owner of C.
    private mutating func receiveWelcome(_ data: ByteBuffer) throws(ZMTPError) -> Mechanism.HandshakeStep {
        var data = data
        guard data.readableBytes == CurveSize.welcomeData,
            let nonceRandom = data.readBytes(length: CurveSize.longNonce),
            let box = data.readBytes(length: data.readableBytes),
            let helloKey, let shortTerm
        else {
            throw .malformedCommand("malformed WELCOME")
        }
        guard
            let plain = SecretBox.open(
                box, nonce: longNonce(prefix: CurveNonce.welcome, random: nonceRandom), key: helloKey)
        else {
            throw cryptoFailure("the WELCOME box did not open (wrong server key?)")
        }
        // plain: S' (32) then the cookie (96).
        let serverShortTermKey = Array(plain[0..<CurveSize.key])
        let cookie = Array(plain[CurveSize.key..<(CurveSize.key + CurveSize.cookie)])
        guard let session = CryptoBox.precompute(publicKey: serverShortTermKey, secretKey: shortTerm.secretKey),
            let vouchKey = CryptoBox.precompute(publicKey: serverShortTermKey, secretKey: secretKey)
        else {
            throw cryptoFailure("the server short-term key cannot be used for a key agreement")
        }
        sessionKey = session

        // Vouch: box[C' + S](C -> S') under a fresh random long nonce.
        let vouchRandom = SecureRandom.bytes(CurveSize.longNonce)
        let vouchBox = SecretBox.seal(
            shortTerm.publicKey + serverKey, nonce: longNonce(prefix: CurveNonce.vouch, random: vouchRandom),
            key: vouchKey)

        let counter = takeNonce()
        let initiateBox = SecretBox.seal(
            publicKey + vouchRandom + vouchBox + metadataBytes(local),
            nonce: shortNonce(prefix: CurveNonce.initiate, counter: counter), key: session)
        var body = ByteBuffer()
        body.writeBytes(cookie)
        body.writeInteger(counter)
        body.writeBytes(initiateBox)
        state = .awaitingReady
        return .send([.unknown(name: CurveCommand.initiate, body: body)])
    }

    /// READY: a box from S' to C' with the server's metadata. Its nonce counts as the first one received.
    private mutating func receiveReady(_ data: ByteBuffer) throws(ZMTPError) -> Mechanism.HandshakeStep {
        var data = data
        guard data.readableBytes >= CurveSize.shortNonce + CurveSize.mac,
            let counter: UInt64 = data.readInteger(),
            let box = data.readBytes(length: data.readableBytes),
            let sessionKey
        else {
            throw .malformedCommand("malformed READY")
        }
        guard
            let plain = SecretBox.open(
                box, nonce: shortNonce(prefix: CurveNonce.ready, counter: counter), key: sessionKey)
        else {
            throw cryptoFailure("the READY box did not open")
        }
        var metadata = ByteBuffer(bytes: plain)
        let peer = try Metadata.parse(&metadata)
        state = .done
        finished = CurveSession(
            key: sessionKey, isClient: true, nextSendNonce: nextNonce, lastReceivedNonce: counter)
        self.sessionKey = nil
        return .complete(peer: peer, send: [])
    }
}

// MARK: - Server

/// The accepting side of CURVE. Like PLAIN's server it hands the client's identity out as an
/// `AuthenticationRequest` and waits in `.authenticating` for the answer. Everything it needs to check
/// the client lives in this connection: a stateless server (the cookie idea of 26/CurveZMQ) would not
/// help when one value serves one connection.
struct CurveServer: Sendable {
    private enum State: Sendable {
        case idle, awaitingHello, awaitingInitiate, authenticating, done
    }

    /// S and s: our long-term pair.
    private let publicKey: [UInt8]
    private let secretKey: [UInt8]
    private let configurationError: String?

    private var state = State.idle
    /// Our Socket-Type (and Identity), sent in READY.
    private var local = Metadata()

    // Per connection, made at HELLO.
    /// S' and s'; s' is dropped when INITIATE has been checked.
    private var shortTerm: X25519KeyPair?
    /// The key that seals and opens our cookie; dropped when INITIATE has been checked.
    private var cookieKey: SecretBoxKey?
    /// C': the client's short-term key from HELLO.
    private var clientShortTermKey: [UInt8] = []
    /// crypto_box_beforenm(C', s'): INITIATE, READY and MESSAGE.
    private var sessionKey: SecretBoxKey?
    /// The last short nonce received (HELLO's, then INITIATE's).
    private var lastReceivedNonce: UInt64 = 0
    /// The client's metadata from INITIATE, kept while the caller authenticates.
    private var peer: Metadata?
    private var finished: CurveSession?

    init(publicKey: [UInt8], secretKey: [UInt8]) {
        self.publicKey = publicKey
        self.secretKey = secretKey
        self.configurationError = longTermKeyProblem(publicKey: publicKey, secretKey: secretKey)
    }

    func validate() throws(ZMTPError) {
        if let configurationError { throw .invalidConfiguration(configurationError) }
    }

    mutating func takeSession() -> CurveSession? {
        defer { finished = nil }
        return finished
    }

    mutating func begin(local: Metadata) -> [Command] {
        self.local = local
        state = .awaitingHello
        return []
    }

    mutating func receive(handshake body: ByteBuffer) throws(ZMTPError) -> Mechanism.HandshakeStep {
        let (name, data) = try splitHandshake(body)
        if name == CurveCommand.error { throw peerError(data) }
        switch state {
        case .awaitingHello:
            guard name == CurveCommand.hello else { throw .unexpected("expected HELLO, got \(name)") }
            return try receiveHello(data)
        case .awaitingInitiate:
            guard name == CurveCommand.initiate else { throw .unexpected("expected INITIATE, got \(name)") }
            return try receiveInitiate(data)
        case .idle, .authenticating, .done:
            throw .unexpected("unexpected \(name)")
        }
    }

    /// HELLO: its box must open with C' and our long-term secret key and hold 64 zeros. Then WELCOME:
    /// S' and a cookie (C' and s' sealed under a key only we know), in a box from S to C'.
    private mutating func receiveHello(_ data: ByteBuffer) throws(ZMTPError) -> Mechanism.HandshakeStep {
        var data = data
        guard data.readableBytes == CurveSize.helloData,
            let version = data.readBytes(length: 2),
            data.readBytes(length: 72) != nil,  // padding, whatever it holds
            let clientShortTerm = data.readBytes(length: CurveSize.key),
            let counter: UInt64 = data.readInteger(),
            let box = data.readBytes(length: data.readableBytes)
        else {
            throw .malformedCommand("malformed HELLO")
        }
        // Major version 1; the minor one is ignored.
        guard version[0] == 1 else { throw .unexpected("unsupported CURVE version \(version[0]).\(version[1])") }
        guard let helloKey = CryptoBox.precompute(publicKey: clientShortTerm, secretKey: secretKey) else {
            throw cryptoFailure("the client short-term key cannot be used for a key agreement")
        }
        guard
            let zeros = SecretBox.open(
                box, nonce: shortNonce(prefix: CurveNonce.hello, counter: counter), key: helloKey),
            zeros.count == 64, ConstantTime.isZero(zeros)
        else {
            throw cryptoFailure("the HELLO box did not open (wrong server key?)")
        }

        let pair = X25519KeyPair.generate()
        guard let session = CryptoBox.precompute(publicKey: clientShortTerm, secretKey: pair.secretKey),
            let cookieKey = SecretBoxKey(SecureRandom.bytes(CurveSize.key))
        else {
            throw cryptoFailure("the client short-term key cannot be used for a key agreement")
        }
        let cookieRandom = SecureRandom.bytes(CurveSize.longNonce)
        let cookie =
            cookieRandom
            + SecretBox.seal(
                clientShortTerm + pair.secretKey, nonce: longNonce(prefix: CurveNonce.cookie, random: cookieRandom),
                key: cookieKey)
        let welcomeRandom = SecureRandom.bytes(CurveSize.longNonce)
        let welcomeBox = SecretBox.seal(
            pair.publicKey + cookie, nonce: longNonce(prefix: CurveNonce.welcome, random: welcomeRandom),
            key: helloKey)

        shortTerm = pair
        self.cookieKey = cookieKey
        clientShortTermKey = clientShortTerm
        sessionKey = session
        lastReceivedNonce = counter
        state = .awaitingInitiate
        var body = ByteBuffer()
        body.writeBytes(welcomeRandom)
        body.writeBytes(welcomeBox)
        return .send([.unknown(name: CurveCommand.welcome, body: body)])
    }

    /// INITIATE: the cookie must open and name this connection's C' and s'; the box (C'->S') must open;
    /// the vouch inside (a box from C to S') must hold C' and our own S. Only then is C the client's
    /// identity, and the caller is asked about it. The cookie key is forgotten at once.
    private mutating func receiveInitiate(_ data: ByteBuffer) throws(ZMTPError) -> Mechanism.HandshakeStep {
        var data = data
        guard data.readableBytes >= CurveSize.initiateDataMinimum,
            let cookie = data.readBytes(length: CurveSize.cookie),
            let counter: UInt64 = data.readInteger(),
            let box = data.readBytes(length: data.readableBytes),
            let cookieKey, let shortTerm, let sessionKey
        else {
            throw .malformedCommand("malformed INITIATE")
        }
        guard
            let cookiePlain = SecretBox.open(
                Array(cookie[CurveSize.longNonce...]),
                nonce: longNonce(prefix: CurveNonce.cookie, random: Array(cookie[..<CurveSize.longNonce])),
                key: cookieKey),
            cookiePlain.count == 2 * CurveSize.key,
            ConstantTime.equal(Array(cookiePlain[..<CurveSize.key]), clientShortTermKey),
            ConstantTime.equal(Array(cookiePlain[CurveSize.key...]), shortTerm.secretKey)
        else {
            throw cryptoFailure("the INITIATE cookie is not ours")
        }
        guard counter > lastReceivedNonce else {
            throw cryptoFailure("the nonce of INITIATE did not increase")
        }
        guard
            let plain = SecretBox.open(
                box, nonce: shortNonce(prefix: CurveNonce.initiate, counter: counter), key: sessionKey),
            plain.count >= CurveSize.key + CurveSize.vouch
        else {
            throw cryptoFailure("the INITIATE box did not open")
        }
        let clientKey = Array(plain[..<CurveSize.key])
        let vouch = Array(plain[CurveSize.key..<(CurveSize.key + CurveSize.vouch)])
        guard let vouchKey = CryptoBox.precompute(publicKey: clientKey, secretKey: shortTerm.secretKey) else {
            throw cryptoFailure("the client key cannot be used for a key agreement")
        }
        guard
            let vouched = SecretBox.open(
                Array(vouch[CurveSize.longNonce...]),
                nonce: longNonce(prefix: CurveNonce.vouch, random: Array(vouch[..<CurveSize.longNonce])),
                key: vouchKey),
            vouched.count == 2 * CurveSize.key,
            ConstantTime.equal(Array(vouched[..<CurveSize.key]), clientShortTermKey),
            ConstantTime.equal(Array(vouched[CurveSize.key...]), publicKey)
        else {
            throw cryptoFailure("the INITIATE vouch is not valid")
        }
        var metadata = ByteBuffer(bytes: plain[(CurveSize.key + CurveSize.vouch)...])
        peer = try Metadata.parse(&metadata)

        lastReceivedNonce = counter
        self.cookieKey = nil
        self.shortTerm = nil
        state = .authenticating
        return .authenticate(.curve(publicKey: clientKey))
    }

    /// The caller's answer about the client key: READY (the first nonce we send) to go on, or ERROR "400"
    /// and close. READY is the only command that ends the handshake, so the session starts here.
    mutating func completeAuthentication(allowed: Bool) throws(ZMTPError) -> Mechanism.HandshakeStep {
        guard state == .authenticating, let peer, let sessionKey else {
            throw .unexpected("no authentication is pending")
        }
        state = .done
        guard allowed else {
            return .reject(send: [.error(reason: plainDeniedReason)], error: .authenticationFailed)
        }
        let readyNonce: UInt64 = 1
        var body = ByteBuffer()
        body.writeInteger(readyNonce)
        body.writeBytes(
            SecretBox.seal(
                metadataBytes(local), nonce: shortNonce(prefix: CurveNonce.ready, counter: readyNonce),
                key: sessionKey))
        finished = CurveSession(
            key: sessionKey, isClient: false, nextSendNonce: readyNonce + 1, lastReceivedNonce: lastReceivedNonce)
        self.sessionKey = nil
        self.peer = nil
        return .complete(peer: peer, send: [.unknown(name: CurveCommand.ready, body: body)])
    }
}
