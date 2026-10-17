import NIOCore

/// The security mechanism of one connection. Everything the connection sends or receives after the
/// greeting goes through here, so a mechanism can run its own handshake and wrap or unwrap frames (CURVE
/// encrypts every frame).
enum Mechanism: Sendable {
    case null
    case plainClient(PlainClient)
    case plainServer(PlainServer)
    case curveClient(CurveClient)
    case curveServer(CurveServer)
    /// A CURVE connection after its handshake: the client or server state turns into this when the
    /// handshake completes, so the ephemeral secrets of the handshake are gone and `encode` / `decode`
    /// touch only the key and the nonce counters.
    case curve(CurveSession)

    /// What the handshake wants next.
    enum HandshakeStep {
        /// Send these commands and keep waiting.
        case send([Command])
        /// Ask the caller to check the peer (`Output.authenticate`) and wait for
        /// `completeAuthentication(allowed:)`. Nothing is read from the peer meanwhile.
        case authenticate(AuthenticationRequest)
        /// The handshake is over; the peer's properties are final. `send` goes out first, unless the
        /// peer's socket type does not fit ours.
        case complete(peer: Metadata, send: [Command])
        /// Send these commands, then close with `error`.
        case reject(send: [Command], error: ZMTPError)
    }

    init(_ configuration: MechanismConfiguration) {
        switch configuration {
        case .null:
            self = .null
        case .plainClient(let username, let password):
            self = .plainClient(PlainClient(username: Array(username.utf8), password: Array(password.utf8)))
        case .plainServer:
            self = .plainServer(PlainServer())
        case .curveClient(let serverPublicKey, let publicKey, let secretKey):
            self = .curveClient(
                CurveClient(serverPublicKey: serverPublicKey, publicKey: publicKey, secretKey: secretKey))
        case .curveServer(let publicKey, let secretKey):
            self = .curveServer(CurveServer(publicKey: publicKey, secretKey: secretKey))
        }
    }

    /// Name for the greeting; padded with NUL to 20 bytes on the wire.
    var name: String {
        switch self {
        case .null: "NULL"
        case .plainClient, .plainServer: "PLAIN"
        case .curveClient, .curveServer, .curve: "CURVE"
        }
    }

    /// Checks the configuration before anything is sent.
    func validate() throws(ZMTPError) {
        switch self {
        case .null, .plainServer, .curve: break
        case .plainClient(let client): try client.validate()
        case .curveClient(let client): try client.validate()
        case .curveServer(let server): try server.validate()
        }
    }

    /// Commands to send once both greetings have been exchanged. `local` holds Socket-Type (and Identity).
    mutating func begin(local: Metadata) -> [Command] {
        switch self {
        case .null:
            return [.ready(local)]
        case .plainClient(var client):
            let commands = client.begin(local: local)
            self = .plainClient(client)
            return commands
        case .plainServer(var server):
            let commands = server.begin(local: local)
            self = .plainServer(server)
            return commands
        case .curveClient(var client):
            let commands = client.begin(local: local)
            self = .curveClient(client)
            return commands
        case .curveServer(var server):
            let commands = server.begin(local: local)
            self = .curveServer(server)
            return commands
        case .curve:
            return []
        }
    }

    /// Feeds the body of a command frame received before the handshake finished. The mechanism parses it
    /// itself, because a body may be encrypted (CURVE's READY is a box, not metadata).
    mutating func receive(handshake body: ByteBuffer) throws(ZMTPError) -> HandshakeStep {
        switch self {
        case .null:
            let command = try Command(body: body)
            switch command {
            case .ready(let peer): return .complete(peer: peer, send: [])
            case .error(let reason): throw .peerError(reason: reason)
            default: throw .unexpected("expected READY, got \(command.name)")
            }
        case .plainClient(var client):
            let step = try client.receive(try Command(body: body))
            self = .plainClient(client)
            return step
        case .plainServer(var server):
            let step = try server.receive(try Command(body: body))
            self = .plainServer(server)
            return step
        case .curveClient(var client):
            let step = try client.receive(handshake: body)
            if let session = client.takeSession() { self = .curve(session) } else { self = .curveClient(client) }
            return step
        case .curveServer(var server):
            let step = try server.receive(handshake: body)
            if let session = server.takeSession() { self = .curve(session) } else { self = .curveServer(server) }
            return step
        case .curve:
            throw .unexpected("handshake command after the CURVE handshake")
        }
    }

    /// The caller's answer to an `.authenticate` step.
    mutating func completeAuthentication(allowed: Bool) throws(ZMTPError) -> HandshakeStep {
        switch self {
        case .plainServer(var server):
            let step = try server.completeAuthentication(allowed: allowed)
            self = .plainServer(server)
            return step
        case .curveServer(var server):
            let step = try server.completeAuthentication(allowed: allowed)
            if let session = server.takeSession() { self = .curve(session) } else { self = .curveServer(server) }
            return step
        case .null, .plainClient, .curveClient, .curve:
            throw .unexpected("no authentication is pending")
        }
    }

    /// Writes a data or command frame after the handshake. Mutating because a mechanism may count
    /// frames (CURVE numbers its messages).
    mutating func encode(_ frame: WireFrame, into buffer: inout ByteBuffer) {
        switch self {
        case .null, .plainClient, .plainServer: FrameCodec.write(frame, into: &buffer)
        case .curve(var session):
            session.encode(frame, into: &buffer)
            self = .curve(session)
        case .curveClient, .curveServer:
            preconditionFailure("a frame was encoded before the CURVE handshake finished")
        }
    }

    /// Turns a frame received after the handshake into the frame it carries.
    mutating func decode(_ frame: WireFrame) throws(ZMTPError) -> WireFrame {
        switch self {
        case .null, .plainClient, .plainServer: return frame
        case .curve(var session):
            let plain = try session.decode(frame)
            self = .curve(session)
            return plain
        case .curveClient, .curveServer:
            throw .unexpected("a frame was decoded before the CURVE handshake finished")
        }
    }
}
