import NIOCore

/// The security mechanism of one connection. Only NULL exists so far; PLAIN and CURVE (stage 3) become
/// more cases. Everything the connection sends or receives after the greeting goes through here, so a
/// mechanism can run its own handshake and wrap or unwrap frames (CURVE encrypts every frame).
enum Mechanism: Sendable {
    case null

    /// What the handshake wants next.
    enum HandshakeStep {
        /// Send these commands and keep waiting.
        case send([Command])
        /// The handshake is over; the peer's properties are final.
        case complete(peer: Metadata)
    }

    /// Name for the greeting; padded with NUL to 20 bytes on the wire.
    var name: String {
        switch self {
        case .null: "NULL"
        }
    }

    /// Commands to send once both greetings have been exchanged. `local` holds Socket-Type (and Identity).
    mutating func begin(local: Metadata) -> [Command] {
        switch self {
        case .null: [.ready(local)]
        }
    }

    /// Feeds a command received before the handshake finished.
    mutating func receive(handshake command: Command) throws(ZMTPError) -> HandshakeStep {
        switch self {
        case .null:
            switch command {
            case .ready(let peer): return .complete(peer: peer)
            case .error(let reason): throw .peerError(reason: reason)
            default: throw .unexpected("expected READY, got \(command.name)")
            }
        }
    }

    /// Writes a data or command frame after the handshake.
    func encode(_ frame: WireFrame, into buffer: inout ByteBuffer) {
        switch self {
        case .null: FrameCodec.write(frame, into: &buffer)
        }
    }

    /// Turns a frame received after the handshake into the frame it carries.
    mutating func decode(_ frame: WireFrame) throws(ZMTPError) -> WireFrame {
        switch self {
        case .null: frame
        }
    }
}
