/// Why a ZMTP connection was closed.
public enum ZMTPError: Error, Sendable, Equatable {
    /// The first 10 bytes are not a ZMTP signature (or the peer speaks ZMTP 1.0).
    case badSignature
    /// The peer's major version is below 3.
    case unsupportedVersion(major: UInt8)
    /// The peer uses a different security mechanism than we do.
    case mechanismMismatch(local: String, peer: String)
    case malformedFrame(String)
    case malformedCommand(String)
    /// A frame is larger than `maxMessageSize`.
    case messageTooLarge
    /// The peer's READY has no Socket-Type property.
    case missingSocketType
    /// The peer's socket type cannot talk to ours.
    case incompatibleSocketType(local: SocketType, peer: String)
    /// For example a data frame before READY, or a second READY.
    case unexpected(String)
    /// The peer sent an ERROR command.
    case peerError(reason: String)
    /// We refused the peer's credentials (a PLAIN server whose caller answered `allowed: false`). The peer
    /// got an ERROR command with reason "400".
    case authenticationFailed
    /// The mechanism configuration cannot be used, for example a PLAIN user name over 255 bytes. Reported
    /// by `start()`, before anything is sent.
    case invalidConfiguration(String)
}
