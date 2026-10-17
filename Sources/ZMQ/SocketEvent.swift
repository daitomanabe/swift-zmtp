/// Something that happened to a socket's connections, delivered by `ZMQSocket.events()`.
///
/// Endpoints are the strings the application knows: the one passed to `connect` for a connecting
/// socket, and the one `bind` returned for a connection accepted on a bound endpoint. A connection
/// goes through `connected` or `accepted`, then either `handshakeSucceeded` or `handshakeFailed`, and
/// ends with `disconnected`.
public enum SocketEvent: Sendable, Equatable {
    /// `bind` succeeded; `endpoint` is the address actually bound.
    case listening(endpoint: String)
    /// `bind` failed after the endpoint had been parsed (address in use, no permission, ...).
    case bindFailed(endpoint: String, reason: String)
    /// A peer connected to a bound endpoint. Its handshake has not finished yet.
    case accepted(endpoint: String)
    /// Our connection attempt reached the peer. The handshake has not finished yet.
    case connected(endpoint: String)
    /// A connection attempt failed or a connection ended; the socket tries again after `after`.
    case connectRetried(endpoint: String, after: Duration)
    /// The ZMTP handshake finished; `peerSocketType` is the peer's Socket-Type, for example "PULL".
    case handshakeSucceeded(endpoint: String, peerSocketType: String)
    /// The connection ended or was closed before the handshake finished.
    case handshakeFailed(endpoint: String, reason: String)
    /// A connection that had been accepted or connected is gone.
    case disconnected(endpoint: String)
    /// The socket is closed. The last event of every stream.
    case closed
}
