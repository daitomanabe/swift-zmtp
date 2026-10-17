/// Errors thrown by socket operations. Task cancellation is reported as `CancellationError`.
public enum ZMQError: Error, Sendable, Equatable {
    /// The endpoint string is malformed.
    case invalidEndpoint(String)
    /// The transport is not available. Reserved: every transport the library knows (`tcp://`, `ipc://`,
    /// `inproc://`) works, so nothing throws this now.
    case unsupportedTransport(String)
    case bindFailed(endpoint: String, reason: String)
    /// The socket was closed.
    case socketClosed
    /// The operation does not fit the state of the socket, for example a REP reply without a request.
    case invalidState(String)
    /// A ROUTER socket with `options.routerMandatory` was asked to send to a routing ID that no connected
    /// peer has.
    case hostUnreachable(RoutingID)
}
