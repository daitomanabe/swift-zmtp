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
    /// A CURVE key is not usable: a Z85 text or byte array of the wrong size, or text that is not Z85. The
    /// text says what is wrong; it never contains the key.
    case invalidKey(String)
    /// A ROUTER socket with `options.routerMandatory` was asked to send to a routing ID that no connected
    /// peer has.
    case hostUnreachable(RoutingID)
    /// A call with a `timeout:` did not finish in time. A `receive(timeout:)` that throws this has taken
    /// no message (a later `receive` gets it), a `send(_:timeout:)` has queued nothing, and a
    /// `request(_:timeout:)` has given up the request (a late reply to it is dropped).
    case timedOut
}
