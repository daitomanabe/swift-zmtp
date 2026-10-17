/// Errors thrown by socket operations. Task cancellation is reported as `CancellationError`.
public enum ZMQError: Error, Sendable, Equatable {
    /// The endpoint string is malformed.
    case invalidEndpoint(String)
    /// The transport is not available yet (`ipc://` and `inproc://`).
    case unsupportedTransport(String)
    case bindFailed(endpoint: String, reason: String)
    /// The socket was closed.
    case socketClosed
    /// The operation does not fit the state of the socket, for example a REP reply without a request.
    case invalidState(String)
}
