import NIOCore

/// What a `Pipe` needs from the connection under it: a TCP or IPC connection driven by NIO
/// (`ConnectionHandler`), or a link to another socket in the same process.
///
/// Everything here is called on the event loop of the socket that owns the pipe. The pipe keeps no
/// reference to NIO; encoding, framing and the handshake are the connection's business.
protocol PipeConnection: AnyObject {
    /// The handshake with the peer is done (and the connection is still up).
    var isReady: Bool { get }
    /// The socket accepted the connection after the handshake: messages may flow.
    var isAttached: Bool { get }
    /// Address for events: the string given to `connect`, or the bound endpoint that accepted the peer.
    var endpoint: String { get }

    /// Hands `messages` to the connection, in order. `completed` runs once on the socket's loop when
    /// they have left (TCP and IPC: the write finished or failed; in-process: the peer took them and has
    /// room again). It may run before this call returns when the connection is already gone. The pipe
    /// counts the messages against its send high-water mark until `completed` runs.
    func write(_ messages: [[ByteBuffer]], completed: @escaping () -> Void)
    /// Sends a SUBSCRIBE (or CANCEL) for `topic` if the handshake is done; otherwise does nothing.
    func writeSubscription(_ topic: ByteBuffer, subscribe: Bool)
    /// Asks for more input if the connection has paused reading and the pipe can take more messages.
    func resumeReadIfNeeded()
    /// Closes the connection. The owner is told through the usual disconnect path.
    func close()
}
