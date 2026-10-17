/// Settings of a socket. They are fixed when the socket is created.
public struct SocketOptions: Sendable {
    /// Messages that may wait to be sent to one peer (queued plus not yet written to the connection).
    /// 0 or less: no limit.
    public var sendHighWaterMark = 1000
    /// Messages that may wait to be received from one peer. When the queue is full the socket stops
    /// reading from that connection, so TCP pushes back on the sender. 0 or less: no limit.
    /// A single read from the network can overshoot the mark by the messages it carries.
    public var receiveHighWaterMark = 1000
    /// How long `close()` waits for queued messages to be written. nil: until everything is sent.
    public var linger: Duration? = .seconds(1)
    /// Identity announced to the peer (REQ, DEALER and ROUTER sockets).
    public var routingID: RoutingID? = nil
    /// Time before reconnecting after a connection fails or ends.
    public var reconnectInterval: Duration = .milliseconds(100)
    /// When set, the reconnect interval doubles after each failure, up to this value.
    public var reconnectIntervalMax: Duration? = nil
    /// Largest accepted frame in bytes. A larger frame closes the connection. nil: no limit.
    public var maxMessageSize: Int? = nil
    /// Send a PING to the peer this often once the handshake is done (ZMTP 3.1 peers only).
    /// nil: no heartbeats. Peers always answer PINGs they receive, whatever this is set to.
    public var heartbeatInterval: Duration? = nil
    /// How long to wait for any traffic from the peer after a PING before closing the connection
    /// (a connecting socket then reconnects). nil: the same as `heartbeatInterval`.
    public var heartbeatTimeout: Duration? = nil
    /// Told to the peer in every PING: it should close the connection if it hears nothing from this
    /// socket for this long. Sent in tenths of a second, at most 6553.5 s. nil: no TTL.
    public var heartbeatTTL: Duration? = nil
    /// How long the connection setup (the ZMTP handshake) may take before the connection is closed.
    /// A connecting socket then tries again. nil: no limit.
    public var handshakeTimeout: Duration? = .seconds(30)

    public init() {}
}
