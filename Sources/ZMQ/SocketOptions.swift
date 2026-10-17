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

    /// How connections are authenticated and protected (libzmq: `ZMQ_PLAIN_*` and `ZMQ_CURVE_*`). `.none` (the
    /// default) is the NULL mechanism. Applies to `tcp://` and `ipc://` connections made and accepted by
    /// this socket; `inproc://` ignores it. Both ends must agree; a mismatch, a refused login or a wrong
    /// key makes the handshake fail (`SocketEvent.handshakeFailed`).
    public var security: Security = .none

    /// ROUTER only (libzmq: `ZMQ_ROUTER_MANDATORY`). Makes `send(_:to:)` report what it would otherwise
    /// hide: a routing ID that no connected peer has throws `ZMQError.hostUnreachable`, and a peer that
    /// is at its send high-water mark makes the call wait (it ends with `CancellationError` when the
    /// task is cancelled) instead of dropping the message. Sends are served in order, so one send
    /// that waits holds up the ones behind it. false (the default): both cases drop the message silently.
    public var routerMandatory = false

    /// PUSH, PULL, PUB, SUB and DEALER sockets; ignored by the other types (libzmq: `ZMQ_CONFLATE`).
    /// Each peer's send queue and each peer's receive queue keeps only the newest message, so a slow
    /// peer or reader sees the latest value rather than a backlog. The high-water marks are not used
    /// then. Messages already handed to the connection are not taken back. Unlike libzmq, a multipart
    /// message is kept whole.
    public var conflate = false

    /// Connecting sockets (libzmq: `ZMQ_IMMEDIATE`). A connection made with `connect` is a target for
    /// sends only once its handshake is done. Load balancing (PUSH, DEALER, REQ) then skips peers that
    /// are not there yet, and `send` waits while none is ready, instead of queueing for a peer that may
    /// never come. When the connection ends, what was queued for it is dropped; it is not kept for the
    /// reconnect. false (the default): messages queue from the `connect` call on and survive reconnects.
    public var immediate = false

    /// TCP connections only (libzmq: `ZMQ_TCP_KEEPALIVE` with `ZMQ_TCP_KEEPALIVE_IDLE`); IPC and inproc
    /// ignore it. Turns on TCP keepalive and starts the probes after the connection has been idle for this
    /// long, in whole seconds and at least 1 (macOS `TCP_KEEPALIVE`, Linux `TCP_KEEPIDLE`). The interval
    /// and the number of probes stay at the system's defaults. Applies to connections made and accepted
    /// by this socket. nil (the default): the system's setting.
    public var tcpKeepAlive: Duration? = nil

    public init() {}
}
