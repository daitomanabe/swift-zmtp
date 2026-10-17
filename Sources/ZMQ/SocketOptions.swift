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

    public init() {}
}
