import NIOCore
import ZMTP

/// The queues and the current connection for one peer. Loop-confined, like everything in the engine.
///
/// A connect-side pipe is created by `connect()` and lives until the socket closes; connections come
/// and go under it. A bind-side pipe is created when a connection finishes its handshake and ends with
/// that connection.
final class Pipe {
    enum Origin {
        /// `endpoint` is the string the application passed to `connect`.
        case connect(Endpoint.ConnectTarget, endpoint: String)
        case bind
    }

    /// Where the pipe's connection stands.
    enum Link {
        /// Bind side before the handshake is done, or connect side before the first attempt.
        case idle
        /// A connect attempt is in flight.
        case connecting
        /// A connection exists (its handshake may still be running).
        case linked(any PipeConnection)
        /// Waiting to reconnect.
        case backoff(Scheduled<Void>)
        /// The socket is closing; no more attempts.
        case stopped
    }

    /// Large writes are cut here, so one flush does not build one enormous buffer.
    private static let chunkSize = 256 * 1024

    let id: Int
    let origin: Origin
    unowned let core: SocketCore
    var link = Link.idle
    /// Handshake result of the current (or last) connection.
    var peer: Metadata?
    /// True once a bind-side connection ended or the socket closed. The pipe takes no more sends and
    /// stays only until its received messages are read.
    var isTerminated = false
    /// Delay before the next reconnect attempt; grows after failures when `reconnectIntervalMax` is set.
    var reconnectDelay: TimeAmount

    /// Messages accepted from the application and not yet handed to a connection.
    var outbound = CircularBuffer<[ByteBuffer]>()
    /// Messages handed to a connection whose write has not completed.
    private(set) var writing = 0
    /// Received messages waiting for `receive()`.
    var inbound = CircularBuffer<SocketBehavior.InboundMessage>()
    private var flushScheduled = false

    init(core: SocketCore, id: Int, origin: Origin) {
        self.core = core
        self.id = id
        self.origin = origin
        self.reconnectDelay = core.options.reconnectInterval.timeAmount
    }

    var isConnectSide: Bool {
        if case .connect = origin { return true }
        return false
    }

    /// The string given to `connect`, for a connect-side pipe.
    var connectEndpoint: String? {
        if case .connect(_, let endpoint) = origin { return endpoint }
        return nil
    }

    /// The connection, if one exists.
    var connection: (any PipeConnection)? {
        if case .linked(let connection) = link { return connection }
        return nil
    }

    /// Messages that count against `sendHighWaterMark`.
    var pending: Int { outbound.count + writing }

    /// True when `enqueue` is within the send high-water mark.
    var canAccept: Bool { !isTerminated && pending < core.sendLimit }

    var hasInboundRoom: Bool { inbound.count < core.receiveLimit }

    // MARK: Sending

    /// Queues one message for this peer. Writes are batched: the actual write and flush happen once, at
    /// the end of the current event-loop tick, for everything queued by then. Check `canAccept` first
    /// if the type must respect the high-water mark.
    func enqueue(_ frames: [ByteBuffer]) {
        outbound.append(frames)
        scheduleFlush()
    }

    /// Writes a SUBSCRIBE or CANCEL for `topic` to the current connection right away, if its handshake
    /// is done (this includes the time inside `pipeAttached`, before the pipe is marked attached).
    /// Subscriptions do not count against the high-water mark and are not kept for a later connection;
    /// a SUB socket sends its whole set again from `pipeAttached`.
    func writeSubscription(_ topic: ByteBuffer, subscribe: Bool) {
        connection?.writeSubscription(topic, subscribe: subscribe)
    }

    /// Arranges for `flushNow` to run once at the end of this tick (if a connection is up).
    func scheduleFlush() {
        guard !flushScheduled, !outbound.isEmpty, let connection, connection.isAttached else { return }
        flushScheduled = true
        core.loop.assumeIsolated().execute { [self] in
            flushScheduled = false
            flushNow()
        }
    }

    /// Hands everything queued to the connection: one `write` for the whole queue, cut into pieces of
    /// about `chunkSize` bytes so that one flush does not build one enormous buffer.
    private func flushNow() {
        guard let connection, connection.isAttached, !outbound.isEmpty else { return }

        var batch: [[ByteBuffer]] = []
        var batchBytes = 0
        while !outbound.isEmpty {
            let message = outbound.removeFirst()
            batchBytes += message.reduce(0) { $0 + $1.readableBytes }
            batch.append(message)
            if batchBytes >= Self.chunkSize {
                write(batch, to: connection)
                batch = []
                batchBytes = 0
            }
        }
        if !batch.isEmpty { write(batch, to: connection) }
    }

    private func write(_ messages: [[ByteBuffer]], to connection: any PipeConnection) {
        let count = messages.count
        // Counted before the write: the connection may complete it before `write` returns.
        writing += count
        connection.write(messages) { [self] in
            // A failed write means the connection broke; those messages are lost, as in libzmq.
            writing -= count
            core.pipeWriteCompleted(self)
        }
    }
}
