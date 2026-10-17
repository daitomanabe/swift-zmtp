import NIOCore
import ZMTP

/// The queues and the current connection for one peer. Loop-confined, like everything in the engine.
///
/// A connect-side pipe is created by `connect()` and lives until the socket closes; connections come
/// and go under it. A bind-side pipe is created when a connection finishes its handshake and ends with
/// that connection.
final class Pipe {
    enum Origin {
        case connect(host: String, port: Int)
        case bind
    }

    /// Where the pipe's connection stands.
    enum Link {
        /// Bind side before the handshake is done, or connect side before the first attempt.
        case idle
        /// A connect attempt is in flight.
        case connecting
        /// A TCP connection exists (handshake may still be running).
        case linked(ConnectionHandler)
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

    /// The connection, if one exists.
    var handler: ConnectionHandler? {
        if case .linked(let handler) = link { return handler }
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
        guard let handler, handler.isReady, let channel = handler.channel, channel.isActive else { return }
        var buffer = channel.allocator.buffer(capacity: topic.readableBytes + 16)
        handler.encodeSubscription(topic, subscribe: subscribe, into: &buffer)
        channel.writeAndFlush(buffer, promise: nil)
    }

    /// Arranges for `flushNow` to run once at the end of this tick (if a connection is up).
    func scheduleFlush() {
        guard !flushScheduled, !outbound.isEmpty, let handler, handler.isAttached else { return }
        flushScheduled = true
        core.loop.assumeIsolated().execute { [self] in
            flushScheduled = false
            flushNow()
        }
    }

    private func flushNow() {
        guard let handler, handler.isAttached, let channel = handler.channel, channel.isActive,
            !outbound.isEmpty
        else { return }

        var buffer = channel.allocator.buffer(capacity: 1024)
        var count = 0
        while !outbound.isEmpty {
            handler.encode(outbound.removeFirst(), into: &buffer)
            count += 1
            if buffer.readableBytes >= Self.chunkSize {
                write(buffer, messages: count, to: channel)
                buffer = channel.allocator.buffer(capacity: 1024)
                count = 0
            }
        }
        if count > 0 { write(buffer, messages: count, to: channel) }
        channel.flush()
    }

    private func write(_ buffer: ByteBuffer, messages: Int, to channel: any Channel) {
        // Counted before the write: the promise can complete inside `flush()`.
        writing += messages
        let promise = channel.eventLoop.makePromise(of: Void.self)
        promise.futureResult.assumeIsolated().whenComplete { [self] _ in
            // A failed write means the connection broke; those messages are lost, as in libzmq.
            writing -= messages
            core.pipeWriteCompleted(self)
        }
        channel.write(buffer, promise: promise)
    }
}
