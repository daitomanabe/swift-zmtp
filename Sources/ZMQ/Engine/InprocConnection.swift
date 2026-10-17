import NIOCore
import ZMTP

/// One end of an in-process link between two sockets of the same `ZMQContext`: the `PipeConnection`
/// of a pipe whose peer is another socket in this process. There is no ZMTP and no NIO channel.
///
/// The two ends live on the event loops of their sockets. Each end touches its own state on its own
/// loop only; to reach the other end it hands a closure to the other end's loop with `execute`, so the
/// work arrives there in the order it was sent.
///
/// Linking (the connect side asks, the bind side answers, both run `behavior.pipeAttached`):
///   1. connect side: `SocketCore.requestInprocLink` makes an end and sends it to the bound socket;
///   2. bind side: `SocketCore.acceptInproc` checks the types, makes its end and its pipe, runs
///      `pipeAttached`, and answers `accepted` with its end (or `refused` / `unavailable`);
///   3. connect side: `SocketCore.inprocAnswered` runs `pipeAttached` and the link is up.
///
/// Messages: `write` hands a batch to the other end. That end queues the messages in its pipe and
/// sends back an acknowledgement as soon as its receive queue has room, or later, when the application
/// has read and made room. The writer keeps the batch counted against its send high-water mark until
/// the acknowledgement arrives, so a reader that does not read stops the writer.
///
/// `@unchecked Sendable`: all mutable state is read and written only on `loop`, the event loop of the
/// owning socket. Other threads see only the immutable `loop` and `endpoint`, and use them to hand
/// work to `loop`.
final class InprocConnection: PipeConnection, @unchecked Sendable {
    /// What the bound socket answers to a link request.
    enum Answer: Sendable {
        /// Accepted. `end` is the bind side's end of the link; `metadata` describes its socket.
        case accepted(end: InprocConnection, metadata: Metadata)
        /// The bound socket said no (the types do not fit, or its socket type refused this peer).
        case refused(reason: String)
        /// The name is not bound any more (its socket is closing).
        case unavailable
    }

    let loop: any EventLoop
    let endpoint: String
    /// What READY would carry: Socket-Type, and Identity for the types that send it.
    let localMetadata: Metadata
    private weak var core: SocketCore?

    private(set) var pipe: Pipe?
    private var peer: InprocConnection?
    private(set) var isReady = false
    private(set) var isEstablished = false
    private(set) var wasAttached = false
    private var isClosed = false
    /// The link ended because the other side went away (not because someone refused): a connect side
    /// then waits for the next bind of the name instead of retrying after a delay.
    private(set) var waitsForBind = false

    /// Completions of the batches the other end has not acknowledged yet, oldest first.
    private var unacknowledged = CircularBuffer<() -> Void>()
    /// Batches that arrived while the receive queue was full; acknowledged when it has room again.
    private var heldAcknowledgements = 0
    /// SUBSCRIBE / CANCEL written before the other end knows about this one (a bind side, from inside
    /// `pipeAttached`); sent right after the answer.
    private var heldSubscriptions: [(topic: ByteBuffer, subscribe: Bool)] = []

    init(core: SocketCore, endpoint: String) {
        core.loop.preconditionInEventLoop()
        self.core = core
        self.loop = core.loop
        self.endpoint = endpoint
        let type = core.behavior.socketType
        var properties = [Metadata.Property(name: "Socket-Type", value: ByteBuffer(string: type.rawValue))]
        if type.sendsIdentity {
            properties.append(
                Metadata.Property(name: "Identity", value: ByteBuffer(bytes: core.options.routingID?.frame.bytes ?? [])))
        }
        self.localMetadata = Metadata(properties: properties)
    }

    // MARK: PipeConnection

    var isAttached: Bool { wasAttached && !isClosed }

    func attach(_ pipe: Pipe) {
        self.pipe = pipe
    }

    func markAttached() {
        wasAttached = true
    }

    func write(_ messages: [[ByteBuffer]], completed: @escaping () -> Void) {
        guard isAttached, peer != nil else {
            completed()
            return
        }
        unacknowledged.append(completed)
        toPeer { $0.receive(messages) }
    }

    func writeSubscription(_ topic: ByteBuffer, subscribe: Bool) {
        guard isReady, !isClosed else { return }
        guard peer != nil else {
            heldSubscriptions.append((topic, subscribe))
            return
        }
        toPeer { $0.receiveSubscription(topic, subscribe: subscribe) }
    }

    /// The application read a message: if the receive queue has room again, the batches held back can
    /// be acknowledged.
    func resumeReadIfNeeded() {
        guard heldAcknowledgements > 0, !isClosed, canTakeMore else { return }
        let count = heldAcknowledgements
        heldAcknowledgements = 0
        toPeer { $0.acknowledged(count) }
    }

    /// Ends the link from this side. The other end is told; this socket is told through `connectionClosed`.
    func close() {
        guard !isClosed else { return }
        end(waitsForBind: false, tellPeer: true)
    }

    // MARK: Called by the core, on the loop

    /// Connect side, when the bind side accepted: the link is up as far as the handshake goes.
    func link(to remote: InprocConnection) {
        peer = remote
        isEstablished = true
        isReady = true
    }

    /// Bind side, before `pipeAttached`: the types fit, so the handshake is done from this side.
    func beginAccept() {
        isEstablished = true
        isReady = true
    }

    /// Connect side: the bind side answered, so the request reached a socket (it will say `disconnected`).
    func reachedPeer() {
        isEstablished = true
    }

    /// The socket refused the connection in `pipeAttached`.
    func markNotReady() {
        isReady = false
    }

    /// Bind side, after `pipeAttached` accepted: tells the connect side, then sends the subscriptions
    /// the hook wrote on the way.
    func announce(to remote: InprocConnection) {
        peer = remote
        let metadata = localMetadata
        remote.deliverAnswer(.accepted(end: self, metadata: metadata))
        let held = heldSubscriptions
        heldSubscriptions.removeAll()
        for subscription in held { writeSubscription(subscription.topic, subscribe: subscription.subscribe) }
    }

    /// Bind side, when `pipeAttached` or the type check said no: drops the end without telling the core
    /// (the pipe never became visible there).
    func discard() {
        isClosed = true
        isReady = false
        peer = nil
        pipe = nil
        heldSubscriptions.removeAll()
    }

    /// The bind side is done with this request without a link (the connect side's end is `unavailable`
    /// or `refused`): the link ended, and a missing bind side means waiting for the next bind.
    func ended(waitingForBind: Bool) {
        guard !isClosed else { return }
        end(waitsForBind: waitingForBind, tellPeer: false)
    }

    // MARK: Called from any thread

    /// Hands the bind side's answer to this connect-side end.
    func deliverAnswer(_ answer: Answer) {
        loop.execute { self.handle(answer) }
    }

    /// Tells this end that the other end is gone.
    func notifyPeerClosed() {
        loop.execute { self.peerClosed() }
    }

    // MARK: On the loop: work sent by the other end

    private func handle(_ answer: Answer) {
        guard !isClosed, let core else {
            // This end was given up while the request was on its way: release the bind side's end.
            if case .accepted(let end, _) = answer { end.notifyPeerClosed() }
            return
        }
        core.inprocAnswered(self, answer)
    }

    private func peerClosed() {
        guard !isClosed else { return }
        end(waitsForBind: true, tellPeer: false)
    }

    /// A batch of messages from the other end.
    private func receive(_ messages: [[ByteBuffer]]) {
        guard !isClosed else { return }
        guard let core, let pipe, wasAttached else {
            // Cannot happen in the normal order of events; do not leave the writer waiting if it does.
            toPeer { $0.acknowledged(1) }
            return
        }
        for frames in messages { core.messageReceived(frames, on: pipe) }
        // Move them on to the application's receive queue first: that is what makes room for the next batch.
        core.receiveBatchEnded()
        if canTakeMore {
            toPeer { $0.acknowledged(1) }
        } else {
            heldAcknowledgements += 1
        }
    }

    /// The other end took `count` batches and has room again.
    private func acknowledged(_ count: Int) {
        for _ in 0..<count {
            guard !isClosed, let completed = unacknowledged.popFirst() else { return }
            completed()
        }
    }

    private func receiveSubscription(_ topic: ByteBuffer, subscribe: Bool) {
        guard !isClosed, let core, let pipe, wasAttached else { return }
        if subscribe {
            core.behavior.peerSubscribed(topic, on: pipe)
        } else {
            core.behavior.peerCancelled(topic, on: pipe)
        }
    }

    // MARK: Helpers

    /// True when the pipe's receive queue has room, or the socket no longer reads anyway (it is closing,
    /// so messages that arrive are dropped and the writer must not wait for them).
    private var canTakeMore: Bool {
        guard let core, let pipe else { return true }
        return core.phase != .open || core.mayRead(from: pipe)
    }

    /// Runs `work` on the other end's loop, after everything sent to it before.
    private func toPeer(_ work: @escaping @Sendable (InprocConnection) -> Void) {
        guard let target = peer else { return }
        target.loop.execute { work(target) }
    }

    /// Ends this end: tells the other end (if asked), settles the writes nobody will acknowledge now,
    /// and lets the core update the pipe.
    private func end(waitsForBind: Bool, tellPeer: Bool) {
        isClosed = true
        self.waitsForBind = waitsForBind
        let remote = peer
        peer = nil
        heldSubscriptions.removeAll()
        heldAcknowledgements = 0
        if tellPeer { remote?.notifyPeerClosed() }
        core?.connectionClosed(self)
        pipe = nil
        // Like a failed write on a broken TCP connection: those messages are lost, and the pipe stops
        // counting them against its send high-water mark.
        let lost = unacknowledged
        unacknowledged.removeAll()
        // (The pipe is gone with its core if the core is, and then there is nothing left to count.)
        if core != nil { for completed in lost { completed() } }
    }
}
