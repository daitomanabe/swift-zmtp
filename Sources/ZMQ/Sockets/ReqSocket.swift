import NIOCore
import ZMTP

/// Sends one request at a time to a REP (or ROUTER) peer and waits for its reply.
public final class ReqSocket: ZMQSocket {
    private let engine: SocketEngine

    public var options: SocketOptions { engine.options }

    public init(context: ZMQContext = .default, options: SocketOptions = SocketOptions()) {
        engine = SocketEngine(context: context, options: options, behavior: ReqBehavior())
    }

    @discardableResult
    public func bind(_ endpoint: String) async throws -> String { try await engine.bind(endpoint) }

    public func connect(_ endpoint: String) async throws { try await engine.connect(endpoint) }

    public func close() async { await engine.close() }

    public func events() -> AsyncStream<SocketEvent> { engine.events() }

    /// Sends `message` to one peer (taking the peers in turn) and returns its reply. Waits while an
    /// earlier request is still open and while no peer can take the message; concurrent calls are served
    /// one after another. A peer that never replies keeps the call waiting until the task is cancelled.
    ///
    /// Cancelling the task ends the call with `CancellationError` and frees the socket for the next
    /// request; a late reply to the cancelled request is dropped.
    public func request(_ message: Message) async throws -> Message {
        try await engine.send(message.wireFrames())
        do {
            return Message(wireFrames: try await engine.receive())
        } catch is CancellationError {
            // The request is out but its reply was not taken. Give the socket back, and let it drop the
            // reply if it still comes. `perform` is not cancellable, so this runs although we are cancelled.
            _ = try? await engine.perform { (behavior: ReqBehavior) in behavior.abandonRequest() }
            throw CancellationError()
        }
    }
}

final class ReqBehavior: SocketBehavior {
    private enum State {
        case idle
        /// A request went to `pipe`. `replyQueued`: its reply arrived and waits in `pipe.inbound`.
        case awaitingReply(pipe: Pipe, replyQueued: Bool)
    }

    private var state = State.idle
    /// False once the connection that carried the open request is gone: no reply will come for it.
    private var replyPossible = true
    /// Replies still to drop, by pipe ID: they answer requests that were cancelled.
    private var staleReplies: [Int: Int] = [:]

    init() { super.init(socketType: .req) }

    /// One request at a time: while one is open, later ones wait (the engine keeps them in order).
    override func trySend(_ frames: [ByteBuffer]) throws -> SendOutcome {
        guard case .idle = state, let pipe = core.nextLoadBalancedPipe() else { return .wouldBlock }
        pipe.enqueue([ByteBuffer()] + frames)
        state = .awaitingReply(pipe: pipe, replyQueued: false)
        replyPossible = true
        return .accepted
    }

    /// Accepts the reply of the open request: from the pipe the request went to, with the empty frame
    /// in front (which is stripped). Everything else is dropped.
    override func received(_ frames: [ByteBuffer], from pipe: Pipe) -> InboundMessage? {
        if let stale = staleReplies[pipe.id] {
            staleReplies[pipe.id] = stale > 1 ? stale - 1 : nil
            return nil
        }
        guard case .awaitingReply(let awaited, false) = state, awaited === pipe,
            frames.count >= 2, frames[0].readableBytes == 0
        else { return nil }
        state = .awaitingReply(pipe: pipe, replyQueued: true)
        return InboundMessage(frames: Array(frames.dropFirst()))
    }

    /// The application took the reply: the next request may go.
    override func delivered(_ message: InboundMessage, from pipe: Pipe) {
        guard case .awaitingReply(let awaited, true) = state, awaited === pipe else { return }
        state = .idle
        core.retrySendWaiters()
    }

    override func pipeDetached(_ pipe: Pipe) { connectionLost(pipe) }

    override func pipeRemoved(_ pipe: Pipe) { connectionLost(pipe) }

    /// Replies do not cross connections: nothing is owed on the old one any more.
    private func connectionLost(_ pipe: Pipe) {
        staleReplies[pipe.id] = nil
        if case .awaitingReply(let awaited, _) = state, awaited === pipe { replyPossible = false }
    }

    /// The task waiting for the reply was cancelled. Back to idle; a reply that is already queued is
    /// thrown away, one that has not come yet is counted so it is dropped when it arrives.
    func abandonRequest() {
        guard case .awaitingReply(let pipe, let replyQueued) = state else { return }
        state = .idle
        if replyQueued {
            core.discardInbound(of: pipe)
        } else if replyPossible {
            staleReplies[pipe.id, default: 0] += 1
        }
        core.retrySendWaiters()
    }
}
