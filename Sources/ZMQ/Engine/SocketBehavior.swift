import NIOCore
import ZMTP

/// What the engine asks a socket type to decide. See the notes at the top of `SocketCore.swift`.
/// Every method runs on the socket's event loop; a behavior needs no locks.
class SocketBehavior {
    /// What a `trySend` did with the message.
    enum SendOutcome {
        /// The message was queued on one or more pipes (or deliberately dropped).
        case accepted
        /// No pipe can take it now. The engine parks the sender and calls `trySend` again, in
        /// order, whenever a pipe gains room or appears.
        case wouldBlock
    }

    /// A received message on its way to the application.
    struct InboundMessage {
        /// What `receive()` returns.
        var frames: [ByteBuffer]
        /// Routing envelope the type wants to remember (REP). Unused by PUSH, PULL and PAIR.
        var envelope: [ByteBuffer] = []
    }

    let socketType: SocketType
    /// The engine this behavior belongs to; set once by `SocketCore.init`. Unowned because the core
    /// owns the behavior and lives at least as long.
    final unowned var core: SocketCore!

    init(socketType: SocketType) {
        self.socketType = socketType
    }

    // MARK: Pipes

    /// A connect-side pipe was created by `connect()`, before any handshake. Throw to refuse the
    /// `connect()` call (the pipe is then dropped).
    func pipeCreated(_ pipe: Pipe) throws {}

    /// The handshake with `peer` finished on `pipe`. Called after every successful handshake: once
    /// for a bind-side pipe (before the pipe becomes visible in `core.pipes`), and again after each
    /// reconnect for a connect-side pipe. Return false to refuse the connection; the engine then closes
    /// it. Typical uses: SUB re-sends its subscriptions (`Pipe.writeSubscription`), ROUTER registers the
    /// peer's Identity.
    func pipeAttached(_ pipe: Pipe, peer: Metadata) -> Bool { true }

    /// A connect-side pipe lost its connection (after it was attached). The pipe, with whatever it still
    /// has queued, stays and is reused after the reconnect.
    func pipeDetached(_ pipe: Pipe) {}

    /// A pipe is gone for good: its bind-side connection ended, or the socket is closing.
    func pipeRemoved(_ pipe: Pipe) {}

    // MARK: Sending

    /// Chooses where `frames` go and queues them with `Pipe.enqueue`. Use `core.nextLoadBalancedPipe()`
    /// for one pipe in turn, or look at `core.pipes` to pick several or one by key. Return `.accepted`
    /// also when a message is dropped on purpose (PUB, ROUTER). Throw to fail the `send` call.
    func trySend(_ frames: [ByteBuffer]) throws -> SendOutcome {
        throw ZMQError.invalidState("\(socketType.rawValue) sockets cannot send")
    }

    /// How many more messages `trySend` would accept right now without returning `.wouldBlock`. The
    /// engine turns this into send credits: `send` may queue that many messages (less those already
    /// queued) and return without waiting, and the loop routes them with `trySend` afterwards. Called
    /// on the loop whenever the answer may have changed, so it must be cheap.
    ///
    /// The default is 0: every `send` takes the slow path and waits for the loop (REQ, and ROUTER with
    /// `routerMandatory`, whose sends depend on state that a credit cannot promise). A type that never
    /// waits (PUB, ROUTER) answers `SocketCore.fixedSendCapacity`; one that waits for room (PUSH,
    /// DEALER, PAIR) adds up the room of the pipes it would pick (`SocketCore.loadBalancedCapacity()`).
    /// A message that is no longer accepted when the loop gets to it (its pipe went away meanwhile)
    /// waits in the engine and is retried before any waiting sender, so the order is kept.
    func sendCapacity() -> Int { 0 }

    // MARK: Receiving

    /// True when `receive()` may take messages from the engine's shared receive queue, which the loop
    /// fills ahead of the application. That is only right for a type whose `received` / `delivered` /
    /// `beforeReceive` do not depend on the moment the application takes a message (REQ and REP do:
    /// they say no). Never used with `conflate`, where a pipe keeps only its newest message.
    var usesReceiveQueue: Bool { false }

    /// Called when a `receive()` starts, before it looks for a message. Throw to fail it.
    func beforeReceive() throws {}

    /// A message arrived on `pipe`. Return what to queue for `receive()` (rewrite frames, split off an
    /// envelope, add a routing ID), or nil to drop it. The default passes it through.
    func received(_ frames: [ByteBuffer], from pipe: Pipe) -> InboundMessage? {
        InboundMessage(frames: frames)
    }

    /// `message`, which came from `pipe`, is being handed to the application.
    func delivered(_ message: InboundMessage, from pipe: Pipe) {}

    // MARK: Subscriptions

    /// The peer behind `pipe` subscribed to `topic` (we are PUB or XPUB).
    func peerSubscribed(_ topic: ByteBuffer, on pipe: Pipe) {}

    /// The peer behind `pipe` cancelled its subscription to `topic`.
    func peerCancelled(_ topic: ByteBuffer, on pipe: Pipe) {}
}
