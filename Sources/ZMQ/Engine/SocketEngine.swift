import NIOCore

/// The Sendable side of a socket: what the public socket classes hold. Every call hops onto the event
/// loop of the socket and waits there; cancellation of the calling task is forwarded to the loop.
final class SocketEngine: Sendable {
    let core: SocketCore

    var options: SocketOptions { core.options }

    init(context: ZMQContext, options: SocketOptions, behavior: SocketBehavior) {
        core = SocketCore(
            loop: context.eventLoopGroup.next(), options: options, behavior: behavior, inproc: context.inproc)
    }

    /// The socket was released without `close()`: close it right away, without waiting for linger.
    deinit {
        let core = core
        core.loop.execute { core.shutdownNow() }
    }

    func bind(_ endpoint: String) async throws -> String {
        let core = core
        return try await core.loop.flatSubmit { core.bind(endpoint) }.get()
    }

    func connect(_ endpoint: String) async throws {
        let core = core
        try await core.loop.submit { try core.connect(endpoint) }.get()
    }

    /// Sends one message (at least one frame). Waits while the type cannot take it, for example
    /// when every pipe is at its high-water mark.
    func send(_ frames: [ByteBuffer]) async throws {
        let core = core
        let waiter = Waiter<Void>(payload: frames)
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                core.loop.execute { core.registerSend(waiter, continuation) }
            }
        } onCancel: {
            core.loop.execute { core.cancelSend(waiter) }
        }
    }

    /// Runs `body` on the socket's event loop with the behavior of this socket, for calls that are neither
    /// a plain send nor a plain receive (SUB subscribe, REP reply, REQ abandoning a request). Throws
    /// `ZMQError.socketClosed` once the socket is closing. Not cancellable: it runs to the end, so it can
    /// be used from a task that is already cancelled.
    func perform<Behavior: SocketBehavior, Result: Sendable>(
        _ body: @escaping @Sendable (Behavior) throws -> Result
    ) async throws -> Result {
        let core = core
        return try await core.loop.submit {
            guard core.phase == .open else { throw ZMQError.socketClosed }
            guard let behavior = core.behavior as? Behavior else {
                preconditionFailure("socket behavior is not a \(Behavior.self)")
            }
            return try body(behavior)
        }.get()
    }

    /// Waits for the next message.
    func receive() async throws -> [ByteBuffer] {
        let core = core
        let waiter = Waiter<[ByteBuffer]>()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<[ByteBuffer], any Error>) in
                core.loop.execute { core.registerReceive(waiter, continuation) }
            }
        } onCancel: {
            core.loop.execute { core.cancelReceive(waiter) }
        }
    }

    /// A new stream of the socket's connection events (`SocketEvent`).
    func events() -> AsyncStream<SocketEvent> {
        core.events.makeStream()
    }

    func close() async {
        let core = core
        await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                core.loop.execute { core.close(continuation) }
            }
        } onCancel: {
            core.loop.execute { core.abortLinger() }
        }
    }
}
