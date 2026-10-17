import NIOCore

/// The Sendable side of a socket: what the public socket classes hold. Every call hops onto the event
/// loop of the socket and waits there; cancellation of the calling task is forwarded to the loop.
final class SocketEngine: Sendable {
    let core: SocketCore

    var options: SocketOptions { core.options }

    init(context: ZMQContext, options: SocketOptions, behavior: SocketBehavior) {
        core = SocketCore(loop: context.eventLoopGroup.next(), options: options, behavior: behavior)
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
