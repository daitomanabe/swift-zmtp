/// Connects two sockets so that messages flow between them: the middle of a broker, a queue or a
/// forwarder.
///
/// Each `run` carries messages as long as both sockets are open. It returns normally as soon as either
/// socket is closed, and throws `CancellationError` when the calling task is cancelled. In both cases
/// every task the proxy started has finished by the time `run` returns. The sockets stay yours to bind,
/// connect and close; messages that were in flight when the proxy ended can be lost.
public enum ZMQProxy {
    /// Publish/subscribe broker. Messages flow from `frontend` (which connects to the publishers) to
    /// `backend` (which the subscribers connect to); the subscriptions of the subscribers flow the other
    /// way, so the publishers only send what somebody wants. Anything else a subscriber sends upstream is
    /// passed on to the publishers.
    public static func run(frontend: XSubSocket, backend: XPubSocket) async throws {
        try await relay([
            {
                while true {
                    try await backend.send(try await frontend.receive())
                }
            },
            {
                while true {
                    switch try await backend.receive() {
                    case .subscribe(let prefix): try await frontend.subscribe(prefix)
                    case .unsubscribe(let prefix): try await frontend.unsubscribe(prefix)
                    case .message(let message): try await frontend.send(message)
                    }
                }
            },
        ])
    }

    /// Request/reply broker. Requests flow from `frontend` (the clients) to `backend` (the workers),
    /// replies flow back. The routing ID of the client travels in front of the message through the
    /// backend, so the reply finds its way back. A reply for a client that is gone is dropped.
    public static func run(frontend: RouterSocket, backend: DealerSocket) async throws {
        try await relay([
            {
                while true {
                    let request = try await frontend.receive()
                    try await backend.send(Message([request.routingID.frame] + request.message.frames))
                }
            },
            {
                while true {
                    var frames = try await backend.receive().frames
                    // The first frame is the client; a reply with nothing after it cannot be sent on.
                    let routingID = RoutingID(frames.removeFirst())
                    guard !frames.isEmpty else { continue }
                    try await frontend.send(Message(frames), to: routingID)
                }
            },
        ])
    }

    /// Pipeline stage. Messages flow from `frontend` (which receives from the producers) to `backend`
    /// (which hands them to the workers). `backend.send` waits when no worker can take a message, so the
    /// proxy then stops reading from `frontend` and the producers feel the pressure.
    public static func run(frontend: PullSocket, backend: PushSocket) async throws {
        try await relay([
            {
                while true {
                    try await backend.send(try await frontend.receive())
                }
            }
        ])
    }

    /// Runs the directions side by side. Each is an endless loop, so it only ends by throwing: a closed
    /// socket ends the proxy quietly, anything else (also cancellation) is rethrown. When one direction
    /// ends, the group cancels the others and waits for them before `relay` returns.
    private static func relay(_ directions: [@Sendable () async throws -> Void]) async throws {
        do {
            try await withThrowingTaskGroup(of: Void.self) { group in
                for direction in directions {
                    group.addTask { try await direction() }
                }
                try await group.next()
                group.cancelAll()
            }
        } catch ZMQError.socketClosed {
            // Cancelling the task while a socket closes can show up as either error; cancellation wins.
            try Task.checkCancellation()
        }
    }
}
