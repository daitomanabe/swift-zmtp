import NIOCore
import ZMTP

/// Exclusive pair: one peer only, messages both ways. A second connection is closed. `send` waits
/// while there is no peer or its queue is full.
public final class PairSocket: ZMQSocket {
    private let engine: SocketEngine

    public var options: SocketOptions { engine.options }

    public init(context: ZMQContext = .default, options: SocketOptions = SocketOptions()) {
        engine = SocketEngine(context: context, options: options, behavior: PairBehavior())
    }

    @discardableResult
    public func bind(_ endpoint: String) async throws -> String { try await engine.bind(endpoint) }

    /// Throws `ZMQError.invalidState` when the socket already has a connected (or connecting) peer.
    public func connect(_ endpoint: String) async throws { try await engine.connect(endpoint) }

    public func close() async { await engine.close() }

    public func send(_ message: Message) async throws {
        try await engine.send(message.wireFrames())
    }

    public func receive() async throws -> Message {
        Message(wireFrames: try await engine.receive())
    }

    public var messages: MessageSequence<Message> {
        MessageSequence { try await self.receive() }
    }
}

final class PairBehavior: SocketBehavior {
    /// The one pipe: the connect-side pipe from `connect()`, or the first bind-side connection.
    private var peerPipe: Pipe?

    init() { super.init(socketType: .pair) }

    override func pipeCreated(_ pipe: Pipe) throws {
        guard peerPipe == nil else { throw ZMQError.invalidState("a PAIR socket has one peer only") }
        peerPipe = pipe
    }

    override func pipeAttached(_ pipe: Pipe, peer: Metadata) -> Bool {
        if let peerPipe { return peerPipe === pipe }
        peerPipe = pipe
        return true
    }

    override func pipeRemoved(_ pipe: Pipe) {
        if peerPipe === pipe { peerPipe = nil }
    }

    override func trySend(_ frames: [ByteBuffer]) throws -> SendOutcome {
        guard let peerPipe, peerPipe.canAccept else { return .wouldBlock }
        peerPipe.enqueue(frames)
        return .accepted
    }
}
