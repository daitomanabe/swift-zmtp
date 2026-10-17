import NIOCore
import ZMTP

/// Answers requests from REQ (or DEALER) peers, one at a time: `receive()` takes a request, `reply(_:)`
/// answers it, and only then may the next `receive()` start.
public final class RepSocket: ZMQSocket {
    private let engine: SocketEngine

    public var options: SocketOptions { engine.options }

    public init(context: ZMQContext = .default, options: SocketOptions = SocketOptions()) {
        engine = SocketEngine(context: context, options: options, behavior: RepBehavior())
    }

    @discardableResult
    public func bind(_ endpoint: String) async throws -> String { try await engine.bind(endpoint) }

    public func connect(_ endpoint: String) async throws { try await engine.connect(endpoint) }

    public func close() async { await engine.close() }

    public func events() -> AsyncStream<SocketEvent> { engine.events() }

    /// Waits for the next request, taking the peers in turn. Throws `ZMQError.invalidState` when the
    /// previous request has not been answered. Cancelling the task ends the wait with
    /// `CancellationError` and loses no request.
    public func receive() async throws -> Message {
        Message(wireFrames: try await engine.receive())
    }

    /// Sends `message` as the answer to the request taken last. Throws `ZMQError.invalidState` when
    /// there is none. If the requester has gone, or its queue is full, the answer is dropped silently.
    public func reply(_ message: Message) async throws {
        let frames = try message.wireFrames()
        try await engine.perform { (behavior: RepBehavior) in try behavior.reply(frames) }
    }
}

final class RepBehavior: SocketBehavior {
    /// The request that `receive()` returned and `reply` has not answered yet.
    private struct OpenRequest {
        /// Where the answer goes; nil once that connection is gone.
        var pipe: Pipe?
        /// Frames up to and including the empty delimiter, as the requester sent them.
        var envelope: [ByteBuffer]
    }

    private var openRequest: OpenRequest?

    init() { super.init(socketType: .rep) }

    override func beforeReceive() throws {
        if openRequest != nil {
            throw ZMQError.invalidState("a REP socket must reply to the last request before receiving the next")
        }
    }

    /// Splits off the envelope (everything up to and including the first empty frame). A message
    /// without an empty frame, or with nothing after it, is dropped.
    override func received(_ frames: [ByteBuffer], from pipe: Pipe) -> InboundMessage? {
        guard let delimiter = frames.firstIndex(where: { $0.readableBytes == 0 }), delimiter + 1 < frames.count
        else { return nil }
        return InboundMessage(
            frames: Array(frames[(delimiter + 1)...]), envelope: Array(frames[...delimiter]))
    }

    override func delivered(_ message: InboundMessage, from pipe: Pipe) {
        openRequest = OpenRequest(pipe: pipe, envelope: message.envelope)
    }

    func reply(_ frames: [ByteBuffer]) throws {
        guard let request = openRequest else {
            throw ZMQError.invalidState("a REP socket can reply only after it received a request")
        }
        openRequest = nil
        // A requester that is gone or at its high-water mark misses the answer (as with libzmq).
        guard let pipe = request.pipe, pipe.canAccept, pipe.connection?.isAttached == true else { return }
        pipe.enqueue(request.envelope + frames)
    }

    override func pipeDetached(_ pipe: Pipe) { connectionLost(pipe) }

    override func pipeRemoved(_ pipe: Pipe) { connectionLost(pipe) }

    private func connectionLost(_ pipe: Pipe) {
        if openRequest?.pipe === pipe { openRequest?.pipe = nil }
    }
}
