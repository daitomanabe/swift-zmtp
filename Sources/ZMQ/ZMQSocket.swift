/// What every socket type can do.
public protocol ZMQSocket: Sendable {
    var options: SocketOptions { get }
    /// Starts listening on `endpoint` and returns the endpoint actually bound
    /// (`tcp://127.0.0.1:*` gives the chosen port). A socket may bind and connect any number of times.
    @discardableResult func bind(_ endpoint: String) async throws -> String
    /// Starts connecting to `endpoint` and returns at once; the connection is made (and remade after
    /// failures) in the background. Messages can be sent before the peer is there.
    func connect(_ endpoint: String) async throws
    /// Closes the socket after queued messages are sent or `options.linger` has passed. Can be called
    /// more than once; later operations throw `ZMQError.socketClosed`. A socket that is released
    /// without `close()` is closed at once, without waiting.
    func close() async
    /// A new stream of what happens to the socket's connections (`SocketEvent`): listening, connecting,
    /// handshakes, disconnects. Every call makes an independent stream, so several listeners can watch one
    /// socket. A stream shows the events from the time of the call on and keeps the newest 256 when its
    /// reader is slow. The last event is `.closed`; the stream ends after it, also when the socket is
    /// closed already.
    func events() -> AsyncStream<SocketEvent>
}

/// Calls `receive()` until the socket is closed, then ends without an error.
public struct MessageSequence<Element: Sendable>: AsyncSequence, Sendable {
    private let receive: @Sendable () async throws -> Element

    package init(_ receive: @escaping @Sendable () async throws -> Element) {
        self.receive = receive
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        fileprivate let receive: @Sendable () async throws -> Element
        private var finished = false

        fileprivate init(receive: @escaping @Sendable () async throws -> Element) {
            self.receive = receive
        }

        public mutating func next() async throws -> Element? {
            guard !finished else { return nil }
            do {
                return try await receive()
            } catch ZMQError.socketClosed {
                finished = true
                return nil
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(receive: receive)
    }
}
