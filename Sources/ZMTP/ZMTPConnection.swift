import NIOCore

/// The ZMTP side of one connection as a state machine over bytes: greeting, handshake, framing,
/// commands. It owns no socket, clock or thread. Feed it received bytes with `receive(_:)`, send what
/// comes back as `.write`, and close the transport after `.closed`.
public struct ZMTPConnection: Sendable {
    public struct Configuration: Sendable {
        public var socketType: SocketType
        /// Goes into Identity when `socketType.sendsIdentity`, and its length into the greeting.
        public var routingID: [UInt8]
        /// Largest accepted frame body, per frame (like libzmq's MAXMSGSIZE). nil: no limit.
        public var maxMessageSize: Int?

        public init(socketType: SocketType, routingID: [UInt8] = [], maxMessageSize: Int? = nil) {
            self.socketType = socketType
            self.routingID = routingID
            self.maxMessageSize = maxMessageSize
        }
    }

    /// Something the transport has to act on. A single `receive` returns these in the order they happened.
    public enum Output: Sendable, Equatable {
        /// Send these bytes to the peer, in this order.
        case write(ByteBuffer)
        /// The handshake is done; messages may flow.
        case ready(peer: Metadata, minorVersion: UInt8)
        /// One complete multipart message (the data frames, bodies are slices of the received bytes).
        case message([ByteBuffer])
        /// The peer subscribed (we are PUB or XPUB). `topic` is a prefix match.
        case subscribe(ByteBuffer)
        /// The peer cancelled a subscription.
        case cancel(ByteBuffer)
        /// The peer sent a PING; `ttl` is its time-to-live in tenths of a second (0: none). The PONG
        /// answer is a `.write` that follows this output, so there is nothing to answer.
        case pingReceived(ttl: UInt16)
        /// Close the transport. Nothing follows, and `receive` returns nothing from now on.
        case closed(ZMTPError)
    }

    private enum Phase: Sendable {
        case idle, greeting, handshake, ready, closed
    }

    private let configuration: Configuration
    private var phase = Phase.idle
    private var mechanism = Mechanism.null
    /// Received bytes that do not make a whole greeting or frame yet.
    private var inbound = ByteBuffer()
    /// Frames of the message being assembled.
    private var partial: [ByteBuffer] = []
    /// Minor version of the peer, known once its greeting is complete (1 for any major above 3).
    public private(set) var peerMinorVersion: UInt8?

    public init(configuration: Configuration) {
        self.configuration = configuration
    }

    /// True between the end of the handshake and the first `.closed`.
    public var isReady: Bool { phase == .ready }

    /// Call once when the transport is up. Returns our greeting (64 bytes in one write).
    public mutating func start() -> [Output] {
        guard phase == .idle else { return [] }
        phase = .greeting
        let greeting = Greeting.encode(routingIDLength: configuration.routingID.count, mechanism: mechanism.name)
        return [.write(greeting)]
    }

    /// Feeds bytes from the peer; partial greetings and frames are kept until the rest arrives.
    public mutating func receive(_ bytes: ByteBuffer) -> [Output] {
        var outputs: [Output] = []
        switch phase {
        case .closed:
            return outputs
        case .idle:
            fail(.unexpected("receive() before start()"), into: &outputs)
            return outputs
        case .greeting, .handshake, .ready:
            break
        }

        if inbound.readableBytes == 0 {
            // Nothing pending: adopt the bytes as they are, so message frames become slices of them.
            inbound = bytes
        } else {
            inbound.discardReadBytes()
            inbound.writeImmutableBuffer(bytes)
        }

        do throws(ZMTPError) {
            try process(into: &outputs)
        } catch {
            fail(error, into: &outputs)
        }
        if inbound.readableBytes == 0 { inbound = ByteBuffer() }
        return outputs
    }

    /// Appends the wire form of one message (all frames, MORE set on all but the last).
    /// Precondition: `isReady`, and `frames` is not empty.
    public func encodeMessage(_ frames: [ByteBuffer], into buffer: inout ByteBuffer) {
        precondition(isReady, "encodeMessage before the handshake finished")
        precondition(!frames.isEmpty, "a message has at least one frame")
        let last = frames.count - 1
        for (index, body) in frames.enumerated() {
            mechanism.encode(WireFrame(flags: index == last ? [] : .more, body: body), into: &buffer)
        }
    }

    /// Appends a subscription for `topic`: a SUBSCRIBE command for a 3.1 peer, a data frame starting
    /// with 0x01 for a 3.0 peer. Precondition: `isReady`.
    public func encodeSubscribe(_ topic: ByteBuffer, into buffer: inout ByteBuffer) {
        encodeSubscription(topic, subscribe: true, into: &buffer)
    }

    /// Appends the cancel of a subscription (CANCEL command, or a data frame starting with 0x00).
    /// Precondition: `isReady`.
    public func encodeCancel(_ topic: ByteBuffer, into buffer: inout ByteBuffer) {
        encodeSubscription(topic, subscribe: false, into: &buffer)
    }

    /// Appends a PING command. `ttl` is in tenths of a second: the peer should close the connection if it
    /// hears nothing for that long (0: no TTL). `context` is echoed by the PONG and is at most 16 bytes.
    /// The connection sends no timers of its own; the caller decides when to ping.
    /// Precondition: `isReady`.
    public func encodePing(ttl: UInt16, context: ByteBuffer, into buffer: inout ByteBuffer) {
        precondition(isReady, "PING before the handshake finished")
        precondition(context.readableBytes <= 16, "a PING context is at most 16 bytes")
        mechanism.encode(
            WireFrame(flags: .command, body: Command.ping(ttl: ttl, context: context).encodedBody()), into: &buffer)
    }

    // MARK: - Writing

    private func encodeSubscription(_ topic: ByteBuffer, subscribe: Bool, into buffer: inout ByteBuffer) {
        precondition(isReady, "subscriptions before the handshake finished")
        if (peerMinorVersion ?? 0) >= 1 {
            let command: Command = subscribe ? .subscribe(topic: topic) : .cancel(topic: topic)
            mechanism.encode(WireFrame(flags: .command, body: command.encodedBody()), into: &buffer)
        } else {
            var body = ByteBufferAllocator().buffer(capacity: 1 + topic.readableBytes)
            body.writeInteger(UInt8(subscribe ? 1 : 0))
            body.writeImmutableBuffer(topic)
            mechanism.encode(WireFrame(flags: [], body: body), into: &buffer)
        }
    }

    /// Handshake commands, framed without any mechanism wrapping, as one `.write`.
    private static func write(commands: [Command]) -> Output {
        var buffer = ByteBuffer()
        for command in commands { command.write(into: &buffer) }
        return .write(buffer)
    }

    // MARK: - Reading

    private mutating func fail(_ error: ZMTPError, into outputs: inout [Output]) {
        phase = .closed
        inbound = ByteBuffer()
        partial = []
        outputs.append(.closed(error))
    }

    /// Consumes `inbound` as far as it goes. Errors are thrown after the outputs collected so far are
    /// already in `outputs`, so a final `.write` (an ERROR command) is not lost.
    private mutating func process(into outputs: inout [Output]) throws(ZMTPError) {
        while true {
            switch phase {
            case .idle, .closed:
                return

            case .greeting:
                guard let peer = try Greeting.validate(inbound) else { return }
                inbound.moveReaderIndex(forwardBy: ZMTP.greetingLength)
                guard peer.mechanism == mechanism.name else {
                    throw .mechanismMismatch(local: mechanism.name, peer: peer.mechanism)
                }
                peerMinorVersion = peer.minorVersion
                phase = .handshake
                let local = localProperties()
                outputs.append(Self.write(commands: mechanism.begin(local: local)))

            case .handshake:
                guard let frame = try FrameCodec.decode(&inbound, maxFrameSize: configuration.maxMessageSize)
                else { return }
                guard frame.flags.contains(.command) else {
                    throw .unexpected("data frame before the handshake finished")
                }
                switch try mechanism.receive(handshake: try Command(body: frame.body)) {
                case .send(let commands):
                    outputs.append(Self.write(commands: commands))
                case .complete(let peer):
                    try completeHandshake(peer: peer, into: &outputs)
                }

            case .ready:
                guard let wire = try FrameCodec.decode(&inbound, maxFrameSize: configuration.maxMessageSize)
                else { return }
                let frame = try mechanism.decode(wire)
                if frame.flags.contains(.command) {
                    try handle(command: try Command(body: frame.body), into: &outputs)
                } else {
                    partial.append(frame.body)
                    if !frame.flags.contains(.more) { finishMessage(into: &outputs) }
                }
            }
        }
    }

    /// Socket-Type, then Identity for the types that send it (the order libzmq uses).
    private func localProperties() -> Metadata {
        var properties = [
            Metadata.Property(name: "Socket-Type", value: ByteBuffer(string: configuration.socketType.rawValue))
        ]
        if configuration.socketType.sendsIdentity {
            properties.append(Metadata.Property(name: "Identity", value: ByteBuffer(bytes: configuration.routingID)))
        }
        return Metadata(properties: properties)
    }

    private mutating func completeHandshake(peer: Metadata, into outputs: inout [Output]) throws(ZMTPError) {
        guard let peerType = peer.socketType else { throw .missingSocketType }
        guard let type = SocketType(rawValue: peerType), configuration.socketType.isCompatible(with: type) else {
            outputs.append(Self.write(commands: [.error(reason: "Invalid socket type")]))
            throw .incompatibleSocketType(local: configuration.socketType, peer: peerType)
        }
        phase = .ready
        outputs.append(.ready(peer: peer, minorVersion: peerMinorVersion ?? 0))
    }

    private mutating func handle(command: Command, into outputs: inout [Output]) throws(ZMTPError) {
        switch command {
        case .ping(let ttl, let context):
            outputs.append(.pingReceived(ttl: ttl))
            var buffer = ByteBuffer()
            mechanism.encode(WireFrame(flags: .command, body: Command.pong(context: context).encodedBody()), into: &buffer)
            outputs.append(.write(buffer))
        case .pong, .unknown:
            break
        case .subscribe(let topic):
            outputs.append(.subscribe(topic))
        case .cancel(let topic):
            outputs.append(.cancel(topic))
        case .error(let reason):
            throw .peerError(reason: reason)
        case .ready:
            throw .unexpected("second READY")
        }
    }

    private mutating func finishMessage(into outputs: inout [Output]) {
        var frames: [ByteBuffer] = []
        swap(&frames, &partial)

        // A ZMTP 3.0 peer subscribes with a one-frame message starting with 0x01 (subscribe) or 0x00 (cancel).
        // libzmq accepts this form from any peer, so we do too.
        if configuration.socketType == .pub || configuration.socketType == .xpub, frames.count == 1,
            let marker: UInt8 = frames[0].getInteger(at: frames[0].readerIndex), marker <= 1
        {
            var topic = frames[0]
            topic.moveReaderIndex(forwardBy: 1)
            outputs.append(marker == 1 ? .subscribe(topic) : .cancel(topic))
            return
        }
        outputs.append(.message(frames))
    }
}
