import NIOCore

/// An ordered list of named binary properties, as carried by READY (and INITIATE in later mechanisms).
public struct Metadata: Sendable, Equatable {
    /// One name/value pair. Names are 1...255 bytes; values can be empty.
    public struct Property: Sendable, Equatable {
        public var name: String
        public var value: ByteBuffer

        public init(name: String, value: ByteBuffer) {
            self.name = name
            self.value = value
        }
    }

    /// Properties in wire order.
    public var properties: [Property]

    public init(properties: [Property] = []) {
        self.properties = properties
    }

    /// The first property called `name`; names compare case-insensitively.
    public subscript(name: String) -> ByteBuffer? {
        let wanted = name.lowercased()
        return properties.first { $0.name.lowercased() == wanted }?.value
    }

    /// Raw value of Socket-Type (for example "PUSH"), if present.
    public var socketType: String? {
        self["Socket-Type"].map { String(decoding: $0.readableBytesView, as: UTF8.self) }
    }

    /// Value of Identity, if present (it may be empty).
    public var identity: ByteBuffer? { self["Identity"] }

    /// Reads properties until `buffer` is empty.
    static func parse(_ buffer: inout ByteBuffer) throws(ZMTPError) -> Metadata {
        var properties: [Property] = []
        while buffer.readableBytes > 0 {
            guard let nameLength: UInt8 = buffer.readInteger(), nameLength > 0 else {
                throw .malformedCommand("property name must be 1...255 bytes")
            }
            guard let name = buffer.readString(length: Int(nameLength)) else {
                throw .malformedCommand("truncated property name")
            }
            guard let valueLength: UInt32 = buffer.readInteger(),
                let value = buffer.readSlice(length: Int(valueLength))
            else {
                throw .malformedCommand("truncated value of property \(name)")
            }
            properties.append(Property(name: name, value: value))
        }
        return Metadata(properties: properties)
    }

    func write(into buffer: inout ByteBuffer) {
        for property in properties {
            let nameLength = property.name.utf8.count
            precondition((1...255).contains(nameLength), "property names are 1...255 bytes")
            buffer.writeInteger(UInt8(nameLength))
            buffer.writeString(property.name)
            buffer.writeInteger(UInt32(property.value.readableBytes))
            buffer.writeImmutableBuffer(property.value)
        }
    }
}

/// A ZMTP command: the body of a frame with the COMMAND flag.
public enum Command: Sendable, Equatable {
    case ready(Metadata)
    case error(reason: String)
    case subscribe(topic: ByteBuffer)
    case cancel(topic: ByteBuffer)
    /// `ttl` is in tenths of a second; `context` is echoed by PONG.
    case ping(ttl: UInt16, context: ByteBuffer)
    case pong(context: ByteBuffer)
    /// Any other command (HELLO, WELCOME, INITIATE, ...). `body` is the data after the name.
    case unknown(name: String, body: ByteBuffer)

    /// Parses the body of a COMMAND frame: name length, name, data.
    public init(body: ByteBuffer) throws(ZMTPError) {
        var data = body
        guard let nameLength: UInt8 = data.readInteger() else {
            throw .malformedCommand("empty command")
        }
        guard let name = data.readString(length: Int(nameLength)) else {
            throw .malformedCommand("truncated command name")
        }
        switch name {
        case "READY":
            self = .ready(try Metadata.parse(&data))
        case "ERROR":
            guard let reasonLength: UInt8 = data.readInteger(),
                let reason = data.readString(length: Int(reasonLength))
            else {
                throw .malformedCommand("truncated ERROR reason")
            }
            self = .error(reason: reason)
        case "SUBSCRIBE":
            self = .subscribe(topic: data)
        case "CANCEL":
            self = .cancel(topic: data)
        case "PING":
            guard let ttl: UInt16 = data.readInteger() else {
                throw .malformedCommand("PING without TTL")
            }
            self = .ping(ttl: ttl, context: data)
        case "PONG":
            self = .pong(context: data)
        default:
            self = .unknown(name: name, body: data)
        }
    }

    /// Writes the whole frame: flags, size, body.
    public func write(into buffer: inout ByteBuffer) {
        FrameCodec.write(WireFrame(flags: .command, body: encodedBody()), into: &buffer)
    }

    /// Command name as it appears on the wire.
    var name: String {
        switch self {
        case .ready: "READY"
        case .error: "ERROR"
        case .subscribe: "SUBSCRIBE"
        case .cancel: "CANCEL"
        case .ping: "PING"
        case .pong: "PONG"
        case .unknown(let name, _): name
        }
    }

    /// The frame body: name length, name, data.
    func encodedBody() -> ByteBuffer {
        var body = ByteBuffer()
        let nameBytes = Array(name.utf8.prefix(255))
        body.writeInteger(UInt8(nameBytes.count))
        body.writeBytes(nameBytes)
        switch self {
        case .ready(let metadata):
            metadata.write(into: &body)
        case .error(let reason):
            // The reason is ASCII text of at most 255 bytes.
            let reasonBytes = Array(reason.utf8.prefix(255))
            body.writeInteger(UInt8(reasonBytes.count))
            body.writeBytes(reasonBytes)
        case .subscribe(let topic), .cancel(let topic):
            body.writeImmutableBuffer(topic)
        case .ping(let ttl, let context):
            body.writeInteger(ttl)
            body.writeImmutableBuffer(context)
        case .pong(let context):
            body.writeImmutableBuffer(context)
        case .unknown(_, let data):
            body.writeImmutableBuffer(data)
        }
        return body
    }
}
