#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import NIOCore

/// A multipart message: one or more frames that travel together.
///
/// A string literal, with or without interpolation, makes a one-frame message of its UTF-8 bytes:
/// `try await req.request("Hello \(number)")`.
public struct Message: Sendable, Hashable, ExpressibleByStringInterpolation, ExpressibleByArrayLiteral {
    public var frames: [Frame]

    public init(_ frames: [Frame]) {
        self.frames = frames
    }

    public init(_ frame: Frame) {
        self.frames = [frame]
    }

    /// A single-frame message holding the UTF-8 bytes of `string`.
    public init(_ string: String) {
        self.frames = [Frame(string)]
    }

    public init(_ data: Data) {
        self.frames = [Frame(data)]
    }

    public init(stringLiteral value: String) {
        self.init(value)
    }

    public init(stringInterpolation: DefaultStringInterpolation) {
        self.init(String(stringInterpolation: stringInterpolation))
    }

    public init(arrayLiteral elements: Frame...) {
        self.frames = elements
    }

    /// The text of a one-frame message: its bytes read as UTF-8. nil when the message has more or fewer
    /// than one frame, or when the frame is not valid UTF-8.
    public var string: String? {
        frames.count == 1 ? frames[0].string : nil
    }

    /// The frames as the engine sends them. A message needs at least one frame on the wire.
    func wireFrames() throws -> [ByteBuffer] {
        guard !frames.isEmpty else { throw ZMQError.invalidState("a message needs at least one frame") }
        return frames.map(\.buffer)
    }

    init(wireFrames: [ByteBuffer]) {
        self.frames = wireFrames.map { Frame(buffer: $0) }
    }
}

/// Identifies one peer of a ROUTER socket.
public struct RoutingID: Sendable, Hashable {
    public var frame: Frame

    public init(_ frame: Frame) {
        self.frame = frame
    }
}

/// A message together with the peer it came from (or goes to).
public struct RoutedMessage: Sendable, Hashable {
    public var routingID: RoutingID
    public var message: Message

    public init(routingID: RoutingID, message: Message) {
        self.routingID = routingID
        self.message = message
    }
}
