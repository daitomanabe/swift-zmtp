import NIOCore

/// Flag bits of a ZMTP 3.x frame (the first byte on the wire).
struct FrameFlags: OptionSet, Sendable {
    let rawValue: UInt8

    /// More frames of the same message follow.
    static let more = FrameFlags(rawValue: 0x01)
    /// The size field is 8 bytes instead of 1.
    static let long = FrameFlags(rawValue: 0x02)
    /// The body is a command, not message data.
    static let command = FrameFlags(rawValue: 0x04)

    /// Bits 3...7 must be zero.
    static let reservedMask: UInt8 = 0xF8
}

/// One frame as it sits on the wire after the mechanism has had its say: `.long` is never set here,
/// it is a property of the encoding only.
struct WireFrame: Sendable {
    var flags: FrameFlags
    var body: ByteBuffer
}

/// Frame encoding and decoding (the framing layer of 23/ZMTP).
enum FrameCodec {
    /// Writes flags and size. `flags` must not contain `.long`; it is added when the size needs 8 bytes.
    static func writeHeader(flags: FrameFlags, length: Int, into buffer: inout ByteBuffer) {
        if length > ZMTP.maxShortFrameSize {
            buffer.writeInteger(flags.union(.long).rawValue)
            buffer.writeInteger(UInt64(length))
        } else {
            buffer.writeInteger(flags.rawValue)
            buffer.writeInteger(UInt8(length))
        }
    }

    /// Writes a whole frame: header, then the body copied in.
    static func write(_ frame: WireFrame, into buffer: inout ByteBuffer) {
        writeHeader(flags: frame.flags, length: frame.body.readableBytes, into: &buffer)
        buffer.writeImmutableBuffer(frame.body)
    }

    /// Takes one complete frame off the front of `buffer`, or returns nil (and leaves `buffer` alone)
    /// when more bytes are needed. The body is a slice of `buffer`, not a copy.
    /// Header errors and oversized frames are reported as soon as the header is in, before the body arrives.
    static func decode(_ buffer: inout ByteBuffer, maxFrameSize: Int?) throws(ZMTPError) -> WireFrame? {
        let start = buffer.readerIndex
        guard let rawFlags: UInt8 = buffer.getInteger(at: start) else { return nil }
        guard rawFlags & FrameFlags.reservedMask == 0 else {
            throw .malformedFrame("reserved flag bits are set (0x\(String(rawFlags, radix: 16)))")
        }
        let flags = FrameFlags(rawValue: rawFlags)
        if flags.contains(.command) && flags.contains(.more) {
            throw .malformedFrame("a command frame cannot have MORE set")
        }

        let headerLength: Int
        let length: UInt64
        if flags.contains(.long) {
            guard let size: UInt64 = buffer.getInteger(at: start + 1) else { return nil }
            headerLength = 9
            length = size
        } else {
            guard let size: UInt8 = buffer.getInteger(at: start + 1) else { return nil }
            headerLength = 2
            length = UInt64(size)
        }

        if let maxFrameSize, length > UInt64(max(maxFrameSize, 0)) { throw .messageTooLarge }
        guard length <= UInt64(Int.max - headerLength) else { throw .messageTooLarge }
        let bodyLength = Int(length)
        guard buffer.readableBytes - headerLength >= bodyLength else { return nil }

        buffer.moveReaderIndex(forwardBy: headerLength)
        // The length was checked above, so the slice exists.
        let body = buffer.readSlice(length: bodyLength) ?? ByteBuffer()
        return WireFrame(flags: flags.subtracting(.long), body: body)
    }
}
