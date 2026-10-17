import NIOCore

/// The 64-byte greeting that opens every ZMTP 3.x connection (23/ZMTP, 37/ZMTP).
enum Greeting {
    /// What we learn from a valid peer greeting.
    struct Peer: Sendable, Equatable {
        /// 1 when the peer announces a major version above 3 (treated as 3.1).
        var minorVersion: UInt8
        /// Mechanism name with the NUL padding removed.
        var mechanism: String
    }

    private static let mechanismOffset = 12
    private static let mechanismLength = 20

    /// Builds our greeting. The padding after the first signature byte carries the routing ID length + 1
    /// as a big-endian UInt64, like libzmq (it dates from ZMTP 1.0); the as-server byte is always 0.
    static func encode(routingIDLength: Int, mechanism: String) -> ByteBuffer {
        var buffer = ByteBufferAllocator().buffer(capacity: ZMTP.greetingLength)
        buffer.writeInteger(UInt8(0xFF))
        buffer.writeInteger(UInt64(routingIDLength + 1))
        buffer.writeInteger(UInt8(0x7F))
        buffer.writeInteger(ZMTP.majorVersion)
        buffer.writeInteger(ZMTP.minorVersion)
        var name = Array(mechanism.utf8.prefix(mechanismLength))
        name.append(contentsOf: repeatElement(0, count: mechanismLength - name.count))
        buffer.writeBytes(name)
        buffer.writeRepeatingByte(0, count: 1 + 31)  // as-server, filler
        return buffer
    }

    /// Checks the peer's greeting as far as it has arrived, without consuming anything: byte 0 at once,
    /// byte 9 at 10 bytes, the major version at 11 bytes. Returns nil until all 64 bytes are in.
    static func validate(_ buffer: ByteBuffer) throws(ZMTPError) -> Peer? {
        let start = buffer.readerIndex
        let count = buffer.readableBytes

        if count >= 1, buffer.getInteger(at: start, as: UInt8.self) != 0xFF { throw .badSignature }
        // Bit 0 of byte 9 is clear in a ZMTP 1.0 greeting.
        if count >= ZMTP.signatureLength,
            let last: UInt8 = buffer.getInteger(at: start + ZMTP.signatureLength - 1), last & 0x01 == 0
        {
            throw .badSignature
        }
        guard count >= ZMTP.signatureLength + 1, let major: UInt8 = buffer.getInteger(at: start + 10) else {
            return nil
        }
        if major < ZMTP.majorVersion { throw .unsupportedVersion(major: major) }
        guard count >= ZMTP.greetingLength else { return nil }

        let minor: UInt8 = major > ZMTP.majorVersion ? 1 : (buffer.getInteger(at: start + 11) ?? 0)
        let nameBytes = buffer.getBytes(at: start + mechanismOffset, length: mechanismLength) ?? []
        let name = String(decoding: nameBytes.prefix { $0 != 0 }, as: UTF8.self)
        return Peer(minorVersion: minor, mechanism: name)
    }
}
