// Z85: ZeroMQ's base-85 text form for binary data (rfc.zeromq.org 32/Z85).
//
// 4 bytes (a big-endian 32-bit number) become 5 characters, most significant
// digit first, from this 85-character alphabet. CURVE keys are 32 bytes = 40 characters.

package enum Z85 {
    private static let alphabet: [UInt8] = Array(
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#".utf8
    )

    /// Character -> digit value, 0xFF for characters outside the alphabet.
    private static let decodeTable: [UInt8] = {
        var table = [UInt8](repeating: 0xFF, count: 256)
        for (value, character) in alphabet.enumerated() {
            table[Int(character)] = UInt8(value)
        }
        return table
    }()

    /// Encodes `bytes`. Returns nil unless the length is a multiple of 4.
    package static func encode(_ bytes: [UInt8]) -> String? {
        guard bytes.count % 4 == 0 else { return nil }
        var out = [UInt8]()
        out.reserveCapacity(bytes.count / 4 * 5)
        var index = 0
        while index < bytes.count {
            var value = UInt32(bytes[index]) << 24
                | UInt32(bytes[index + 1]) << 16
                | UInt32(bytes[index + 2]) << 8
                | UInt32(bytes[index + 3])
            var digits = [UInt8](repeating: 0, count: 5)
            for position in stride(from: 4, through: 0, by: -1) {
                digits[position] = alphabet[Int(value % 85)]
                value /= 85
            }
            out.append(contentsOf: digits)
            index += 4
        }
        return String(decoding: out, as: UTF8.self)
    }

    /// Decodes `string`. Returns nil unless the length is a multiple of 5, every
    /// character is in the alphabet, and every group of 5 fits in 32 bits.
    package static func decode(_ string: String) -> [UInt8]? {
        let characters = Array(string.utf8)
        guard characters.count % 5 == 0 else { return nil }
        var out = [UInt8]()
        out.reserveCapacity(characters.count / 5 * 4)
        var index = 0
        while index < characters.count {
            var value: UInt64 = 0
            for position in 0..<5 {
                let digit = decodeTable[Int(characters[index + position])]
                guard digit != 0xFF else { return nil }
                value = value * 85 + UInt64(digit)
            }
            guard value <= UInt64(UInt32.max) else { return nil }
            out.append(UInt8(truncatingIfNeeded: value >> 24))
            out.append(UInt8(truncatingIfNeeded: value >> 16))
            out.append(UInt8(truncatingIfNeeded: value >> 8))
            out.append(UInt8(truncatingIfNeeded: value))
            index += 5
        }
        return out
    }
}
