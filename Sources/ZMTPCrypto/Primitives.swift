// Shared value types and small helpers for the CURVE primitives.
//
// Everything in this module is `package` access: it is for the ZMTP target only,
// not a public cryptography library.

/// A 32-byte symmetric key for HSalsa20, XSalsa20 and `SecretBox`.
///
/// Also the type of a precomputed NaCl box key (`crypto_box_beforenm`): once the
/// X25519 and HSalsa20 steps are done, a box is a secretbox under that key.
/// The key is stored as eight little-endian 32-bit words, the form the Salsa20
/// family consumes, so using it needs no per-message byte shuffling.
///
/// Deliberately not `Equatable` (an `==` on key material would not be constant time).
package struct SecretBoxKey: Sendable {
    package static let length = 32

    /// Key bytes 0..<32 as little-endian words.
    package let words: SIMD8<UInt32>

    package init(words: SIMD8<UInt32>) {
        self.words = words
    }

    /// Reads 32 bytes. Returns nil if `bytes` does not have exactly 32 bytes.
    package init?(bytes: UnsafeRawBufferPointer) {
        guard bytes.count == Self.length else { return nil }
        var w = SIMD8<UInt32>()
        for i in 0..<8 { w[i] = loadLittleEndian32(bytes, 4 * i) }
        self.words = w
    }

    /// Returns nil if `bytes` does not have exactly 32 bytes.
    package init?(_ bytes: [UInt8]) {
        guard let key = bytes.withUnsafeBytes({ SecretBoxKey(bytes: $0) }) else { return nil }
        self = key
    }

    /// The 32 key bytes.
    package var bytes: [UInt8] {
        var out = [UInt8](repeating: 0, count: Self.length)
        out.withUnsafeMutableBytes { buffer in
            for i in 0..<8 { storeLittleEndian32(words[i], buffer, 4 * i) }
        }
        return out
    }
}

/// A 24-byte NaCl nonce.
///
/// Stored as the first 16 bytes (HSalsa20 input) and the last 8 bytes (Salsa20
/// nonce) of the XSalsa20 construction, as little-endian words.
package struct SecretBoxNonce: Sendable {
    package static let length = 24

    /// Nonce bytes 0..<16.
    package let head: SIMD4<UInt32>
    /// Nonce bytes 16..<24.
    package let tail: SIMD2<UInt32>

    /// Reads 24 bytes. Returns nil if `bytes` does not have exactly 24 bytes.
    package init?(bytes: UnsafeRawBufferPointer) {
        guard bytes.count == Self.length else { return nil }
        var h = SIMD4<UInt32>()
        for i in 0..<4 { h[i] = loadLittleEndian32(bytes, 4 * i) }
        self.head = h
        self.tail = SIMD2(loadLittleEndian32(bytes, 16), loadLittleEndian32(bytes, 20))
    }

    /// Returns nil if `bytes` does not have exactly 24 bytes.
    package init?(_ bytes: [UInt8]) {
        guard let nonce = bytes.withUnsafeBytes({ SecretBoxNonce(bytes: $0) }) else { return nil }
        self = nonce
    }

    /// A nonce made of a fixed prefix and a suffix (`prefix.count + suffix.count == 24`),
    /// e.g. CurveZMQ's `"WELCOME-"` followed by a 16-byte random value.
    package init?(prefix: [UInt8], suffix: [UInt8]) {
        guard prefix.count + suffix.count == Self.length else { return nil }
        guard let nonce = SecretBoxNonce(prefix + suffix) else { return nil }
        self = nonce
    }

    /// A nonce made of a 16-byte prefix and an 8-byte big-endian counter,
    /// e.g. CurveZMQ's `"CurveZMQMESSAGEC"` followed by the short nonce.
    /// Returns nil unless `prefix` has exactly 16 bytes. Does not allocate.
    package init?(prefix: [UInt8], counter: UInt64) {
        guard prefix.count == 16 else { return nil }
        var h = SIMD4<UInt32>()
        prefix.withUnsafeBytes { buffer in
            for i in 0..<4 { h[i] = loadLittleEndian32(buffer, 4 * i) }
        }
        self.head = h
        // The eight big-endian counter bytes read as two little-endian words:
        // each word is the byte-swapped half of the counter.
        self.tail = SIMD2(
            UInt32(truncatingIfNeeded: counter >> 32).byteSwapped,
            UInt32(truncatingIfNeeded: counter).byteSwapped
        )
    }
}

/// Comparisons whose running time does not depend on the contents.
package enum ConstantTime {
    /// True if both buffers have the same length and the same bytes.
    /// The length is treated as public; the time depends only on it, and the
    /// loop never exits early on a difference.
    package static func equal(_ a: UnsafeRawBufferPointer, _ b: UnsafeRawBufferPointer) -> Bool {
        guard a.count == b.count else { return false }
        var difference: UInt8 = 0
        for i in 0..<a.count {
            difference |= a[i] ^ b[i]
        }
        return difference == 0
    }

    package static func equal(_ a: [UInt8], _ b: [UInt8]) -> Bool {
        a.withUnsafeBytes { x in b.withUnsafeBytes { y in equal(x, y) } }
    }

    /// True if every byte is zero (used to reject an all-zero X25519 result).
    package static func isZero(_ a: [UInt8]) -> Bool {
        var accumulated: UInt8 = 0
        for byte in a { accumulated |= byte }
        return accumulated == 0
    }
}

/// Cryptographically secure random bytes.
///
/// `SystemRandomNumberGenerator` is arc4random on Apple platforms and
/// getrandom(2) on Linux; both are meant for key material.
package enum SecureRandom {
    /// Fills `buffer` with random bytes.
    package static func fill(_ buffer: UnsafeMutableRawBufferPointer) {
        var generator = SystemRandomNumberGenerator()
        var offset = 0
        while offset + 8 <= buffer.count {
            buffer.storeBytes(of: generator.next(), toByteOffset: offset, as: UInt64.self)
            offset += 8
        }
        if offset < buffer.count {
            var word = generator.next()
            while offset < buffer.count {
                buffer[offset] = UInt8(truncatingIfNeeded: word)
                word >>= 8
                offset += 1
            }
        }
    }

    package static func bytes(_ count: Int) -> [UInt8] {
        var out = [UInt8](repeating: 0, count: count)
        out.withUnsafeMutableBytes { fill($0) }
        return out
    }
}

// MARK: - Byte order helpers (the whole Salsa20 family is little-endian)

@inline(__always)
func loadLittleEndian32(_ buffer: UnsafeRawBufferPointer, _ offset: Int) -> UInt32 {
    UInt32(littleEndian: buffer.loadUnaligned(fromByteOffset: offset, as: UInt32.self))
}

@inline(__always)
func loadLittleEndian32(_ buffer: UnsafeMutableRawBufferPointer, _ offset: Int) -> UInt32 {
    UInt32(littleEndian: buffer.loadUnaligned(fromByteOffset: offset, as: UInt32.self))
}

@inline(__always)
func storeLittleEndian32(_ value: UInt32, _ buffer: UnsafeMutableRawBufferPointer, _ offset: Int) {
    buffer.storeBytes(of: value.littleEndian, toByteOffset: offset, as: UInt32.self)
}
