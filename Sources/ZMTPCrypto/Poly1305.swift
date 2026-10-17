// Poly1305 one-time authenticator.
//
// D. J. Bernstein, "The Poly1305-AES message-authentication code" (2005);
// RFC 8439 section 2.5 for the exact definition used here:
//
//   r = clamp(key[0..<16]),  s = key[16..<32]
//   for each 16-byte block m (the last may be shorter): n = m as a little-endian
//       number, plus 2^(8 * length) ; acc = (acc + n) * r  mod (2^130 - 5)
//   tag = (acc + s) mod 2^128
//
// Written from that description. The number representation is five 26-bit limbs
// held in 64-bit words, with products accumulated in 64 bits; 2^130 = 5 mod p
// folds the high limbs back with a factor of 5.

/// A 128-bit Poly1305 tag as two little-endian 64-bit halves.
package struct Poly1305Tag: Sendable {
    package var low: UInt64
    package var high: UInt64

    package init(low: UInt64, high: UInt64) {
        self.low = low
        self.high = high
    }

    /// Reads 16 bytes. Returns nil if `bytes` does not have exactly 16 bytes.
    package init?(bytes: UnsafeRawBufferPointer) {
        guard bytes.count == Poly1305.tagLength else { return nil }
        low = UInt64(littleEndian: bytes.loadUnaligned(fromByteOffset: 0, as: UInt64.self))
        high = UInt64(littleEndian: bytes.loadUnaligned(fromByteOffset: 8, as: UInt64.self))
    }

    /// Writes the 16 tag bytes. `buffer` must have exactly 16 bytes.
    package func store(to buffer: UnsafeMutableRawBufferPointer) {
        precondition(buffer.count == Poly1305.tagLength)
        buffer.storeBytes(of: low.littleEndian, toByteOffset: 0, as: UInt64.self)
        buffer.storeBytes(of: high.littleEndian, toByteOffset: 8, as: UInt64.self)
    }

    package var bytes: [UInt8] {
        var out = [UInt8](repeating: 0, count: Poly1305.tagLength)
        out.withUnsafeMutableBytes { store(to: $0) }
        return out
    }

    /// Constant-time comparison: no early exit, no branch on the tag values.
    package func isEqual(to other: Poly1305Tag) -> Bool {
        ((low ^ other.low) | (high ^ other.high)) == 0
    }
}

package enum Poly1305 {
    package static let keyLength = 32
    package static let tagLength = 16

    /// The tag of `message` under the one-time key `key` (32 bytes: r then s).
    /// Returns nil if `key` does not have exactly 32 bytes.
    /// A key must never authenticate two different messages.
    package static func authenticate(_ message: UnsafeRawBufferPointer, key: UnsafeRawBufferPointer) -> Poly1305Tag? {
        guard key.count == keyLength else { return nil }
        var k = SIMD8<UInt32>()
        for i in 0..<8 { k[i] = loadLittleEndian32(key, 4 * i) }
        return authenticate(
            message,
            r: SIMD4(k[0], k[1], k[2], k[3]),
            s: SIMD4(k[4], k[5], k[6], k[7])
        )
    }

    /// Convenience over arrays. Returns nil unless `key` has 32 bytes.
    package static func authenticate(_ message: [UInt8], key: [UInt8]) -> [UInt8]? {
        message.withUnsafeBytes { m in
            key.withUnsafeBytes { k in
                authenticate(m, key: k)?.bytes
            }
        }
    }

    /// The core. `r` and `s` are the two key halves as little-endian words
    /// (r is clamped here).
    ///
    /// Timing: the only loop runs once per 16 message bytes (the message length is
    /// public); every operation inside is an add, multiply, shift, mask or XOR on
    /// fixed-width words, and the final "subtract p if acc >= p" is a mask select,
    /// not a branch. No table lookups.
    static func authenticate(_ message: UnsafeRawBufferPointer, r: SIMD4<UInt32>, s: SIMD4<UInt32>) -> Poly1305Tag {
        var state = Poly1305State(r: r)

        var offset = 0
        while message.count - offset >= 16 {
            state.absorb(
                loadLittleEndian32(message, offset),
                loadLittleEndian32(message, offset + 4),
                loadLittleEndian32(message, offset + 8),
                loadLittleEndian32(message, offset + 12),
                top: 1 << 24  // the 2^128 bit of a full block, in limb 4
            )
            offset += 16
        }

        // Short last block: the bytes, then a single 1 byte, and no 2^128 bit.
        let remaining = message.count - offset
        if remaining > 0 {
            var n = SIMD4<UInt32>()
            for i in 0..<remaining {
                n[i >> 2] |= UInt32(message[offset + i]) << UInt32((i & 3) << 3)
            }
            n[remaining >> 2] |= 1 << UInt32((remaining & 3) << 3)
            state.absorb(n[0], n[1], n[2], n[3], top: 0)
        }

        return state.finish(addingS: s)
    }
}

/// The accumulator h (five 26-bit limbs, each held in a UInt64) and the clamped key r.
private struct Poly1305State {
    private static let mask26: UInt64 = 0x3ff_ffff

    private var h0: UInt64 = 0, h1: UInt64 = 0, h2: UInt64 = 0, h3: UInt64 = 0, h4: UInt64 = 0
    private let r0: UInt64, r1: UInt64, r2: UInt64, r3: UInt64, r4: UInt64
    /// 5 * r for the limbs that wrap around 2^130 (2^130 = 5 mod p).
    private let s1: UInt64, s2: UInt64, s3: UInt64, s4: UInt64

    init(r: SIMD4<UInt32>) {
        // Clamp r (RFC 8439 2.5): clear the top 4 bits of each word and the low
        // 2 bits of words 1...3.
        let t0 = r[0] & 0x0fff_ffff
        let t1 = r[1] & 0x0fff_fffc
        let t2 = r[2] & 0x0fff_fffc
        let t3 = r[3] & 0x0fff_fffc
        let mask = Self.mask26
        r0 = UInt64(t0) & mask
        r1 = (UInt64(t0 >> 26) | UInt64(t1) << 6) & mask
        r2 = (UInt64(t1 >> 20) | UInt64(t2) << 12) & mask
        r3 = (UInt64(t2 >> 14) | UInt64(t3) << 18) & mask
        r4 = UInt64(t3 >> 8)
        s1 = r1 &* 5
        s2 = r2 &* 5
        s3 = r3 &* 5
        s4 = r4 &* 5
    }

    /// h = (h + n) * r mod p, where n is a block given as four little-endian words
    /// and `top` is 2^24 for the 2^128 bit of a full block (0 for a short last block).
    ///
    /// Bounds: limbs of h are below 2^27 after adding a block, s_i below 2^29, so each
    /// sum of five products stays below 2^59: no 64-bit overflow.
    @inline(__always)
    mutating func absorb(_ n0: UInt32, _ n1: UInt32, _ n2: UInt32, _ n3: UInt32, top: UInt64) {
        let mask = Self.mask26
        h0 &+= UInt64(n0) & mask
        h1 &+= (UInt64(n0 >> 26) | UInt64(n1) << 6) & mask
        h2 &+= (UInt64(n1 >> 20) | UInt64(n2) << 12) & mask
        h3 &+= (UInt64(n2 >> 14) | UInt64(n3) << 18) & mask
        h4 &+= UInt64(n3 >> 8) | top

        let d0 = h0 &* r0 &+ h1 &* s4 &+ h2 &* s3 &+ h3 &* s2 &+ h4 &* s1
        var d1 = h0 &* r1 &+ h1 &* r0 &+ h2 &* s4 &+ h3 &* s3 &+ h4 &* s2
        var d2 = h0 &* r2 &+ h1 &* r1 &+ h2 &* r0 &+ h3 &* s4 &+ h4 &* s3
        var d3 = h0 &* r3 &+ h1 &* r2 &+ h2 &* r1 &+ h3 &* r0 &+ h4 &* s4
        var d4 = h0 &* r4 &+ h1 &* r3 &+ h2 &* r2 &+ h3 &* r1 &+ h4 &* r0

        // Carry between limbs; the carry out of limb 4 re-enters limb 0 times 5.
        var c = d0 >> 26
        h0 = d0 & mask
        d1 &+= c; c = d1 >> 26; h1 = d1 & mask
        d2 &+= c; c = d2 >> 26; h2 = d2 & mask
        d3 &+= c; c = d3 >> 26; h3 = d3 & mask
        d4 &+= c; c = d4 >> 26; h4 = d4 & mask
        h0 &+= c &* 5
        c = h0 >> 26; h0 &= mask
        h1 &+= c
    }

    /// tag = (h mod p + s) mod 2^128, with the reduction done by a mask select.
    mutating func finish(addingS s: SIMD4<UInt32>) -> Poly1305Tag {
        let mask = Self.mask26

        // Fully carry h so every limb is below 2^26.
        var c = h1 >> 26; h1 &= mask
        h2 &+= c; c = h2 >> 26; h2 &= mask
        h3 &+= c; c = h3 >> 26; h3 &= mask
        h4 &+= c; c = h4 >> 26; h4 &= mask
        h0 &+= c &* 5; c = h0 >> 26; h0 &= mask
        h1 &+= c

        // g = h + 5 - 2^130. If h >= p = 2^130 - 5, g = h - p is the reduced value;
        // otherwise g went negative and h is already reduced. Choose with a mask.
        var g0 = h0 &+ 5; c = g0 >> 26; g0 &= mask
        var g1 = h1 &+ c; c = g1 >> 26; g1 &= mask
        var g2 = h2 &+ c; c = g2 >> 26; g2 &= mask
        var g3 = h3 &+ c; c = g3 >> 26; g3 &= mask
        let g4 = h4 &+ c &- (1 << 26)
        // g4 wrapped to a huge value (top bit set) iff g was negative: keep h, else take g.
        let takeG = (g4 >> 63) &- 1  // all ones if g4's top bit is clear, else zero
        h0 = (h0 & ~takeG) | (g0 & takeG)
        h1 = (h1 & ~takeG) | (g1 & takeG)
        h2 = (h2 & ~takeG) | (g2 & takeG)
        h3 = (h3 & ~takeG) | (g3 & takeG)
        h4 = (h4 & ~takeG) | (g4 & mask & takeG)

        // Repack the limbs into 128 bits (bits 128 and up are dropped: mod 2^128),
        // then add s with carry.
        let w0 = UInt64(UInt32(truncatingIfNeeded: h0 | h1 << 26))
        let w1 = UInt64(UInt32(truncatingIfNeeded: h1 >> 6 | h2 << 20))
        let w2 = UInt64(UInt32(truncatingIfNeeded: h2 >> 12 | h3 << 14))
        let w3 = UInt64(UInt32(truncatingIfNeeded: h3 >> 18 | h4 << 8))

        var f = w0 &+ UInt64(s[0])
        let o0 = UInt32(truncatingIfNeeded: f)
        f = w1 &+ UInt64(s[1]) &+ (f >> 32)
        let o1 = UInt32(truncatingIfNeeded: f)
        f = w2 &+ UInt64(s[2]) &+ (f >> 32)
        let o2 = UInt32(truncatingIfNeeded: f)
        f = w3 &+ UInt64(s[3]) &+ (f >> 32)
        let o3 = UInt32(truncatingIfNeeded: f)

        return Poly1305Tag(
            low: UInt64(o0) | UInt64(o1) << 32,
            high: UInt64(o2) | UInt64(o3) << 32
        )
    }
}
