// Salsa20 core, HSalsa20 and XSalsa20.
//
// Salsa20:  D. J. Bernstein, "Salsa20 specification" (2005) and
//           "The Salsa20 family of stream ciphers" (2008). 20 rounds.
// HSalsa20, XSalsa20: D. J. Bernstein, "Extending the Salsa20 nonce" (2011).
//
// Written from the algorithm descriptions in those papers. Only additions,
// rotations and XORs on 32-bit words: no secret-dependent branches or table
// lookups.

/// The 16-word Salsa20 state. Plain fields (not an array) so the optimizer keeps
/// the whole state in registers during the rounds.
///
/// Layout (paper, section 9 / "expand 32-byte k"):
///
///     x0 = c0   x1 = k0   x2 = k1   x3 = k2
///     x4 = k3   x5 = c1   x6 = n0   x7 = n1
///     x8 = p0   x9 = p1   x10 = c2  x11 = k4
///     x12 = k5  x13 = k6  x14 = k7  x15 = c3
///
/// where c is the constant "expand 32-byte k", k the key, n the nonce, p the block position.
private struct SalsaState {
    var x0: UInt32, x1: UInt32, x2: UInt32, x3: UInt32
    var x4: UInt32, x5: UInt32, x6: UInt32, x7: UInt32
    var x8: UInt32, x9: UInt32, x10: UInt32, x11: UInt32
    var x12: UInt32, x13: UInt32, x14: UInt32, x15: UInt32

    /// "expand 32-byte k" as four little-endian words.
    static let c0: UInt32 = 0x6170_7865
    static let c1: UInt32 = 0x3320_646e
    static let c2: UInt32 = 0x7962_2d32
    static let c3: UInt32 = 0x6b20_6574

    /// Initial state. Salsa20 passes the nonce as `w6`/`w7` and the block counter as
    /// `w8`/`w9`; HSalsa20 passes its 16-byte input as `w6`...`w9`.
    init(key k: SIMD8<UInt32>, w6: UInt32, w7: UInt32, w8: UInt32, w9: UInt32) {
        x0 = Self.c0; x1 = k[0]; x2 = k[1]; x3 = k[2]
        x4 = k[3]; x5 = Self.c1; x6 = w6; x7 = w7
        x8 = w8; x9 = w9; x10 = Self.c2; x11 = k[4]
        x12 = k[5]; x13 = k[6]; x14 = k[7]; x15 = Self.c3
    }

    @inline(__always)
    private static func rotl(_ x: UInt32, _ n: UInt32) -> UInt32 {
        (x &<< n) | (x &>> (32 &- n))
    }

    /// Salsa20 quarter-round on (y0, y1, y2, y3) = (a, b, c, d).
    @inline(__always)
    private static func quarterRound(_ a: inout UInt32, _ b: inout UInt32, _ c: inout UInt32, _ d: inout UInt32) {
        b ^= rotl(a &+ d, 7)
        c ^= rotl(b &+ a, 9)
        d ^= rotl(c &+ b, 13)
        a ^= rotl(d &+ c, 18)
    }

    /// Ten double rounds = 20 rounds (a column round, then a row round, each time).
    mutating func twentyRounds() {
        for _ in 0..<10 {
            // Column round.
            Self.quarterRound(&x0, &x4, &x8, &x12)
            Self.quarterRound(&x5, &x9, &x13, &x1)
            Self.quarterRound(&x10, &x14, &x2, &x6)
            Self.quarterRound(&x15, &x3, &x7, &x11)
            // Row round.
            Self.quarterRound(&x0, &x1, &x2, &x3)
            Self.quarterRound(&x5, &x6, &x7, &x4)
            Self.quarterRound(&x10, &x11, &x8, &x9)
            Self.quarterRound(&x15, &x12, &x13, &x14)
        }
    }

    /// The 16 words as a vector (for the final addition and the XOR loop).
    var words: SIMD16<UInt32> {
        [x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15]
    }
}

/// HSalsa20: 20 Salsa20 rounds over (key, 16-byte input) and no feed-forward
/// addition; the output is words 0, 5, 10, 15, 6, 7, 8, 9 of the final state.
package enum HSalsa20 {
    /// Derives a 32-byte key. `input` is 16 bytes as four little-endian words
    /// (the first 16 bytes of an XSalsa20 nonce, or zero for `crypto_box_beforenm`).
    package static func derive(key: SecretBoxKey, input: SIMD4<UInt32>) -> SecretBoxKey {
        var s = SalsaState(key: key.words, w6: input[0], w7: input[1], w8: input[2], w9: input[3])
        s.twentyRounds()
        return SecretBoxKey(words: [s.x0, s.x5, s.x10, s.x15, s.x6, s.x7, s.x8, s.x9])
    }
}

/// The Salsa20/20 stream cipher with a 64-bit nonce and a 64-bit block counter.
package enum Salsa20 {
    package static let blockLength = 64

    /// Keystream block number `counter` (64 bytes as 16 little-endian words):
    /// the Salsa20 hash of the state, which includes the feed-forward addition.
    package static func block(key: SecretBoxKey, nonce: SIMD2<UInt32>, counter: UInt64) -> SIMD16<UInt32> {
        let initial = SalsaState(
            key: key.words,
            w6: nonce[0], w7: nonce[1],
            w8: UInt32(truncatingIfNeeded: counter), w9: UInt32(truncatingIfNeeded: counter >> 32)
        )
        var s = initial
        s.twentyRounds()
        return s.words &+ initial.words
    }

    /// XORs `data` in place with the keystream, starting at block `counter`.
    package static func xor(
        _ data: UnsafeMutableRawBufferPointer,
        key: SecretBoxKey,
        nonce: SIMD2<UInt32>,
        startingAtBlock counter: UInt64 = 0
    ) {
        var counter = counter
        var offset = 0
        while data.count - offset >= blockLength {
            let stream = block(key: key, nonce: nonce, counter: counter)
            for i in 0..<16 {
                let at = offset + 4 * i
                storeLittleEndian32(loadLittleEndian32(data, at) ^ stream[i], data, at)
            }
            offset += blockLength
            counter &+= 1
        }
        let remaining = data.count - offset
        if remaining > 0 {
            let stream = block(key: key, nonce: nonce, counter: counter)
            for k in 0..<remaining {
                let word = stream[k >> 2]
                data[offset + k] ^= UInt8(truncatingIfNeeded: word >> UInt32((k & 3) << 3))
            }
        }
    }
}

/// XSalsa20: Salsa20 with a 192-bit nonce. The subkey is
/// HSalsa20(key, nonce[0..<16]); the stream is Salsa20(subkey, nonce[16..<24]).
package struct XSalsa20: Sendable {
    private let subkey: SecretBoxKey
    private let salsaNonce: SIMD2<UInt32>

    package init(key: SecretBoxKey, nonce: SecretBoxNonce) {
        self.subkey = HSalsa20.derive(key: key, input: nonce.head)
        self.salsaNonce = nonce.tail
    }

    /// Keystream block number `counter` as 16 little-endian words.
    package func block(_ counter: UInt64) -> SIMD16<UInt32> {
        Salsa20.block(key: subkey, nonce: salsaNonce, counter: counter)
    }

    /// XORs `data` in place with the keystream, starting at block `counter`.
    package func xor(_ data: UnsafeMutableRawBufferPointer, startingAtBlock counter: UInt64 = 0) {
        Salsa20.xor(data, key: subkey, nonce: salsaNonce, startingAtBlock: counter)
    }
}
