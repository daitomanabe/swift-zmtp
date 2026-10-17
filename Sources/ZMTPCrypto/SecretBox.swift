// NaCl crypto_secretbox: XSalsa20-Poly1305.
//
// D. J. Bernstein, T. Lange, P. Schwabe, "The security impact of a new
// cryptographic library" (NaCl, 2012), and D. J. Bernstein, "Cryptography in
// NaCl" (secretbox construction):
//
//   - keystream = XSalsa20(key, 24-byte nonce)
//   - keystream bytes 0..<32 are the one-time Poly1305 key (r, s)
//   - the message is XORed with keystream bytes 32... (so the first 32 message
//     bytes use the rest of block 0, and byte 32 starts at the beginning of block 1)
//   - the tag is Poly1305 over the ciphertext (no associated data, no length block)
//
// Layout: the "easy" layout of libsodium, which is also what CurveZMQ's Box means:
// output = 16-byte tag, then the ciphertext (same length as the message).
// This is not NaCl's original zero-padded buffer form.
//
// Shapes for callers that must not copy large payloads:
//   - `sealInPlace` / `openInPlace` work on one buffer. For a sealed box, the caller
//     reserves 16 bytes in front of the plaintext in a buffer it already owns (for
//     example a NIO ByteBuffer's writable bytes), and the cipher writes the tag there.
//   - `seal(_:into:)` / `open(_:into:)` read from one buffer and write to another.
//   - the `[UInt8]` forms are conveniences for small handshake messages and tests.

package enum SecretBox {
    package static let keyLength = 32
    package static let nonceLength = 24
    package static let macLength = 16

    // MARK: In place

    /// Seals in place.
    ///
    /// On entry `buffer` holds 16 bytes of unused space followed by the plaintext
    /// (`buffer.count == 16 + message length`). On return it holds the tag followed
    /// by the ciphertext, i.e. the finished box.
    package static func sealInPlace(_ buffer: UnsafeMutableRawBufferPointer, nonce: SecretBoxNonce, key: SecretBoxKey) {
        precondition(buffer.count >= macLength, "a secretbox buffer needs room for the 16-byte tag")
        let stream = XSalsa20(key: key, nonce: nonce)
        let block0 = stream.block(0)
        let body = UnsafeMutableRawBufferPointer(rebasing: buffer[macLength...])
        applyKeystream(to: body, block0: block0, stream: stream)
        let tag = Poly1305.authenticate(
            UnsafeRawBufferPointer(body),
            r: SIMD4(block0[0], block0[1], block0[2], block0[3]),
            s: SIMD4(block0[4], block0[5], block0[6], block0[7])
        )
        tag.store(to: UnsafeMutableRawBufferPointer(rebasing: buffer[..<macLength]))
    }

    /// Opens in place.
    ///
    /// On entry `buffer` holds a box (tag, then ciphertext). If the tag is valid,
    /// the bytes after the first 16 are replaced by the plaintext and the result is
    /// true. If not, nothing is decrypted, `buffer` is unchanged and the result is
    /// false. The tag is compared in constant time before any decryption.
    package static func openInPlace(_ buffer: UnsafeMutableRawBufferPointer, nonce: SecretBoxNonce, key: SecretBoxKey) -> Bool {
        guard buffer.count >= macLength else { return false }
        let stream = XSalsa20(key: key, nonce: nonce)
        let block0 = stream.block(0)
        let body = UnsafeMutableRawBufferPointer(rebasing: buffer[macLength...])
        let raw = UnsafeRawBufferPointer(buffer)
        guard verify(
            tag: UnsafeRawBufferPointer(rebasing: raw[..<macLength]),
            ciphertext: UnsafeRawBufferPointer(body),
            block0: block0
        ) else { return false }
        applyKeystream(to: body, block0: block0, stream: stream)
        return true
    }

    // MARK: Separate buffers

    /// Seals `message` into `output` (`output.count == message.count + 16`).
    /// The buffers may overlap (a plaintext already at `output + 16` is not copied).
    package static func seal(
        _ message: UnsafeRawBufferPointer,
        nonce: SecretBoxNonce,
        key: SecretBoxKey,
        into output: UnsafeMutableRawBufferPointer
    ) {
        precondition(output.count == message.count + macLength, "output must be message length + 16")
        let body = UnsafeMutableRawBufferPointer(rebasing: output[macLength...])
        if UnsafeRawBufferPointer(body).baseAddress != message.baseAddress {
            body.copyMemory(from: message)
        }
        sealInPlace(output, nonce: nonce, key: key)
    }

    /// Opens `box` (tag, then ciphertext) into `output` (`output.count == box.count - 16`).
    /// Returns false, leaving `output` untouched, if the tag does not verify or
    /// `box` is shorter than 16 bytes. Plaintext is never written for a forged box.
    package static func open(
        _ box: UnsafeRawBufferPointer,
        nonce: SecretBoxNonce,
        key: SecretBoxKey,
        into output: UnsafeMutableRawBufferPointer
    ) -> Bool {
        guard box.count >= macLength else { return false }
        precondition(output.count == box.count - macLength, "output must be box length - 16")
        let stream = XSalsa20(key: key, nonce: nonce)
        let block0 = stream.block(0)
        let ciphertext = UnsafeRawBufferPointer(rebasing: box[macLength...])
        guard verify(
            tag: UnsafeRawBufferPointer(rebasing: box[..<macLength]),
            ciphertext: ciphertext,
            block0: block0
        ) else { return false }
        if UnsafeRawBufferPointer(output).baseAddress != ciphertext.baseAddress {
            output.copyMemory(from: ciphertext)
        }
        applyKeystream(to: output, block0: block0, stream: stream)
        return true
    }

    // MARK: Arrays

    /// The box for `message`: 16-byte tag, then the ciphertext.
    package static func seal(_ message: [UInt8], nonce: SecretBoxNonce, key: SecretBoxKey) -> [UInt8] {
        var out = [UInt8](repeating: 0, count: macLength + message.count)
        message.withUnsafeBytes { m in
            out.withUnsafeMutableBytes { seal(m, nonce: nonce, key: key, into: $0) }
        }
        return out
    }

    /// The plaintext of `box`, or nil if the tag does not verify (or `box` is too short).
    package static func open(_ box: [UInt8], nonce: SecretBoxNonce, key: SecretBoxKey) -> [UInt8]? {
        guard box.count >= macLength else { return nil }
        var out = [UInt8](repeating: 0, count: box.count - macLength)
        let ok = box.withUnsafeBytes { b in
            out.withUnsafeMutableBytes { open(b, nonce: nonce, key: key, into: $0) }
        }
        return ok ? out : nil
    }

    // MARK: Internals

    /// XORs `body` with the secretbox keystream: bytes 32... of the XSalsa20 stream.
    /// The first min(32, count) bytes use the second half of block 0 (words 8...15);
    /// the rest continues from block 1.
    private static func applyKeystream(to body: UnsafeMutableRawBufferPointer, block0: SIMD16<UInt32>, stream: XSalsa20) {
        let head = min(32, body.count)
        for k in 0..<head {
            let word = block0[8 + (k >> 2)]
            body[k] ^= UInt8(truncatingIfNeeded: word >> UInt32((k & 3) << 3))
        }
        if body.count > 32 {
            stream.xor(UnsafeMutableRawBufferPointer(rebasing: body[32...]), startingAtBlock: 1)
        }
    }

    /// Recomputes the Poly1305 tag over `ciphertext` with the one-time key taken
    /// from keystream block 0, and compares it with `tag` without an early exit.
    private static func verify(tag: UnsafeRawBufferPointer, ciphertext: UnsafeRawBufferPointer, block0: SIMD16<UInt32>) -> Bool {
        guard let expected = Poly1305Tag(bytes: tag) else { return false }
        let computed = Poly1305.authenticate(
            ciphertext,
            r: SIMD4(block0[0], block0[1], block0[2], block0[3]),
            s: SIMD4(block0[4], block0[5], block0[6], block0[7])
        )
        return computed.isEqual(to: expected)
    }
}
