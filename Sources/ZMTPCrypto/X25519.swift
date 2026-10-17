// X25519 (RFC 7748) and the NaCl box key precomputation.
//
// The curve arithmetic is not written here: Apple platforms use CryptoKit's
// `Curve25519.KeyAgreement`, Linux uses swift-crypto's `Crypto` module (same API,
// BoringSSL underneath). Raw 32-byte keys go in and out, as in CurveZMQ.

#if canImport(CryptoKit)
import CryptoKit
#else
import Crypto
#endif

/// A Curve25519 key pair as raw 32-byte values.
package struct X25519KeyPair: Sendable {
    package let publicKey: [UInt8]
    package let secretKey: [UInt8]

    /// A new random key pair: 32 random bytes as the secret key (as `crypto_box_keypair`
    /// does; X25519 clamps internally), and the public key derived from it.
    package static func generate() -> X25519KeyPair {
        let secretKey = SecureRandom.bytes(X25519.keyLength)
        guard let pair = X25519KeyPair(secretKey: secretKey) else {
            preconditionFailure("X25519 rejected a 32-byte secret key")
        }
        return pair
    }

    /// The pair for an existing secret key. Returns nil unless it has 32 bytes.
    package init?(secretKey: [UInt8]) {
        guard let publicKey = X25519.publicKey(secretKey: secretKey) else { return nil }
        self.publicKey = publicKey
        self.secretKey = secretKey
    }
}

package enum X25519 {
    package static let keyLength = 32

    /// `crypto_scalarmult_base`: the public key for a secret key.
    /// Returns nil unless `secretKey` has 32 bytes.
    package static func publicKey(secretKey: [UInt8]) -> [UInt8]? {
        guard secretKey.count == keyLength,
            let key = try? Curve25519.KeyAgreement.PrivateKey(rawRepresentation: secretKey)
        else { return nil }
        return Array(key.publicKey.rawRepresentation)
    }

    /// `crypto_scalarmult`: the raw 32-byte shared secret (no hashing).
    /// Returns nil for keys of the wrong size, or when the result is all zeros
    /// (a low-order peer key), as libsodium refuses it too.
    package static func sharedSecret(secretKey: [UInt8], publicKey: [UInt8]) -> [UInt8]? {
        guard secretKey.count == keyLength, publicKey.count == keyLength,
            let privateKey = try? Curve25519.KeyAgreement.PrivateKey(rawRepresentation: secretKey),
            let peer = try? Curve25519.KeyAgreement.PublicKey(rawRepresentation: publicKey),
            let secret = try? privateKey.sharedSecretFromKeyAgreement(with: peer)
        else { return nil }
        let bytes = secret.withUnsafeBytes { Array($0) }
        return ConstantTime.isZero(bytes) ? nil : bytes
    }
}

/// NaCl `crypto_box`: X25519 key agreement, then HSalsa20, then a secretbox.
///
/// `crypto_box_beforenm(pk, sk)` is `HSalsa20(key: X25519(sk, pk), input: 16 zero bytes)`;
/// with that key, `crypto_box_afternm` / `crypto_box_open_afternm` are exactly
/// `SecretBox.seal` / `SecretBox.open`. CURVE should precompute once per key pair
/// and then call `SecretBox` directly, so a message costs no X25519.
package enum CryptoBox {
    /// `crypto_box_beforenm`. Returns nil if either key is not 32 bytes, or the
    /// shared secret is all zeros.
    package static func precompute(publicKey: [UInt8], secretKey: [UInt8]) -> SecretBoxKey? {
        guard let shared = X25519.sharedSecret(secretKey: secretKey, publicKey: publicKey),
            let key = SecretBoxKey(shared)
        else { return nil }
        return HSalsa20.derive(key: key, input: SIMD4<UInt32>(repeating: 0))
    }

    /// `crypto_box_easy`: the box (16-byte tag, then ciphertext) from the sender's
    /// secret key to the recipient's public key. Nil only if the keys are unusable.
    /// For repeated use, `precompute` once and call `SecretBox.seal`.
    package static func seal(
        _ message: [UInt8],
        nonce: SecretBoxNonce,
        recipientPublicKey: [UInt8],
        senderSecretKey: [UInt8]
    ) -> [UInt8]? {
        guard let key = precompute(publicKey: recipientPublicKey, secretKey: senderSecretKey) else { return nil }
        return SecretBox.seal(message, nonce: nonce, key: key)
    }

    /// `crypto_box_open_easy`: the plaintext, or nil if the keys are unusable or the tag does not verify.
    package static func open(
        _ box: [UInt8],
        nonce: SecretBoxNonce,
        senderPublicKey: [UInt8],
        recipientSecretKey: [UInt8]
    ) -> [UInt8]? {
        guard let key = precompute(publicKey: senderPublicKey, secretKey: recipientSecretKey) else { return nil }
        return SecretBox.open(box, nonce: nonce, key: key)
    }
}
