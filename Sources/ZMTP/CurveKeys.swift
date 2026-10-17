import ZMTPCrypto

/// Key helpers for the ZMQ module, which does not see `ZMTPCrypto`: key pairs, public keys from secret
/// keys, and the Z85 text form (32/Z85) of 32-byte keys. Everything is raw bytes in and out.
package enum CurveKeys {
    /// A Curve25519 key is 32 bytes, 40 characters in Z85.
    package static let keyLength = 32
    package static let z85Length = 40

    /// A new random key pair (public key, secret key).
    package static func generate() -> (publicKey: [UInt8], secretKey: [UInt8]) {
        let pair = X25519KeyPair.generate()
        return (pair.publicKey, pair.secretKey)
    }

    /// The public key that belongs to `secretKey`; nil unless the secret key has 32 bytes.
    package static func publicKey(forSecretKey secretKey: [UInt8]) -> [UInt8]? {
        X25519.publicKey(secretKey: secretKey)
    }

    /// The Z85 text of `bytes` (empty for a length that is not a multiple of 4, which a key never has).
    package static func z85(_ bytes: [UInt8]) -> String {
        Z85.encode(bytes) ?? ""
    }

    /// The bytes of a Z85 text; nil for a length that is not a multiple of 5 or for characters outside
    /// the Z85 alphabet.
    package static func bytes(z85 text: String) -> [UInt8]? {
        Z85.decode(text)
    }
}
