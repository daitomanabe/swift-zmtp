import ZMTP

// Keys for the CURVE security mechanism (25/ZMTP-CURVE, 26/CurveZMQ): Curve25519 keys of 32 bytes, written
// as 40 characters of Z85 (32/Z85) when a person has to handle them, like libzmq's `zmq_curve_keypair`.

private func decode(z85 text: String, what: String) throws -> [UInt8] {
    let length = text.utf8.count
    guard length == CurveKeys.z85Length else {
        throw ZMQError.invalidKey("a CURVE \(what) key is \(CurveKeys.z85Length) Z85 characters, not \(length)")
    }
    guard let bytes = CurveKeys.bytes(z85: text), bytes.count == CurveKeys.keyLength else {
        throw ZMQError.invalidKey("a CURVE \(what) key is not valid Z85")
    }
    return bytes
}

private func check(bytes: [UInt8], what: String) throws {
    guard bytes.count == CurveKeys.keyLength else {
        throw ZMQError.invalidKey("a CURVE \(what) key is \(CurveKeys.keyLength) bytes, not \(bytes.count)")
    }
}

/// A CURVE public key: what a server publishes so clients can check it is the server they mean, and what a
/// server's `authenticate` function receives to recognise a client.
public struct CurvePublicKey: Sendable, Hashable {
    /// The 32 raw bytes.
    public let bytes: [UInt8]

    /// - Throws: `ZMQError.invalidKey` unless `bytes` has 32 bytes.
    public init(bytes: [UInt8]) throws {
        try check(bytes: bytes, what: "public")
        self.bytes = bytes
    }

    /// - Throws: `ZMQError.invalidKey` unless `z85` is 40 characters of valid Z85.
    public init(z85: String) throws {
        self.bytes = try decode(z85: z85, what: "public")
    }

    /// For bytes that are known to be 32 long.
    init(validated bytes: [UInt8]) {
        self.bytes = bytes
    }

    /// The key as 40 characters of Z85.
    public var z85: String { CurveKeys.z85(bytes) }
}

/// A CURVE secret key. It never shows its bytes: `print` and `String(describing:)` give a fixed text.
public struct CurveSecretKey: Sendable, CustomStringConvertible, CustomDebugStringConvertible, CustomReflectable {
    let bytes: [UInt8]

    /// - Throws: `ZMQError.invalidKey` unless `bytes` has 32 bytes.
    public init(bytes: [UInt8]) throws {
        try check(bytes: bytes, what: "secret")
        self.bytes = bytes
    }

    /// - Throws: `ZMQError.invalidKey` unless `z85` is 40 characters of valid Z85.
    public init(z85: String) throws {
        self.bytes = try decode(z85: z85, what: "secret")
    }

    init(validated bytes: [UInt8]) {
        self.bytes = bytes
    }

    /// The key as 40 characters of Z85, to store it. Treat the text as secret.
    public var z85: String { CurveKeys.z85(bytes) }

    public var description: String { "CurveSecretKey(redacted)" }
    public var debugDescription: String { description }
    public var customMirror: Mirror { Mirror(self, children: []) }
}

/// A CURVE key pair: the secret key and the public key that belongs to it.
public struct CurveKeyPair: Sendable {
    public let publicKey: CurvePublicKey
    public let secretKey: CurveSecretKey

    /// A new random pair.
    public static func generate() -> CurveKeyPair {
        let pair = CurveKeys.generate()
        return CurveKeyPair(
            publicKey: CurvePublicKey(validated: pair.publicKey),
            secretKey: CurveSecretKey(validated: pair.secretKey))
    }

    /// The pair for an existing secret key: the public key is derived from it.
    public init(secretKey: CurveSecretKey) {
        // A CurveSecretKey always has 32 bytes, so the derivation does not fail.
        self.publicKey = CurvePublicKey(validated: CurveKeys.publicKey(forSecretKey: secretKey.bytes) ?? [])
        self.secretKey = secretKey
    }

    private init(publicKey: CurvePublicKey, secretKey: CurveSecretKey) {
        self.publicKey = publicKey
        self.secretKey = secretKey
    }
}
