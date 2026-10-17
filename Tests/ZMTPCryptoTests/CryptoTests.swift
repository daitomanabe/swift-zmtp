import Foundation
import Testing
import ZMTPCrypto

// Known answers from libsodium (Fixtures/vectors.json, made by Tools/crypto/make_vectors.py),
// plus the published Poly1305 and Z85 examples.

private struct Vectors: Decodable {
    struct ScalarMultBase: Decodable { let secret: String; let `public`: String }
    struct ScalarMult: Decodable { let secret: String; let `public`: String; let shared: String }
    struct BoxBeforeNM: Decodable { let secret: String; let `public`: String; let key: String }
    struct SecretBoxEasy: Decodable { let key: String; let nonce: String; let plaintext: String; let box: String }
    struct BoxEasy: Decodable {
        let sender_secret: String, sender_public: String
        let recipient_secret: String, recipient_public: String
        let nonce: String, plaintext: String, box: String
    }

    let scalarmult_base: [ScalarMultBase]
    let scalarmult: [ScalarMult]
    let box_beforenm: [BoxBeforeNM]
    let secretbox_easy: [SecretBoxEasy]
    let box_easy: [BoxEasy]

    static func load() throws -> Vectors {
        guard let directory = Bundle.module.url(forResource: "Fixtures", withExtension: nil) else {
            throw CocoaError(.fileNoSuchFile)
        }
        let data = try Data(contentsOf: directory.appendingPathComponent("vectors.json"))
        return try JSONDecoder().decode(Vectors.self, from: data)
    }
}

private func bytes(hex: String) -> [UInt8] {
    var result: [UInt8] = []
    var index = hex.startIndex
    while index < hex.endIndex {
        let next = hex.index(index, offsetBy: 2)
        result.append(UInt8(hex[index..<next], radix: 16)!)
        index = next
    }
    return result
}

private func key(_ hex: String) throws -> SecretBoxKey {
    try #require(SecretBoxKey(bytes(hex: hex)))
}

private func nonce(_ hex: String) throws -> SecretBoxNonce {
    try #require(SecretBoxNonce(bytes(hex: hex)))
}

@Suite struct CryptoTests {
    @Test func scalarMultBase() throws {
        for vector in try Vectors.load().scalarmult_base {
            #expect(X25519.publicKey(secretKey: bytes(hex: vector.secret)) == bytes(hex: vector.public))
        }
    }

    @Test func scalarMult() throws {
        for vector in try Vectors.load().scalarmult {
            let shared = X25519.sharedSecret(secretKey: bytes(hex: vector.secret), publicKey: bytes(hex: vector.public))
            #expect(shared == bytes(hex: vector.shared))
        }
    }

    @Test func boxBeforeNM() throws {
        for vector in try Vectors.load().box_beforenm {
            let derived = CryptoBox.precompute(publicKey: bytes(hex: vector.public), secretKey: bytes(hex: vector.secret))
            #expect(derived?.bytes == bytes(hex: vector.key))
        }
    }

    @Test func secretBoxEasy() throws {
        for vector in try Vectors.load().secretbox_easy {
            let k = try key(vector.key), n = try nonce(vector.nonce)
            let plaintext = bytes(hex: vector.plaintext), box = bytes(hex: vector.box)

            #expect(SecretBox.seal(plaintext, nonce: n, key: k) == box, "seal, length \(plaintext.count)")
            #expect(SecretBox.open(box, nonce: n, key: k) == plaintext, "open, length \(plaintext.count)")

            // The in-place forms give the same bytes: 16 spare bytes, then the plaintext.
            var buffer = [UInt8](repeating: 0, count: 16) + plaintext
            buffer.withUnsafeMutableBytes { SecretBox.sealInPlace($0, nonce: n, key: k) }
            #expect(buffer == box, "sealInPlace, length \(plaintext.count)")
            let opened = buffer.withUnsafeMutableBytes { SecretBox.openInPlace($0, nonce: n, key: k) }
            #expect(opened && Array(buffer[16...]) == plaintext, "openInPlace, length \(plaintext.count)")
        }
    }

    @Test func boxEasy() throws {
        for vector in try Vectors.load().box_easy {
            let n = try nonce(vector.nonce)
            let plaintext = bytes(hex: vector.plaintext), box = bytes(hex: vector.box)
            let sealed = CryptoBox.seal(
                plaintext, nonce: n,
                recipientPublicKey: bytes(hex: vector.recipient_public),
                senderSecretKey: bytes(hex: vector.sender_secret)
            )
            #expect(sealed == box, "seal, length \(plaintext.count)")
            let opened = CryptoBox.open(
                box, nonce: n,
                senderPublicKey: bytes(hex: vector.sender_public),
                recipientSecretKey: bytes(hex: vector.recipient_secret)
            )
            #expect(opened == plaintext, "open, length \(plaintext.count)")
        }
    }

    /// RFC 8439 section 2.5.2.
    @Test func poly1305RFC8439() throws {
        let key = bytes(hex: "85d6be7857556d337f4452fe42d506a80103808afb0db2fd4abff6af4149f51b")
        let message = Array("Cryptographic Forum Research Group".utf8)
        let tag = try #require(Poly1305.authenticate(message, key: key))
        #expect(tag == bytes(hex: "a8061dc1305136c6c22b8baf0c0127a9"))
    }

    /// 32/Z85: the bytes 86 4F D2 6F B5 59 F7 5B are "HelloWorld".
    @Test func z85Example() throws {
        let raw: [UInt8] = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B]
        #expect(Z85.encode(raw) == "HelloWorld")
        #expect(Z85.decode("HelloWorld") == raw)
        // Lengths that are not multiples of 4 (encode) / 5 (decode) are rejected.
        #expect(Z85.encode([1, 2, 3]) == nil)
        #expect(Z85.decode("Hello") != nil)
        #expect(Z85.decode("HelloWorl") == nil)
    }

    @Test func z85RoundTripOfAKey() throws {
        let publicKey = X25519KeyPair.generate().publicKey
        let text = try #require(Z85.encode(publicKey))
        #expect(text.utf8.count == 40)
        #expect(Z85.decode(text) == publicKey)
    }

    @Test func tamperedBoxDoesNotOpen() throws {
        let k = try #require(SecretBoxKey(SecureRandom.bytes(32)))
        let n = try #require(SecretBoxNonce(SecureRandom.bytes(24)))
        let message = SecureRandom.bytes(100)
        let box = SecretBox.seal(message, nonce: n, key: k)
        #expect(SecretBox.open(box, nonce: n, key: k) == message)

        var badMAC = box
        badMAC[3] ^= 0x04  // one bit in the MAC
        #expect(SecretBox.open(badMAC, nonce: n, key: k) == nil)

        var badCiphertext = box
        badCiphertext[16 + 40] ^= 0x80  // one bit in the ciphertext
        #expect(SecretBox.open(badCiphertext, nonce: n, key: k) == nil)

        // A failed in-place open leaves the buffer as it was.
        var buffer = badCiphertext
        let ok = buffer.withUnsafeMutableBytes { SecretBox.openInPlace($0, nonce: n, key: k) }
        #expect(!ok && buffer == badCiphertext)
    }
}
