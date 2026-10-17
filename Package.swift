// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "swift-zmtp",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
    ],
    products: [
        .library(name: "ZMQ", targets: ["ZMQ"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.104.0"),
        // X25519 on Linux only; Apple platforms use CryptoKit (see ZMTPCrypto).
        .package(url: "https://github.com/apple/swift-crypto.git", "3.0.0"..<"6.0.0"),
    ],
    targets: [
        // ZMTP 3.x without any I/O: greeting, handshake, framing, commands, mechanisms.
        // NIOCore only for ByteBuffer, so frame bodies are slices, not copies.
        .target(
            name: "ZMTP",
            dependencies: [
                "ZMTPCrypto",
                .product(name: "NIOCore", package: "swift-nio"),
            ]
        ),
        // CURVE primitives (Salsa20, XSalsa20-Poly1305, NaCl box, X25519, Z85), no I/O, no NIO.
        // Not a product: `package` access, for the ZMTP target only.
        .target(
            name: "ZMTPCrypto",
            dependencies: [
                .product(name: "Crypto", package: "swift-crypto", condition: .when(platforms: [.linux])),
            ]
        ),
        // Public API: socket semantics and the transports, on top of SwiftNIO.
        .target(
            name: "ZMQ",
            dependencies: [
                "ZMTP",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ]
        ),
        .testTarget(
            name: "ZMQTests",
            dependencies: ["ZMQ"]
        ),
        // Talks to a real libzmq (pyzmq via Tools/interop/peer.py). Runs only with ZMQ_INTEROP=1.
        .testTarget(
            name: "InteropTests",
            dependencies: ["ZMQ"]
        ),
        .testTarget(
            name: "ZMTPCryptoTests",
            dependencies: ["ZMTPCrypto"],
            resources: [.copy("Fixtures")]
        ),
        .testTarget(
            name: "ZMTPTests",
            dependencies: ["ZMTP"],
            resources: [.copy("Fixtures")]
        ),
    ]
)
