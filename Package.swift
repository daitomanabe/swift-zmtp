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
    ],
    targets: [
        // ZMTP 3.x without any I/O: greeting, handshake, framing, commands, mechanisms.
        // NIOCore only for ByteBuffer, so frame bodies are slices, not copies.
        .target(
            name: "ZMTP",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
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
            name: "ZMTPTests",
            dependencies: ["ZMTP"],
            resources: [.copy("Fixtures")]
        ),
    ]
)
