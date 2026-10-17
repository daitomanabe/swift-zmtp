// swift-tools-version: 6.0
import PackageDescription

// Separate package so the root package stays free of benchmark code.
let package = Package(
    name: "zmq-bench",
    platforms: [
        .macOS(.v15),
    ],
    dependencies: [
        .package(path: ".."),
    ],
    targets: [
        .executableTarget(
            name: "zmq-bench",
            dependencies: [
                .product(name: "ZMQ", package: "libzmq-swift"),
            ]
        ),
    ]
)
