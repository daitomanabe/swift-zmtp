// swift-tools-version: 6.0
import PackageDescription

// Separate package so the root package stays free of benchmark code.
// A local package's identity is the name of its directory, which depends on how it was cloned
// (swift-zmtp, or anything else), so take it from the path instead of hard-coding it.
let rootPackage = Context.packageDirectory.split(separator: "/").dropLast().last.map(String.init) ?? "swift-zmtp"

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
                .product(name: "ZMQ", package: rootPackage),
            ]
        ),
    ]
)
