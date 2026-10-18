// swift-tools-version: 6.0
import PackageDescription

// A separate package that uses ZMQ the way an application does, through a path dependency on the root package.
// A local package's identity is the name of its directory, which depends on how it was cloned
// (swift-zmtp, or anything else), so take it from the path instead of hard-coding it.
let rootPackage = Context.packageDirectory.split(separator: "/").dropLast().last.map(String.init) ?? "swift-zmtp"

let package = Package(
    name: "zmq-examples",
    platforms: [
        .macOS(.v15),
    ],
    dependencies: [
        .package(path: ".."),
    ],
    targets: [
        .executableTarget(
            name: "zmq-examples",
            dependencies: [
                .product(name: "ZMQ", package: rootPackage),
            ]
        ),
    ]
)
