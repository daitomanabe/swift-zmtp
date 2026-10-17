import Foundation
import Testing
import ZMQ

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

/// A path in the temporary directory for a socket file of a test.
private func temporaryPath() -> String {
    NSTemporaryDirectory() + "zmq-test-" + UUID().uuidString.prefix(8)
}

enum StaleFile: String, CaseIterable, Sendable {
    case regularFile, socket
}

/// Leaves a file at `path` the way a crashed run would.
private func leaveStaleFile(_ kind: StaleFile, at path: String) throws {
    switch kind {
    case .regularFile:
        guard FileManager.default.createFile(atPath: path, contents: Data("stale".utf8)) else {
            throw CocoaError(.fileWriteUnknown)
        }
    case .socket:
        let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw CocoaError(.fileWriteUnknown) }
        defer { close(descriptor) }
        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        withUnsafeMutableBytes(of: &address.sun_path) { buffer in
            for (index, byte) in path.utf8CString.enumerated() { buffer[index] = UInt8(bitPattern: byte) }
        }
        let size = socklen_t(MemoryLayout<sockaddr_un>.size)
        let result = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(descriptor, $0, size) }
        }
        guard result == 0 else { throw CocoaError(.fileWriteUnknown) }
    }
}

@Suite struct IPCTests {
    @Test(.timeLimit(.minutes(1)))
    func pushToPullOverAWildcardEndpoint() async throws {
        let pull = PullSocket()
        let endpoint = try await pull.bind("ipc://*")
        #expect(endpoint.hasPrefix("ipc://"))
        #expect(!endpoint.hasSuffix("*"))
        let path = String(endpoint.dropFirst("ipc://".count))
        #expect(FileManager.default.fileExists(atPath: path))

        let push = PushSocket()
        try await push.connect(endpoint)
        let message = Message(["a", "", "ccc"])
        try await within { try await push.send(message) }
        #expect(try await within { try await pull.receive() } == message)

        await push.close()
        await pull.close()
        #expect(!FileManager.default.fileExists(atPath: path))
    }

    @Test(.timeLimit(.minutes(1)), arguments: StaleFile.allCases)
    func bindReplacesAStaleFileAndCloseRemovesIt(_ kind: StaleFile) async throws {
        let path = temporaryPath()
        try leaveStaleFile(kind, at: path)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let pull = PullSocket()
        let endpoint = try await pull.bind("ipc://" + path)
        #expect(endpoint == "ipc://" + path)
        let push = PushSocket()
        try await push.connect(endpoint)
        try await within { try await push.send("through the new socket") }
        #expect(try await within { try await pull.receive() } == "through the new socket")

        await push.close()
        await pull.close()
        #expect(!FileManager.default.fileExists(atPath: path))
    }

    @Test(.timeLimit(.minutes(1)))
    func overlongPathIsAnInvalidEndpoint() async throws {
        let endpoint = "ipc://" + String(repeating: "x", count: 200)
        let pull = PullSocket()
        await #expect {
            try await pull.bind(endpoint)
        } throws: { error in
            if case ZMQError.invalidEndpoint = error { return true }
            return false
        }
        let push = PushSocket()
        await #expect {
            try await push.connect(endpoint)
        } throws: { error in
            if case ZMQError.invalidEndpoint = error { return true }
            return false
        }
        await push.close()
        await pull.close()
    }
}
