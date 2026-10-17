#if os(macOS)
import Foundation

/// One event line printed by `Tools/interop/peer.py`.
struct PeerEvent: Sendable, CustomStringConvertible {
    let name: String
    /// The `frames` of the event, decoded from hex. Empty for events without frames.
    let frames: [[UInt8]]
    let endpoints: [String]
    /// The raw line, for failure messages.
    let line: String

    var description: String { line }
}

struct PeerError: Error, CustomStringConvertible {
    let description: String
    init(_ description: String) { self.description = description }
}

func hexString(_ bytes: some Sequence<UInt8>) -> String {
    let digits = Array("0123456789abcdef")
    return bytes.reduce(into: "") {
        $0.append(digits[Int($1 >> 4)])
        $0.append(digits[Int($1 & 0x0F)])
    }
}

private func decodeHex(_ text: String) -> [UInt8] {
    var bytes: [UInt8] = []
    var index = text.startIndex
    while index < text.endIndex {
        let next = text.index(index, offsetBy: 2)
        bytes.append(UInt8(text[index..<next], radix: 16) ?? 0)
        index = next
    }
    return bytes
}

/// A message in the form the peer takes on its command line: frames in hex, joined by commas.
func peerMessage(_ frames: [[UInt8]]) -> String {
    frames.map { hexString($0) }.joined(separator: ",")
}

/// Runs `uv run Tools/interop/peer.py …` and collects its JSON lines.
///
/// `@unchecked Sendable`: every mutable field is guarded by `lock`. `Process` itself is only started
/// in `init` and stopped through `terminate()`.
final class PeerProcess: @unchecked Sendable {
    /// Root of the repository, from the location of this file (Tests/InteropTests/PeerProcess.swift).
    static let repositoryRoot = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()

    private let process = Process()
    private let lock = NSLock()
    private var lineBuffer = Data()
    private var storedEvents: [PeerEvent] = []
    private var storedStderr = Data()
    private var stdoutClosed = false
    private var exitStatus: Int32?

    /// Starts the peer. The peer stops itself after `--timeout` seconds, so even a leaked process
    /// does not live long; `terminate()` ends it at once.
    init(arguments: [String], peerTimeout: Int = 20) throws {
        let script = Self.repositoryRoot.appendingPathComponent("Tools/interop/peer.py").path
        process.executableURL = try Self.locateUV()
        process.arguments = ["run", "--quiet", script, "--timeout", String(peerTimeout)] + arguments
        process.currentDirectoryURL = Self.repositoryRoot

        let stdout = Pipe()
        let stderr = Pipe()
        process.standardOutput = stdout
        process.standardError = stderr
        process.standardInput = FileHandle.nullDevice

        stdout.fileHandleForReading.readabilityHandler = { [self] handle in
            let data = handle.availableData
            if data.isEmpty {
                handle.readabilityHandler = nil
                lock.withLock { stdoutClosed = true }
            } else {
                append(stdout: data)
            }
        }
        stderr.fileHandleForReading.readabilityHandler = { [self] handle in
            let data = handle.availableData
            if data.isEmpty {
                handle.readabilityHandler = nil
            } else {
                lock.withLock { storedStderr.append(data) }
            }
        }
        process.terminationHandler = { [self] finished in
            lock.withLock { exitStatus = finished.terminationStatus }
        }
        try process.run()
    }

    private static func locateUV() throws -> URL {
        let path = ProcessInfo.processInfo.environment["PATH"] ?? ""
        var candidates = path.split(separator: ":").map { URL(fileURLWithPath: String($0)).appendingPathComponent("uv") }
        candidates.append(URL(fileURLWithPath: "/opt/homebrew/bin/uv"))
        if let found = candidates.first(where: { FileManager.default.isExecutableFile(atPath: $0.path) }) {
            return found
        }
        throw PeerError("uv not found on PATH or at /opt/homebrew/bin/uv")
    }

    private func append(stdout data: Data) {
        lock.withLock {
            lineBuffer.append(data)
            while let newline = lineBuffer.firstIndex(of: 0x0A) {
                let lineData = lineBuffer[lineBuffer.startIndex..<newline]
                lineBuffer.removeSubrange(lineBuffer.startIndex...newline)
                if let event = Self.parse(String(decoding: lineData, as: UTF8.self)) {
                    storedEvents.append(event)
                }
            }
        }
    }

    private static func parse(_ line: String) -> PeerEvent? {
        guard let object = try? JSONSerialization.jsonObject(with: Data(line.utf8)) as? [String: Any],
            let name = object["event"] as? String
        else { return nil }
        return PeerEvent(
            name: name,
            frames: (object["frames"] as? [String] ?? []).map(decodeHex),
            endpoints: object["endpoints"] as? [String] ?? [],
            line: line)
    }

    // MARK: Reading what the peer printed

    var events: [PeerEvent] { lock.withLock { storedEvents } }

    /// Frames of every `recv` event, one entry per message.
    var received: [[[UInt8]]] { events.filter { $0.name == "recv" }.map(\.frames) }

    /// Frames of every `subscription` event (XPUB peers).
    var subscriptions: [[[UInt8]]] { events.filter { $0.name == "subscription" }.map(\.frames) }

    /// Everything the peer printed, for failure messages.
    var log: String {
        let stderr = lock.withLock { String(decoding: storedStderr, as: UTF8.self) }
        let lines = events.map(\.line).joined(separator: "\n")
        return "peer stdout:\n\(lines)\npeer stderr:\n\(stderr)"
    }

    // MARK: Waiting

    /// Waits until the peer has printed `count` events called `name` and returns them. Throws when the
    /// peer reports `error` or `timeout`, exits first, or the time is up.
    @discardableResult
    func waitForEvent(_ name: String, count: Int = 1, timeout: Duration = .seconds(20)) async throws -> [PeerEvent] {
        let deadline = ContinuousClock.now + timeout
        while true {
            let (snapshot, exited) = lock.withLock { (storedEvents, exitStatus != nil && stdoutClosed) }
            let matching = snapshot.filter { $0.name == name }
            if matching.count >= count { return matching }
            if let failure = snapshot.first(where: { $0.name == "error" || $0.name == "timeout" }) {
                throw PeerError("peer reported \(failure.line) while waiting for \(count) '\(name)' event(s)\n\(log)")
            }
            if exited { throw PeerError("peer exited before \(count) '\(name)' event(s)\n\(log)") }
            if ContinuousClock.now >= deadline {
                terminate()
                throw PeerError("no \(count) '\(name)' event(s) within \(timeout)\n\(log)")
            }
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    /// Waits for the peer to exit and checks that it ended with status 0 and a `done` event.
    func finish(timeout: Duration = .seconds(30)) async throws {
        let deadline = ContinuousClock.now + timeout
        while true {
            let (status, closed) = lock.withLock { (exitStatus, stdoutClosed) }
            if let status, closed {
                guard status == 0, events.last?.name == "done" else {
                    throw PeerError("peer exited with status \(status)\n\(log)")
                }
                return
            }
            if ContinuousClock.now >= deadline {
                terminate()
                throw PeerError("peer still running after \(timeout)\n\(log)")
            }
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    /// Stops the peer if it still runs. `uv` passes SIGTERM on to Python; SIGKILL follows if it lingers.
    func terminate() {
        guard process.isRunning else { return }
        process.terminate()
        DispatchQueue.global().asyncAfter(deadline: .now() + 2) { [self] in
            if process.isRunning { kill(process.processIdentifier, SIGKILL) }
        }
    }
}
#endif
