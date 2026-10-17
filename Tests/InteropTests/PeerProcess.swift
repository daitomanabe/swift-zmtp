#if os(macOS)
import Foundation

/// One event line printed by `Tools/interop/peer.py`.
struct PeerEvent: Sendable, CustomStringConvertible {
    let name: String
    /// The `frames` of the event, decoded from hex. Empty for events without frames.
    let frames: [[UInt8]]
    let endpoints: [String]
    /// The `pid` of the event (the `ready` line carries the Python process id).
    let pid: Int32?
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
    private var suspended = false
    private var watchdog: Process?

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
            pid: (object["pid"] as? Int).map { Int32($0) },
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

    // MARK: Stopping and continuing

    /// The id of the Python process, from the `ready` event. `uv run` may run Python as a child, so
    /// `process.processIdentifier` is not always the process that has to be stopped.
    var pythonPID: Int32? { events.first { $0.name == "ready" }?.pid }

    /// Stops the Python process with SIGSTOP, as a frozen or unreachable peer. A watchdog process
    /// continues and ends it if this test process dies before `resume()` or `terminate()` (a stopped
    /// process would otherwise stay behind forever; its own `--timeout` cannot fire while it is stopped).
    func suspend() throws {
        guard let pid = pythonPID else { throw PeerError("peer printed no pid\n\(log)") }
        let guardProcess = Process()
        guardProcess.executableURL = URL(fileURLWithPath: "/bin/sh")
        guardProcess.arguments = [
            "-c",
            // SIGTERM (from `resume()` / `terminate()`) ends the watchdog together with its sleep.
            "trap 'kill $s 2>/dev/null; exit 0' TERM; sleep 25 & s=$!; wait $s; "
                + "if ps -p \"$1\" -o command= | grep -q '[p]eer.py'; then kill -CONT \"$1\"; kill -TERM \"$1\"; fi",
            "sh", String(pid),
        ]
        guardProcess.standardInput = FileHandle.nullDevice
        guardProcess.standardOutput = FileHandle.nullDevice
        guardProcess.standardError = FileHandle.nullDevice
        try guardProcess.run()
        lock.withLock {
            watchdog = guardProcess
            suspended = true
        }
        guard kill(pid, SIGSTOP) == 0 else {
            resume()
            throw PeerError("SIGSTOP to \(pid) failed (errno \(errno))")
        }
    }

    /// Continues a peer stopped by `suspend()`. Harmless when it is not stopped.
    func resume() {
        let (pid, wasSuspended, guardProcess) = lock.withLock {
            let state = (storedEvents.first { $0.name == "ready" }?.pid, suspended, watchdog)
            suspended = false
            watchdog = nil
            return state
        }
        if wasSuspended, let pid { kill(pid, SIGCONT) }
        guardProcess?.terminate()
    }

    /// Stops the peer if it still runs; a stopped peer is continued first so that the signal is acted on.
    /// `uv` passes SIGTERM on to Python; SIGKILL follows if it lingers.
    func terminate() {
        resume()
        guard process.isRunning else { return }
        process.terminate()
        let python = pythonPID
        DispatchQueue.global().asyncAfter(deadline: .now() + 2) { [self] in
            if process.isRunning {
                kill(process.processIdentifier, SIGKILL)
                if let python { kill(python, SIGKILL) }
            }
        }
    }
}
#endif
