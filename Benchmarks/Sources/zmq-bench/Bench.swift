import Foundation
import ZMQ

/// `zmq-bench thr <count> <size>` and `zmq-bench lat <count> <size>`: the same two tests as
/// `libzmq_bench.c`, over `tcp://127.0.0.1:*`, in one process.
@main
enum Bench {
    static func main() async {
        let args = CommandLine.arguments
        guard args.count == 4, args[1] == "thr" || args[1] == "lat",
            let count = Int(args[2]), let size = Int(args[3]), count >= 2, size >= 1
        else {
            printError("usage: zmq-bench thr|lat <count >= 2> <size >= 1>")
            exit(64)
        }

        // A hung run must not hang the caller.
        Task.detached {
            try? await Task.sleep(for: .seconds(120))
            printError("error: watchdog: no result after 120 s")
            exit(2)
        }

        do {
            if args[1] == "thr" {
                try await throughput(count: count, size: size)
            } else {
                try await latency(count: count, size: size)
            }
            exit(0)
        } catch {
            printError("error: \(error)")
            exit(1)
        }
    }

    // MARK: - thr

    /// PULL binds, PUSH connects. PUSH sends `count` messages while PULL receives them; the time runs
    /// from the first receive to the last, so `count - 1` messages are timed.
    static func throughput(count: Int, size: Int) async throws {
        let pull = PullSocket()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")
        let push = PushSocket()
        try await push.connect(endpoint)
        let payload = Message(Frame([UInt8](repeating: 0x61, count: size)))

        let elapsedNs = try await withThrowingTaskGroup(of: UInt64?.self) { group in
            group.addTask {
                for _ in 0..<count { try await push.send(payload) }
                return nil
            }
            group.addTask {
                try check(await pull.receive(), size: size)
                let start = nowNs()
                for _ in 1..<count { try check(await pull.receive(), size: size) }
                return nowNs() - start
            }
            var elapsed: UInt64 = 0
            for try await result in group {
                if let result { elapsed = result }
            }
            return elapsed
        }

        await push.close()
        await pull.close()

        let seconds = Double(elapsedNs) / 1e9
        let msgPerSec = Double(count - 1) / seconds
        let mbPerSec = msgPerSec * Double(size) / 1e6
        print(String(format: "swift   thr count=%d size=%d: %.0f msg/s, %.2f MB/s (%.3f s)", count, size, msgPerSec, mbPerSec, seconds))
        print(String(format: "{\"impl\":\"swift\",\"test\":\"thr\",\"count\":%d,\"size\":%d,\"msgPerSec\":%.0f,\"mbPerSec\":%.2f}", count, size, msgPerSec, mbPerSec))
    }

    // MARK: - lat

    /// REP echoes, REQ connects. After a warm-up, `count` round trips are timed one by one.
    static func latency(count: Int, size: Int) async throws {
        let warmup = 500
        let rep = RepSocket()
        let endpoint = try await rep.bind("tcp://127.0.0.1:*")
        let req = ReqSocket()
        try await req.connect(endpoint)
        let payload = Message(Frame([UInt8](repeating: 0x61, count: size)))

        let samples = try await withThrowingTaskGroup(of: [UInt64].self) { group in
            group.addTask {
                for _ in 0..<(warmup + count) {
                    try await rep.reply(rep.receive())
                }
                return []
            }
            group.addTask {
                var rtts = [UInt64]()
                rtts.reserveCapacity(count)
                for i in 0..<(warmup + count) {
                    let start = nowNs()
                    let reply = try await req.request(payload)
                    let end = nowNs()
                    try check(reply, size: size)
                    if i >= warmup { rtts.append(end - start) }
                }
                return rtts
            }
            var rtts = [UInt64]()
            for try await result in group where !result.isEmpty { rtts = result }
            return rtts
        }

        await req.close()
        await rep.close()

        let sorted = samples.sorted()
        let mean = Double(sorted.reduce(0, +)) / Double(sorted.count) / 1e3
        let p50 = Double(sorted[percentileIndex(0.50, of: sorted.count)]) / 1e3
        let p99 = Double(sorted[percentileIndex(0.99, of: sorted.count)]) / 1e3
        print(String(format: "swift   lat count=%d size=%d: mean %.2f us, p50 %.2f us, p99 %.2f us", count, size, mean, p50, p99))
        print(String(format: "{\"impl\":\"swift\",\"test\":\"lat\",\"count\":%d,\"size\":%d,\"meanUs\":%.2f,\"p50Us\":%.2f,\"p99Us\":%.2f}", count, size, mean, p50, p99))
    }

    /// Nearest-rank percentile: the smallest sample with at least `p` of the samples at or below it.
    static func percentileIndex(_ p: Double, of n: Int) -> Int {
        max(0, Int((p * Double(n)).rounded(.up)) - 1)
    }

    // MARK: - helpers

    struct BadMessage: Error { var detail: String }

    static func check(_ message: Message, size: Int) throws {
        guard message.frames.count == 1, message.frames[0].count == size else {
            throw BadMessage(detail: "expected 1 frame of \(size) bytes, got \(message.frames.count) frames, first \(message.frames.first?.count ?? -1) bytes")
        }
    }

    static func nowNs() -> UInt64 { clock_gettime_nsec_np(CLOCK_MONOTONIC_RAW) }

    static func printError(_ text: String) {
        FileHandle.standardError.write(Data((text + "\n").utf8))
    }
}
