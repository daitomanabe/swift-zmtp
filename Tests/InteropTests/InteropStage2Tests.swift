#if os(macOS)
import Foundation
import Testing
import ZMQ

// Stage 2 interop (docs/DESIGN.md 5.9): IPC, XPUB / XSUB, and heartbeats against libzmq 4.3.5 (pyzmq).
// Shares the helpers of InteropTests.swift (`interop`, `within`, `Role`, `Transport`, `PeerProcess`).

/// Collects the `SocketEvent`s of one `events()` stream so that a test can poll for them.
///
/// `@unchecked Sendable`: `stored` is guarded by `lock`; `collector` is set once in `init`.
final class EventLog: @unchecked Sendable {
    private let lock = NSLock()
    private var stored: [SocketEvent] = []
    private var collector: Task<Void, Never>?

    init(_ stream: AsyncStream<SocketEvent>) {
        collector = Task { [self] in
            for await event in stream { lock.withLock { stored.append(event) } }
        }
    }

    var all: [SocketEvent] { lock.withLock { stored } }

    func count(_ matches: (SocketEvent) -> Bool) -> Int { all.filter(matches).count }

    /// Waits until `condition` holds for the events so far; throws with the events when the time is up.
    func wait(
        _ description: String, timeout: Duration = .seconds(10), until condition: ([SocketEvent]) -> Bool
    ) async throws {
        let deadline = ContinuousClock.now + timeout
        while !condition(all) {
            if ContinuousClock.now >= deadline {
                throw PeerError("no \(description) within \(timeout); socket events: \(all)")
            }
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    func stop() { collector?.cancel() }
}

private func isDisconnected(_ event: SocketEvent) -> Bool {
    if case .disconnected = event { return true }
    return false
}

private func isHandshakeSucceeded(_ event: SocketEvent) -> Bool {
    if case .handshakeSucceeded = event { return true }
    return false
}

private func isConnectRetried(_ event: SocketEvent) -> Bool {
    if case .connectRetried = event { return true }
    return false
}

/// Collects what `XPubSocket.receive()` returns.
///
/// `@unchecked Sendable`: `stored` is guarded by `lock`; `collector` is set once in `init`.
final class NoticeLog: @unchecked Sendable {
    private let lock = NSLock()
    private var stored: [XPubMessage] = []
    private var collector: Task<Void, Never>?

    init(_ xpub: XPubSocket) {
        collector = Task { [self] in
            while !Task.isCancelled {
                guard let notice = try? await xpub.receive() else { return }
                lock.withLock { stored.append(notice) }
            }
        }
    }

    var all: [XPubMessage] { lock.withLock { stored } }

    func wait(
        _ description: String, timeout: Duration = .seconds(10), until condition: ([XPubMessage]) -> Bool
    ) async throws {
        let deadline = ContinuousClock.now + timeout
        while !condition(all) {
            if ContinuousClock.now >= deadline {
                throw PeerError("no \(description) within \(timeout); notices: \(all)")
            }
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    func stop() { collector?.cancel() }
}

@Suite(.serialized, .enabled(if: interopEnabled), .timeLimit(.minutes(2)))
struct LibzmqInteropStage2 {
    // MARK: IPC

    @Test(arguments: Role.allCases)
    func ipcSwiftPushToPeerPull(_ role: Role) async throws {
        let push = PushSocket(options: interopOptions())
        let messages: [Message] = ["hello", ["part1", "part2"]]
        let peer = try await interop(push, peer: "PULL", role, transport: .ipc, peerArguments: ["--recv", "2"]) { _ in
            for message in messages { try await within { try await push.send(message) } }
        }
        #expect(peer.received == messages.map(bytes), "\(peer.log)")
    }

    @Test(arguments: Role.allCases)
    func ipcSwiftReqToPeerRep(_ role: Role) async throws {
        let req = ReqSocket(options: interopOptions())
        let requests: [Message] = [["first", "second"], "again"]
        var replies: [Message] = []
        let peer = try await interop(req, peer: "REP", role, transport: .ipc, peerArguments: ["--echo", "2"]) { _ in
            for request in requests { replies.append(try await within { try await req.request(request) }) }
        }
        #expect(peer.received == requests.map(bytes), "\(peer.log)")
        #expect(replies == requests, "\(peer.log)")
    }

    // MARK: XPUB <- SUB

    /// The peer SUB subscribes to "a" once. The XPUB reports it once; when the peer goes away the
    /// subscription ends and the XPUB reports that.
    @Test(arguments: Role.allCases)
    func swiftXpubSeesOneSubscribeFromPeerSub(_ role: Role) async throws {
        let xpub = XPubSocket(options: interopOptions())
        let notices = NoticeLog(xpub)
        defer { notices.stop() }
        let peer = try await interop(
            xpub, peer: "SUB", role,
            peerArguments: ["--subscribe", hexString(bytes("a")), "--then", "pause:1.5"]
        ) { peer in
            try await notices.wait("subscribe notice") { $0.contains(.subscribe(prefix: "a")) }
            // Nothing else may follow while the peer is still connected.
            try await Task.sleep(for: .milliseconds(300))
            #expect(notices.all == [.subscribe(prefix: "a")], "\(peer.log)")
            try await peer.waitForEvent("done")
            try await notices.wait("unsubscribe notice after the peer left") {
                $0.contains(.unsubscribe(prefix: "a"))
            }
            try await Task.sleep(for: .milliseconds(200))
        }
        #expect(notices.all == [.subscribe(prefix: "a"), .unsubscribe(prefix: "a")], "\(peer.log)")
    }

    /// libzmq's SUB sends SUBSCRIBE for every `subscribe` call but CANCEL only for the last `unsubscribe`.
    /// The peer subscribes "a" twice and takes it back twice: the XPUB must report one `subscribe` and,
    /// while the peer is still connected, one `unsubscribe` (the publisher keeps a set per peer).
    @Test(arguments: Role.allCases)
    func swiftXpubSeesSubscribeAndUnsubscribeOnceFromPeerSub(_ role: Role) async throws {
        let xpub = XPubSocket(options: interopOptions())
        let notices = NoticeLog(xpub)
        defer { notices.stop() }
        // The peer's --then steps, counted by its "step" events:
        // 1 pause, 2 second subscribe, 3 pause, 4 first unsubscribe, 5 second unsubscribe, 6 pause.
        let peer = try await interop(
            xpub, peer: "SUB", role,
            peerArguments: [
                "--subscribe", hexString(bytes("a")),
                "--then", "pause:0.5", "--then", "subscribe:61", "--then", "pause:0.5",
                "--then", "unsubscribe:61", "--then", "unsubscribe:61", "--then", "pause:2.0",
            ]
        ) { peer in
            try await notices.wait("first subscribe notice") { $0.contains(.subscribe(prefix: "a")) }
            try await peer.waitForEvent("step", count: 2)  // the second SUBSCRIBE has been sent
            try await Task.sleep(for: .milliseconds(300))
            #expect(notices.all == [.subscribe(prefix: "a")], "second subscribe changed something\n\(peer.log)")

            try await peer.waitForEvent("step", count: 5)  // both unsubscribes have been called
            // The peer sleeps for 2 s more and is still connected: the notice must come from the CANCEL.
            try await notices.wait("unsubscribe notice while the peer is connected", timeout: .seconds(1)) {
                $0.contains(.unsubscribe(prefix: "a"))
            }
            try await peer.waitForEvent("done")
            try await Task.sleep(for: .milliseconds(200))
        }
        #expect(notices.all == [.subscribe(prefix: "a"), .unsubscribe(prefix: "a")], "\(peer.log)")
    }

    // MARK: XSUB -> XPUB

    /// Swift XSUB subscribes to "t"; the XPUB peer sees `0x01 t`, then sends a message that does not
    /// match (the peer's XPUB filters it) and one that does. The XSUB must receive the matching one first.
    @Test(arguments: Role.allCases)
    func swiftXsubSubscribesOnPeerXpub(_ role: Role) async throws {
        let xsub = XSubSocket(options: interopOptions())
        try await xsub.subscribe("t")
        let skipped: Message = ["x", "no"]
        let wanted: Message = ["t1", "payload"]
        var received: Message?
        let peer = try await interop(
            xsub, peer: "XPUB", role,
            peerArguments: [
                "--wait-sub", "1", "--send", peerMessage(bytes(skipped)), "--send", peerMessage(bytes(wanted)),
            ]
        ) { _ in
            received = try await within { try await xsub.receive() }
        }
        #expect(received == wanted, "\(peer.log)")
        #expect(peer.subscriptions == [[[0x01] + bytes("t")]], "\(peer.log)")
    }

    // MARK: Heartbeats

    /// Swift PUSH pings every 100 ms and gives up after 300 ms of silence. A peer that is frozen
    /// (SIGSTOP) must show up as `disconnected`; after SIGCONT the socket reconnects and delivers.
    @Test
    func swiftDetectsAFrozenPeer() async throws {
        var options = interopOptions()
        options.heartbeatInterval = .milliseconds(100)
        options.heartbeatTimeout = .milliseconds(300)
        options.handshakeTimeout = .milliseconds(300)
        options.reconnectInterval = .milliseconds(50)
        let push = PushSocket(options: options)
        let log = EventLog(push.events())
        defer { log.stop() }

        let peer = try await interop(push, peer: "PULL", .peerBinds, peerArguments: ["--recv", "2"]) { peer in
            // The peer is stopped only inside this scope; every way out continues it (and a failure
            // terminates it in `interop`, which continues a stopped peer first).
            defer { peer.resume() }
            try await within { try await push.send("one") }
            try await peer.waitForEvent("recv", count: 1)
            let handshakesBefore = log.count(isHandshakeSucceeded)
            #expect(handshakesBefore == 1, "\(log.all)")
            #expect(log.count(isDisconnected) == 0, "\(log.all)")

            try peer.suspend()
            let stoppedAt = ContinuousClock.now
            try await log.wait("disconnected after SIGSTOP", timeout: .seconds(4)) {
                $0.contains(where: isDisconnected)
            }
            let detection = ContinuousClock.now - stoppedAt
            #expect(detection < .seconds(2), "took \(detection)")

            peer.resume()
            try await log.wait("handshakeSucceeded after SIGCONT", timeout: .seconds(15)) {
                $0.filter(isHandshakeSucceeded).count > handshakesBefore
            }
            try await within { try await push.send("two") }
        }
        #expect(peer.received == [[bytes("one")], [bytes("two")]], "\(peer.log)")
    }

    /// The libzmq peer pings every 50 ms and drops the connection after 200 ms without an answer
    /// (its PINGs carry a 1 s TTL). Swift must answer with PONGs: the connection lasts for over a
    /// second with no `disconnected`, and a message still flows afterwards.
    @Test(arguments: Role.allCases)
    func peerHeartbeatsKeepTheConnectionUp(_ role: Role) async throws {
        let pull = PullSocket(options: interopOptions())
        let log = EventLog(pull.events())
        defer { log.stop() }
        let message: Message = ["still", "here"]
        var received: Message?
        // Taken before `interop` closes the socket, which ends the connection with its own `disconnected`.
        var eventsAtTheEnd: [SocketEvent] = []
        let peer = try await interop(
            pull, peer: "PUSH", role,
            peerArguments: [
                // Without our PONGs libzmq would drop the connection about 0.65 s in, well inside the
                // 2 s pause; the 600 ms timeout leaves room for a loaded machine to answer late.
                // The pause after the send keeps the peer from closing (and so disconnecting us) before
                // the events are read.
                "--sockopt", "HEARTBEAT_IVL=50", "--sockopt", "HEARTBEAT_TIMEOUT=600",
                "--sockopt", "HEARTBEAT_TTL=1000",
                "--then", "pause:2.0", "--then", "send:" + peerMessage(bytes(message)),
                "--then", "pause:0.5",
            ]
        ) { _ in
            received = try await within { try await pull.receive() }
            eventsAtTheEnd = log.all
        }
        #expect(received == message, "\(peer.log)")
        #expect(eventsAtTheEnd.filter(isDisconnected).isEmpty, "the connection dropped: \(eventsAtTheEnd)")
        #expect(eventsAtTheEnd.filter(isConnectRetried).isEmpty, "\(eventsAtTheEnd)")
        #expect(eventsAtTheEnd.filter(isHandshakeSucceeded).count == 1, "\(eventsAtTheEnd)")
    }
}
#endif
