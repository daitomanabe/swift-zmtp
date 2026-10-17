#if os(macOS)
import Foundation
import Testing
import ZMQ

/// Which side listens. The other side connects.
enum Role: String, CaseIterable, Sendable, CustomTestStringConvertible {
    case swiftBinds
    case peerBinds

    var testDescription: String { rawValue }
}

private let interopEnabled = ProcessInfo.processInfo.environment["ZMQ_INTEROP"] == "1"

private struct Timeout: Error {}

/// Runs `operation`; fails with `Timeout` if it takes longer than `seconds`. The losing task is cancelled.
private func within<T: Sendable>(
    _ seconds: Int = 15, _ operation: @escaping @Sendable () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask { try await operation() }
        group.addTask {
            try await Task.sleep(for: .seconds(seconds))
            throw Timeout()
        }
        defer { group.cancelAll() }
        guard let result = try await group.next() else { throw Timeout() }
        return result
    }
}

private func interopOptions(routingID: String? = nil) -> SocketOptions {
    var options = SocketOptions()
    options.reconnectInterval = .milliseconds(20)
    options.linger = .milliseconds(500)
    if let routingID { options.routingID = RoutingID(Frame(routingID)) }
    return options
}

/// The frames of `message` as bytes, the form the peer's events are decoded into.
private func bytes(_ message: Message) -> [[UInt8]] { message.frames.map(\.bytes) }

private func bytes(_ text: String) -> [UInt8] { Array(text.utf8) }

/// Brings one Swift socket and one libzmq peer together, runs `body`, waits for the peer to finish
/// (status 0, `done`), and closes the socket. Returns the finished peer so the caller can check
/// what it received. On any failure the peer is terminated and the socket closed.
private func interop<S: ZMQSocket>(
    _ socket: S, peer peerType: String, _ role: Role, peerArguments: [String] = [],
    _ body: (PeerProcess) async throws -> Void
) async throws -> PeerProcess {
    var peer: PeerProcess?
    do {
        switch role {
        case .swiftBinds:
            let endpoint = try await within { try await socket.bind("tcp://127.0.0.1:*") }
            peer = try PeerProcess(arguments: ["--type", peerType, "--connect", endpoint] + peerArguments)
            try await peer?.waitForEvent("ready", timeout: .seconds(60))
        case .peerBinds:
            peer = try PeerProcess(arguments: ["--type", peerType, "--bind", "tcp://127.0.0.1:*"] + peerArguments)
            let ready = try await peer?.waitForEvent("ready", timeout: .seconds(60))
            guard let endpoint = ready?.first?.endpoints.first else { throw PeerError("peer printed no endpoint") }
            try await within { try await socket.connect(endpoint) }
        }
        guard let running = peer else { throw PeerError("peer did not start") }
        try await body(running)
        try await running.finish()
        await socket.close()
        return running
    } catch {
        peer?.terminate()
        await socket.close()
        throw error
    }
}

@Suite(.serialized, .enabled(if: interopEnabled), .timeLimit(.minutes(2)))
struct LibzmqInterop {
    // MARK: PUSH / PULL

    @Test(arguments: Role.allCases)
    func swiftPushToPeerPull(_ role: Role) async throws {
        let push = PushSocket(options: interopOptions())
        let messages: [Message] = ["hello", ["part1", "part2"]]
        let peer = try await interop(push, peer: "PULL", role, peerArguments: ["--recv", "2"]) { _ in
            for message in messages { try await within { try await push.send(message) } }
        }
        #expect(peer.received == messages.map(bytes), "\(peer.log)")
    }

    @Test(arguments: Role.allCases)
    func swiftPullFromPeerPush(_ role: Role) async throws {
        let pull = PullSocket(options: interopOptions())
        let messages: [Message] = ["hello", ["part1", "", "part3"]]
        let sends = messages.flatMap { ["--send", peerMessage(bytes($0))] }
        var received: [Message] = []
        let peer = try await interop(pull, peer: "PUSH", role, peerArguments: sends) { _ in
            for _ in messages { received.append(try await within { try await pull.receive() }) }
        }
        #expect(received == messages, "\(peer.log)")
    }

    // MARK: PUB / SUB

    /// The peer subscribes to the prefix `topic.`; Swift publishes a message that does not match and one
    /// that does, every 20 ms, until the peer reports a message. Only the matching one may arrive.
    @Test(arguments: Role.allCases)
    func swiftPubToPeerSub(_ role: Role) async throws {
        let pub = PubSocket(options: interopOptions())
        let skipped: Message = ["other", "x"]
        let wanted: Message = ["topic.a", "payload"]
        let peer = try await interop(
            pub, peer: "SUB", role,
            peerArguments: ["--subscribe", hexString(bytes("topic.")), "--recv", "1"]
        ) { peer in
            let publisher = Task {
                while !Task.isCancelled {
                    try await pub.send(skipped)
                    try await pub.send(wanted)
                    try await Task.sleep(for: .milliseconds(20))
                }
            }
            defer { publisher.cancel() }
            try await peer.waitForEvent("recv")
            publisher.cancel()
            _ = await publisher.result
        }
        #expect(peer.received == [bytes(wanted)], "\(peer.log)")
    }

    /// Swift SUB subscribes to `topic.`; the XPUB peer sees that subscription (0x01 + prefix), then
    /// sends a message that does not match and one that does. Only the matching one may arrive.
    @Test(arguments: Role.allCases)
    func swiftSubFromPeerXpub(_ role: Role) async throws {
        let sub = SubSocket(options: interopOptions())
        try await sub.subscribe("topic.")
        let skipped: Message = ["other", "x"]
        let wanted: Message = ["topic.a", "payload"]
        var received: Message?
        let peer = try await interop(
            sub, peer: "XPUB", role,
            peerArguments: [
                "--wait-sub", "1", "--send", peerMessage(bytes(skipped)), "--send", peerMessage(bytes(wanted)),
            ]
        ) { _ in
            received = try await within { try await sub.receive() }
        }
        #expect(received == wanted, "\(peer.log)")
        #expect(peer.subscriptions == [[[0x01] + bytes("topic.")]], "\(peer.log)")
    }

    // MARK: REQ / REP

    @Test(arguments: Role.allCases)
    func swiftReqToPeerRep(_ role: Role) async throws {
        let req = ReqSocket(options: interopOptions())
        let requests: [Message] = [["first", "second"], "again"]
        var replies: [Message] = []
        let peer = try await interop(req, peer: "REP", role, peerArguments: ["--echo", "2"]) { _ in
            for request in requests { replies.append(try await within { try await req.request(request) }) }
        }
        #expect(peer.received == requests.map(bytes), "\(peer.log)")
        #expect(replies == requests, "\(peer.log)")
    }

    @Test(arguments: Role.allCases)
    func swiftRepFromPeerReq(_ role: Role) async throws {
        let rep = RepSocket(options: interopOptions())
        let request: Message = ["ping", "2nd"]
        let reply: Message = ["pong"]
        var received: Message?
        let peer = try await interop(
            rep, peer: "REQ", role,
            peerArguments: ["--send", peerMessage(bytes(request)), "--recv", "1"]
        ) { _ in
            received = try await within { try await rep.receive() }
            try await within { try await rep.reply(reply) }
        }
        #expect(received == request, "\(peer.log)")
        #expect(peer.received == [bytes(reply)], "\(peer.log)")
    }

    // MARK: ROUTER / DEALER

    /// The ROUTER peer sees `[REQ's routing ID, empty frame, body…]` and sends the same back.
    @Test(arguments: Role.allCases)
    func swiftReqToPeerRouter(_ role: Role) async throws {
        let req = ReqSocket(options: interopOptions(routingID: "req-1"))
        let request: Message = ["hello", "world"]
        var reply: Message?
        let peer = try await interop(req, peer: "ROUTER", role, peerArguments: ["--echo", "1"]) { _ in
            reply = try await within { try await req.request(request) }
        }
        #expect(peer.received == [[bytes("req-1"), []] + bytes(request)], "\(peer.log)")
        #expect(reply == request, "\(peer.log)")
    }

    @Test(arguments: Role.allCases)
    func swiftDealerToPeerRouter(_ role: Role) async throws {
        let dealer = DealerSocket(options: interopOptions(routingID: "dealer-1"))
        let message: Message = ["a", Frame(), "b"]
        var reply: Message?
        let peer = try await interop(dealer, peer: "ROUTER", role, peerArguments: ["--echo", "1"]) { _ in
            try await within { try await dealer.send(message) }
            reply = try await within { try await dealer.receive() }
        }
        #expect(peer.received == [[bytes("dealer-1")] + bytes(message)], "\(peer.log)")
        #expect(reply == message, "\(peer.log)")
    }

    /// With `withRoutingID` the peer DEALER announces `peer-1`; without it, Swift ROUTER makes up an ID.
    /// Either way the answer must reach the peer.
    @Test(arguments: Role.allCases, [true, false])
    func swiftRouterFromPeerDealer(_ role: Role, withRoutingID: Bool) async throws {
        let router = RouterSocket(options: interopOptions())
        let message: Message = ["hello", "world"]
        let answer: Message = ["reply", "x"]
        var peerArguments = ["--send", peerMessage(bytes(message)), "--recv", "1"]
        if withRoutingID { peerArguments += ["--sockopt", "ROUTING_ID=peer-1"] }
        var received: RoutedMessage?
        let peer = try await interop(router, peer: "DEALER", role, peerArguments: peerArguments) { _ in
            let routed = try await within { try await router.receive() }
            received = routed
            try await within { try await router.send(answer, to: routed.routingID) }
        }
        #expect(received?.message == message, "\(peer.log)")
        if withRoutingID {
            #expect(received?.routingID.frame.bytes == bytes("peer-1"), "\(peer.log)")
        } else {
            #expect(received?.routingID.frame.isEmpty == false, "\(peer.log)")
        }
        #expect(peer.received == [bytes(answer)], "\(peer.log)")
    }

    /// An empty frame, then the body: what a REQ would send. The REP peer sees the body only.
    @Test(arguments: Role.allCases)
    func swiftDealerToPeerRep(_ role: Role) async throws {
        let dealer = DealerSocket(options: interopOptions())
        let request: Message = [Frame(), "body"]
        var reply: Message?
        let peer = try await interop(dealer, peer: "REP", role, peerArguments: ["--echo", "1"]) { _ in
            try await within { try await dealer.send(request) }
            reply = try await within { try await dealer.receive() }
        }
        #expect(peer.received == [[bytes("body")]], "\(peer.log)")
        #expect(reply == request, "\(peer.log)")
    }

    // MARK: PAIR

    /// The second message has an empty frame and a 300-byte frame (the long frame form on the wire).
    @Test(arguments: Role.allCases)
    func swiftPairWithPeerPair(_ role: Role) async throws {
        let pair = PairSocket(options: interopOptions())
        let messages: [Message] = ["hello", [Frame(), Frame(Array(repeating: 0x61, count: 300)), "tail"]]
        var replies: [Message] = []
        let peer = try await interop(pair, peer: "PAIR", role, peerArguments: ["--echo", "2"]) { _ in
            for message in messages {
                try await within { try await pair.send(message) }
                replies.append(try await within { try await pair.receive() })
            }
        }
        #expect(peer.received == messages.map(bytes), "\(peer.log)")
        #expect(replies == messages, "\(peer.log)")
    }
}
#endif
