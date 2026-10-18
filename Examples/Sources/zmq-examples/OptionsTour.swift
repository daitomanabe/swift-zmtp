import ZMQ

/// `SocketOptions`: the settings of a socket, fixed when it is created. A short tour with something to see for
/// the ones whose effect is easy to show. Each part is one function, and none needs another process.
enum OptionsTour {
    static func run(_ arguments: Arguments) async throws {
        try await tuned()
        try await highWaterMark()
        try await conflate()
        try await immediate()
        try await routerMandatory()
    }

    /// Options for a connection that has to survive a flaky network. See the comments for what each one does.
    static func tunedOptions() -> SocketOptions {
        var options = SocketOptions()
        // At most this many messages wait for each peer (see highWaterMark() for what happens at the limit).
        options.sendHighWaterMark = 100
        options.receiveHighWaterMark = 100
        // close() waits this long for queued messages to be written. nil: as long as it takes.
        options.linger = .seconds(2)
        // A connecting socket that lost its peer retries after 100 ms, then 200 ms, 400 ms, ... up to 5 s.
        options.reconnectInterval = .milliseconds(100)
        options.reconnectIntervalMax = .seconds(5)
        // Send a PING every 2 s. If nothing at all comes back within 4 s, the connection is closed
        // (and a connecting socket makes a new one).
        options.heartbeatInterval = .seconds(2)
        options.heartbeatTimeout = .seconds(4)
        // TCP keepalive probes after 30 s without traffic, for the operating system's own dead-peer detection.
        options.tcpKeepAlive = .seconds(30)
        // A frame bigger than this closes the connection instead of filling the memory.
        options.maxMessageSize = 1_000_000
        // The name by which a ROUTER knows this socket (DEALER and REQ).
        options.routingID = RoutingID(Frame("worker-1"))
        return options
    }

    static func tuned() async throws {
        print("tuned options: a PUSH socket with them sends to a PULL socket with the defaults")
        let pull = PullSocket()
        let endpoint = try await pull.bind("tcp://127.0.0.1:*")
        let push = PushSocket(options: tunedOptions())
        try await push.connect(endpoint)
        try await push.send("hello")
        print("  received \"\(try await pull.receive().frames[0].string ?? "")\"")
        await push.close()
        await pull.close()
    }

    /// With a send high-water mark of 2 and nobody to send to, two messages fit and the third `send` waits.
    static func highWaterMark() async throws {
        print("high-water mark: sendHighWaterMark = 2, and nobody is listening")
        var options = SocketOptions()
        options.sendHighWaterMark = 2
        let push = PushSocket(options: options)
        try await push.connect(try await unusedEndpoint())
        for number in 1...3 {
            let message = Message("message \(number)")
            let queued = try await timeout(after: .milliseconds(300)) { try await push.send(message) } != nil
            print("  message \(number): \(queued ? "queued" : "still waiting for room after 300 ms")")
        }
        await push.close()
    }

    /// With `conflate`, a queue keeps only the newest message: for a slow reader the latest value matters,
    /// not the backlog (a sensor reading, a camera frame).
    static func conflate() async throws {
        print("conflate: five messages are sent before the receiver is there")
        var options = SocketOptions()
        options.conflate = true
        let push = PushSocket(options: options)
        let endpoint = try await unusedEndpoint()
        try await push.connect(endpoint)
        for value in 1...5 { try await push.send(Message("value \(value)")) }   // never waits, keeps the newest

        let pull = PullSocket()
        try await pull.bind(endpoint)
        let newest = try await pull.receive()
        print("  received \"\(newest.frames[0].string ?? "")\"")
        let more = try await timeout(after: .milliseconds(300)) { try await pull.receive() }
        print("  anything else after that? \(more == nil ? "no" : "yes")")
        await push.close()
        await pull.close()
    }

    /// By default `connect` creates the queue at once, so messages wait for a peer that is not there yet.
    /// With `immediate` a connection counts only after its handshake, and `send` waits until there is one.
    static func immediate() async throws {
        print("immediate: send to an endpoint where nothing listens")
        let endpoint = try await unusedEndpoint()
        for useImmediate in [false, true] {
            var options = SocketOptions()
            options.immediate = useImmediate
            let push = PushSocket(options: options)
            try await push.connect(endpoint)
            let queued = try await timeout(after: .milliseconds(300)) { try await push.send("hello") } != nil
            print("  immediate = \(useImmediate): \(queued ? "send returned, the message is queued" : "send is still waiting for a peer")")
            await push.close()
        }
    }

    /// A ROUTER drops a message for an unknown routing ID without a word, unless `routerMandatory` is set.
    static func routerMandatory() async throws {
        print("routerMandatory: send to a routing ID that nobody has")
        for mandatory in [false, true] {
            var options = SocketOptions()
            options.routerMandatory = mandatory
            let router = RouterSocket(options: options)
            do {
                try await router.send("hello", to: RoutingID(Frame("nobody")))
                print("  routerMandatory = \(mandatory): no error, the message was dropped")
            } catch ZMQError.hostUnreachable(let routingID) {
                print("  routerMandatory = \(mandatory): hostUnreachable, nobody is called \"\(routingID.frame.string ?? "")\"")
            }
            await router.close()
        }
    }
}
