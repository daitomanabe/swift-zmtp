import ZMQ

/// Request/reply: the first pattern of the ZeroMQ Guide.
///
/// A client (REQ) sends one request and waits for one reply. A server (REP) takes one request at a time
/// and answers it before it takes the next. Each socket type allows only that order, and the API makes
/// the wrong order impossible to write: `ReqSocket` has `request(_:)` (send and wait in one call), and
/// `RepSocket` has `receive()` and `reply(_:)`.
///
///     zmq-examples hwserver                  in one terminal
///     zmq-examples hwclient                  in another
enum HelloWorld {
    static let defaultEndpoint = "tcp://127.0.0.1:5555"

    /// `hwserver [endpoint] [--count N]`: answers requests until it has answered N of them (default: no limit).
    static func server(_ arguments: Arguments) async throws {
        let endpoint = arguments.endpoint(default: defaultEndpoint)
        let limit = try arguments.int("count", default: .max)

        let rep = RepSocket()
        // bind() returns the endpoint that was bound; it differs from the one asked for when the
        // endpoint is "tcp://127.0.0.1:*" (the system picks a free port).
        let bound = try await rep.bind(endpoint)
        print("hwserver: listening on \(bound)")

        var served = 0
        while served < limit {
            let request = try await rep.receive()
            print("hwserver: received \(request.string ?? "(binary data)")")
            try await rep.reply("World")
            served += 1
        }
        await rep.close()
    }

    /// `hwclient [endpoint] [--count N]`: sends N requests (default 10), one after the other.
    static func client(_ arguments: Arguments) async throws {
        let endpoint = arguments.endpoint(default: defaultEndpoint)
        let count = try arguments.int("count", default: 10)

        let req = ReqSocket()
        // connect() returns at once. The connection is made in the background, so the server may be
        // started after the client, and a request sent before then waits in the socket.
        try await req.connect(endpoint)

        for number in 0..<count {
            print("hwclient: sending Hello \(number)")
            let reply = try await req.request("Hello")
            print("hwclient: received \(reply.string ?? "(binary data)") \(number)")
        }
        await req.close()
    }
}
