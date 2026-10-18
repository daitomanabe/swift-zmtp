import ZMQ

/// A server that answers many clients at once: ROUTER and DEALER with a proxy between them, and a pool of
/// workers behind it.
///
///     clients (REQ) ---tcp---> ROUTER --proxy--> DEALER ---inproc---> workers (REP)
///
/// The ROUTER is the front door: it knows which client each request came from. The DEALER hands the
/// requests to the workers in turn. `ZMQProxy.run` carries messages between the two in both directions;
/// the replies find their way back because the client's address travels in front of the request.
///
/// `inproc://` connects sockets inside one process, without the network. Both ends must use the same
/// `ZMQContext` (here the default one).
///
/// Everything runs in one process: the proxy, the workers, and a few clients that send some requests.
/// With `--clients 0` there are no clients of its own, and the server waits for outside ones until its
/// time limit, so `zmq-examples hwclient` can talk to it.
enum MultithreadedServer {
    static let workersEndpoint = "inproc://workers"

    /// `mtserver [endpoint] [--workers N] [--clients N] [--count N]`
    static func run(_ arguments: Arguments) async throws {
        let frontendEndpoint = arguments.endpoint(default: HelloWorld.defaultEndpoint)
        let workerCount = try arguments.int("workers", default: 3)
        let clientCount = try arguments.int("clients", default: 3)
        let requestCount = try arguments.int("count", default: 3)

        let router = RouterSocket()
        let dealer = DealerSocket()
        let bound = try await router.bind(frontendEndpoint)
        try await dealer.bind(workersEndpoint)
        print("mtserver: clients connect to \(bound), \(workerCount) workers behind \(workersEndpoint)")

        try await withThrowingTaskGroup(of: Void.self) { server in
            server.addTask { try await ZMQProxy.run(frontend: router, backend: dealer) }
            for id in stride(from: 1, through: workerCount, by: 1) {
                server.addTask { await work(id: id) }
            }

            if clientCount > 0 {
                try await withThrowingTaskGroup(of: Void.self) { clients in
                    for id in 1...clientCount {
                        clients.addTask { try await ask(id: id, endpoint: bound, count: requestCount) }
                    }
                    try await clients.waitForAll()
                }
                server.cancelAll()   // the clients are done: stop the proxy and the workers
            } else {
                try await server.waitForAll()   // until the time limit cancels everything
            }
        }
        await router.close()
        await dealer.close()
    }

    /// One worker: a REP socket that connects to the DEALER, as if it were a plain server.
    /// It does not know that there is a proxy, nor how many workers there are.
    static func work(id: Int) async {
        let rep = RepSocket()
        do {
            try await rep.connect(workersEndpoint)
            while true {
                let request = try await rep.receive()
                let text = request.string ?? "(binary data)"
                try await rep.reply("World (\(text)) from worker \(id)")
            }
        } catch {
            // Cancelled: the server is stopping.
        }
        await rep.close()
    }

    /// One client: sends `count` requests, one after the other, and prints the answers.
    static func ask(id: Int, endpoint: String, count: Int) async throws {
        let req = ReqSocket()
        try await req.connect(endpoint)
        for number in 0..<count {
            let reply = try await req.request("Hello \(number)")
            print("mtserver: client \(id) got \"\(reply.string ?? "(binary data)")\"")
        }
        await req.close()
    }
}
