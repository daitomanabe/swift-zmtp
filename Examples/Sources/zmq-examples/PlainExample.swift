import ZMQ

/// PLAIN: a user name and a password. Nothing is encrypted, so use it only where the network is trusted
/// (or inside another protected channel); use CURVE otherwise. The server and the clients run in this process.
enum PlainExample {
    static func run(_ arguments: Arguments) async throws {
        // The server knows its accounts: user name to password.
        var serverOptions = SocketOptions()
        serverOptions.security = .plainServer(accepting: ["admin": "secret"])
        let rep = RepSocket(options: serverOptions)
        let endpoint = try await rep.bind("tcp://127.0.0.1:*")
        let server = Task {
            while true {
                _ = try await rep.receive()
                try await rep.reply("World")
            }
        }

        for password in ["secret", "guess"] {
            // A client sends its user name and password right after it connects.
            var clientOptions = SocketOptions()
            clientOptions.security = .plainClient(username: "admin", password: password)
            clientOptions.reconnectInterval = .seconds(1)
            let req = ReqSocket(options: clientOptions)
            let events = req.events()
            let log = Task {
                for await event in events {
                    if case .handshakeFailed(_, let reason) = event {
                        print("  handshake failed: \(reason)")
                        break   // a refused client tries again and again; the first time is enough
                    }
                }
            }
            try await req.connect(endpoint)

            let reply = try await timeout(after: .seconds(2)) { try await req.request("Hello") }
            print("password \"\(password)\": \(reply.map { "reply \"\($0.frames[0].string ?? "")\"" } ?? "no reply within 2 s")")
            await req.close()
            await log.value
        }

        server.cancel()
        await rep.close()
    }
}
