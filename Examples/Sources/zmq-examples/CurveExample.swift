import ZMQ

/// CURVE: encryption and authentication with public keys (CurveZMQ).
///
/// Every CURVE peer has a key pair: a public key, which it may tell anybody, and a secret key, which it keeps.
/// The CLIENT must know the SERVER's public key in advance: that is how it knows it reached the right
/// server and not somebody in the middle. The server can find out the client's public key when the client
/// connects, and decide with `authenticate` whether to let it in. Everything after the handshake is encrypted.
///
/// A key is 32 bytes. People handle it as 40 characters of Z85 text (`CurvePublicKey.z85`).
///
///     zmq-examples curve-keygen                                  a key pair for the client
///     zmq-examples curve-server --allow CLIENT_PUBLIC_KEY       prints its own public key
///     zmq-examples curve-client SERVER_PUBLIC_KEY --secret-key CLIENT_SECRET_KEY
///
/// Without `--allow`, the server accepts every client (and prints its key); without `--secret-key` a program
/// makes a new key pair each time it starts. Passing a secret key on the command line is for this example
/// only: a real program reads it from a file or the keychain, because the command line is visible to other users.
enum CurveExample {
    /// `curve-keygen`: like libzmq's `zmq_curve_keypair`.
    static func keygen(_ arguments: Arguments) async throws {
        let keys = CurveKeyPair.generate()
        print("public key: \(keys.publicKey.z85)")
        print("secret key: \(keys.secretKey.z85)")
    }

    /// A new key pair, or the one that belongs to a stored secret key (the public key is derived from it).
    static func keyPair(secretKey text: String?) throws -> CurveKeyPair {
        guard let text else { return CurveKeyPair.generate() }
        return CurveKeyPair(secretKey: try CurveSecretKey(z85: text))
    }

    /// `curve-server [endpoint] [--secret-key KEY] [--allow CLIENT_KEY]... [--count N]`
    static func server(_ arguments: Arguments) async throws {
        let endpoint = arguments.endpoint(default: HelloWorld.defaultEndpoint)
        let limit = try arguments.int("count", default: .max)
        let keys = try keyPair(secretKey: arguments.string("secret-key"))
        let allowed = Set(try arguments.strings("allow").map { try CurvePublicKey(z85: $0) })

        var options = SocketOptions()
        options.security = .curveServer(keyPair: keys) { clientKey in
            // Called once for each client that has proved that it owns `clientKey`, before any message.
            // Returning false refuses it: the connection is closed. Leave `authenticate` out to accept everybody.
            let accepted = allowed.isEmpty || allowed.contains(clientKey)
            print("curve-server: client \(clientKey.z85) \(accepted ? "accepted" : "refused")")
            return accepted
        }
        let rep = RepSocket(options: options)
        let bound = try await rep.bind(endpoint)
        print("curve-server: listening on \(bound)")
        print("curve-server: server public key \(keys.publicKey.z85)")

        var served = 0
        while served < limit {
            let request = try await rep.receive()
            print("curve-server: received \(request.string ?? "(binary data)")")
            try await rep.reply("World")
            served += 1
        }
        await rep.close()
    }

    /// `curve-client SERVER_KEY [endpoint] [--secret-key KEY] [--count N]`
    static func client(_ arguments: Arguments) async throws {
        let serverKey = try CurvePublicKey(z85: arguments.positional[0])
        let endpoint = arguments.positional.dropFirst().first ?? HelloWorld.defaultEndpoint
        let count = try arguments.int("count", default: 3)
        let keys = try keyPair(secretKey: arguments.string("secret-key"))

        var options = SocketOptions()
        options.security = .curveClient(serverKey: serverKey, keyPair: keys)
        // A refused client tries again at once and then forever. Do not hammer a server that said no.
        options.reconnectInterval = .seconds(1)
        let req = ReqSocket(options: options)

        // A refused client gets no error from request(): it just waits. The events say what happened.
        let events = req.events()
        let log = Task {
            var shown: SocketEvent?
            for await event in events {
                switch event {
                case .handshakeSucceeded, .handshakeFailed:
                    if event != shown { print("curve-client: \(event)") }   // a refused client fails again and again
                    shown = event
                default: break
                }
            }
        }

        print("curve-client: client public key \(keys.publicKey.z85)")
        try await req.connect(endpoint)
        for number in 0..<count {
            let reply: Message
            do {
                reply = try await req.request("Hello \(number)", timeout: .seconds(5))
            } catch ZMQError.timedOut {
                await req.close()
                await log.value
                throw ExampleError("no reply within 5 s: is the server key right, and does the server accept this client key?")
            }
            print("curve-client: received \(reply.string ?? "(binary data)") \(number)")
        }
        await req.close()
        await log.value
    }
}
