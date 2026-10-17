import NIOCore
import ZMTP

// The inproc side of `SocketCore`: binding a name, linking a connect-side pipe to the socket bound to
// that name, and the type check that replaces the ZMTP handshake. Everything runs on the loop of the
// socket it is called on; the two sockets of a link talk through `InprocConnection`.
extension SocketCore {
    // MARK: Bind side

    /// `bind("inproc://NAME")`: takes the name in the context's table, then links the connect-side pipes
    /// that were waiting for it.
    func bindInproc(_ name: String, endpoint: String) -> EventLoopFuture<String> {
        loop.preconditionInEventLoop()
        guard let waiting = inproc.bind(name, core: self) else {
            let reason = "address already in use"
            events.emit(.bindFailed(endpoint: endpoint, reason: reason))
            return loop.makeFailedFuture(ZMQError.bindFailed(endpoint: endpoint, reason: reason))
        }
        inprocNames.append(name)
        events.emit(.listening(endpoint: endpoint))
        let bound = self
        for waiter in waiting {
            let connector = waiter.core
            let pipeID = waiter.pipeID
            connector.loop.execute { connector.inprocNameBound(pipeID: pipeID, to: bound) }
        }
        return loop.makeSucceededFuture(endpoint)
    }

    /// Gives the names back (`close()` and teardown). Links that are up stay until the socket is torn down.
    func unbindInprocNames() {
        for name in inprocNames { inproc.unbind(name, core: self) }
        inprocNames.removeAll()
    }

    /// A connect request for one of our names arrived (on our loop). Answers on `remote`.
    func acceptInproc(name: String, from remote: InprocConnection) {
        loop.preconditionInEventLoop()
        // The name may have been given back since the connector looked it up.
        guard phase == .open, inprocNames.contains(name) else {
            remote.deliverAnswer(.unavailable)
            return
        }
        let endpoint = "inproc://\(name)"
        events.emit(.accepted(endpoint: endpoint))

        let peer = remote.localMetadata
        if let reason = incompatibility(with: peer) {
            events.emit(.handshakeFailed(endpoint: endpoint, reason: reason))
            events.emit(.disconnected(endpoint: endpoint))
            remote.deliverAnswer(.refused(reason: reason))
            return
        }

        let connection = InprocConnection(core: self, endpoint: endpoint)
        inprocLinks[ObjectIdentifier(connection)] = connection
        connection.beginAccept()
        // Creates the pipe and runs `pipeAttached`; the connection may write subscriptions meanwhile, and
        // holds them back until the connect side has been told.
        guard handshakeCompleted(connection, peer: peer) else {
            let reason = "the socket refused the connection"
            inprocLinks[ObjectIdentifier(connection)] = nil
            connection.discard()
            events.emit(.handshakeFailed(endpoint: endpoint, reason: reason))
            events.emit(.disconnected(endpoint: endpoint))
            remote.deliverAnswer(.refused(reason: reason))
            return
        }
        connection.announce(to: remote)
    }

    // MARK: Connect side

    /// Connect-side pipe on an `inproc://` endpoint: link to the bound socket, or wait for the bind.
    func connectInproc(_ pipe: Pipe, name: String, endpoint: String) {
        loop.preconditionInEventLoop()
        let waiter = InprocRegistry.Waiter(core: self, pipeID: pipe.id)
        if let bound = inproc.lookupOrWait(name, waiter: waiter) {
            requestInprocLink(pipe, name: name, endpoint: endpoint, to: bound)
        } else {
            pipe.link = .waitingForBind
        }
    }

    /// The name a waiting pipe of ours was waiting for has been bound (on our loop, from the binder).
    func inprocNameBound(pipeID: Int, to bound: SocketCore) {
        loop.preconditionInEventLoop()
        guard phase == .open || phase == .lingering,
            let pipe = pipes.first(where: { $0.id == pipeID }),
            case .waitingForBind = pipe.link,
            case .connect(.inproc(let name), let endpoint) = pipe.origin
        else { return }
        requestInprocLink(pipe, name: name, endpoint: endpoint, to: bound)
    }

    private func requestInprocLink(_ pipe: Pipe, name: String, endpoint: String, to bound: SocketCore) {
        let connection = InprocConnection(core: self, endpoint: endpoint)
        connection.attach(pipe)
        pipe.link = .linked(connection)
        inprocLinks[ObjectIdentifier(connection)] = connection
        bound.loop.execute { bound.acceptInproc(name: name, from: connection) }
    }

    /// The bound socket answered the request of `connection` (on our loop).
    func inprocAnswered(_ connection: InprocConnection, _ answer: InprocConnection.Answer) {
        loop.preconditionInEventLoop()
        // Only the attempt the pipe is waiting for may take the answer.
        guard phase == .open || phase == .lingering, let pipe = connection.pipe,
            case .linked(let current) = pipe.link, current === connection
        else {
            if case .accepted(let end, _) = answer { end.notifyPeerClosed() }
            connection.close()
            return
        }
        switch answer {
        case .unavailable:
            // Nothing was reached; wait for the next bind of the name.
            connection.ended(waitingForBind: true)
        case .refused(let reason):
            connection.reachedPeer()
            events.emit(.connected(endpoint: connection.endpoint))
            events.emit(.handshakeFailed(endpoint: connection.endpoint, reason: reason))
            connection.close()
        case .accepted(let end, let metadata):
            connection.link(to: end)
            events.emit(.connected(endpoint: connection.endpoint))
            if let reason = incompatibility(with: metadata) {
                events.emit(.handshakeFailed(endpoint: connection.endpoint, reason: reason))
                connection.close()
            } else if !handshakeCompleted(connection, peer: metadata) {
                connection.markNotReady()
                events.emit(
                    .handshakeFailed(endpoint: connection.endpoint, reason: "the socket refused the connection"))
                connection.close()
            }
        }
    }

    // MARK: Handshake replacement

    /// Why this socket cannot talk to `peer`, or nil when the socket types fit.
    private func incompatibility(with peer: Metadata) -> String? {
        let local = behavior.socketType
        guard let name = peer.socketType, let type = SocketType(rawValue: name) else {
            return "the peer sent no valid Socket-Type"
        }
        guard local.isCompatible(with: type) else {
            return "incompatible socket types: \(local.rawValue) cannot talk to \(name)"
        }
        return nil
    }
}
