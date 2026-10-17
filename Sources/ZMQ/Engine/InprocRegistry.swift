import Synchronization

/// The `inproc://` names bound inside one `ZMQContext`, and the connecting sockets waiting for a name.
///
/// The mutex here is the only lock of the inproc transport. Every call changes the table and returns;
/// no socket is called while the lock is held. Linking two sockets then runs on their event loops.
final class InprocRegistry: Sendable {
    /// A connect-side pipe that waits for its name to be bound.
    struct Waiter: Sendable {
        let core: SocketCore
        let pipeID: Int
    }

    private struct State {
        var bound: [String: SocketCore] = [:]
        var waiting: [String: [Waiter]] = [:]
    }

    private let state = Mutex(State())

    /// Binds `name` to `core`. Returns the sockets that were waiting for the name (they are not waiting
    /// any more and must be told), or nil when the name is taken.
    func bind(_ name: String, core: SocketCore) -> [Waiter]? {
        state.withLock { state in
            guard state.bound[name] == nil else { return nil }
            state.bound[name] = core
            return state.waiting.removeValue(forKey: name) ?? []
        }
    }

    /// Frees `name` if `core` holds it.
    func unbind(_ name: String, core: SocketCore) {
        // The entry is released after the lock is gone, so nothing runs under the lock but table updates.
        let removed: SocketCore? = state.withLock { state in
            guard let owner = state.bound[name], owner === core else { return nil }
            return state.bound.removeValue(forKey: name)
        }
        _ = removed
    }

    /// The socket bound to `name`. When there is none, `waiter` is recorded and told at the next bind.
    /// Looking and recording happen under one lock, so a bind cannot slip in between.
    func lookupOrWait(_ name: String, waiter: Waiter) -> SocketCore? {
        state.withLock { state in
            if let owner = state.bound[name] { return owner }
            state.waiting[name, default: []].append(waiter)
            return nil
        }
    }

    /// Takes `core`'s waiting entry for `pipeID` back, if it is still there.
    func cancelWait(_ name: String, core: SocketCore, pipeID: Int) {
        let removed: [Waiter] = state.withLock { state in
            guard var waiters = state.waiting[name] else { return [] }
            let mine = waiters.filter { $0.core === core && $0.pipeID == pipeID }
            waiters.removeAll { $0.core === core && $0.pipeID == pipeID }
            state.waiting[name] = waiters.isEmpty ? nil : waiters
            return mine
        }
        _ = removed
    }
}
