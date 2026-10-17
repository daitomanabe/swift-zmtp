import NIOCore
import Synchronization

/// The one place where the application's tasks and the socket's event loop meet without a hop.
///
/// Each socket owns one box. It holds a send queue (the application appends, the loop empties it and
/// routes the messages), a pool of send credits (how many more messages the application may append
/// without waiting), and a receive queue (the loop fills it, the application pops from it). Both sides
/// work under one `Mutex`, and the lock is held only to move items in or out of a queue or to update
/// a counter. Nothing is resumed, no NIO call is made and no `loop.execute` is issued while it is held;
/// callers get back what they need to do that afterwards (for example "wake the loop").
///
/// The box is plain `Sendable`: `Mutex<State>` is `Sendable` because `State` is a value of `Sendable`
/// parts, so no `@unchecked` is needed. See the top of `SocketCore.swift` for how the fast path fits
/// with the slow path (`Waiter`, `loop.execute`).
final class SocketShared: Sendable {
    /// What `fastSend` decided.
    enum SendRoute {
        /// The message is in the send queue. `wake` is set when the loop has no wake-up task pending:
        /// the caller must then call `loop.execute { core.fastSendsQueued(upTo: wake) }` (see
        /// `takeSendQueue(upTo:into:)` for why the task covers only the messages queued so far).
        case queued(wake: Int?)
        /// The socket is closing or closed.
        case closed
        /// No credit, or an earlier send is waiting on the slow path. The caller is now counted as a
        /// slow-path sender and must call `slowSendFinished()` when it is done.
        case slow
    }

    /// What `fastReceive` found.
    enum ReceiveRoute {
        /// A message. `refill` is true when the queue just fell to half its limit and the loop has not
        /// been asked to refill it yet: the caller must call `loop.execute` (once) to do so.
        case message([ByteBuffer], refill: Bool)
        /// Nothing in the queue (or the socket is closed): take the slow path.
        case slow
    }

    private struct State {
        var isClosed = false
        /// Messages the application has accepted and the loop has not routed yet, oldest first.
        var sendQueue = CircularBuffer<[ByteBuffer]>()
        /// How many more messages may be appended without waiting. Recomputed by the loop.
        var sendCredits = 0
        /// Senders that are on the slow path (waiting for room, or on their way to the loop to wait).
        /// While there is one, nobody uses the fast path, so nobody overtakes it.
        var slowSenders = 0
        /// How many messages were ever appended to the send queue, and how many the loop has taken.
        /// The queue holds the difference.
        var sendAppended = 0
        var sendTaken = 0
        /// A wake-up task for the loop is pending (or running and about to schedule a follow-up).
        /// Invariant: the send queue is not empty only while this is set.
        var sendWakePending = false
        /// Messages the loop has moved out of the pipes for `receive()`, oldest first.
        var receiveQueue = CircularBuffer<[ByteBuffer]>()
        /// The loop has been asked to refill the receive queue and has not started yet.
        var refillPending = false
    }

    private let state = Mutex(State())

    /// The most messages the receive queue is meant to hold; 0 when `receive()` does not use the queue
    /// (REQ, REP, and every socket with `conflate`).
    let receiveLimit: Int

    init(receiveLimit: Int) {
        self.receiveLimit = receiveLimit
    }

    // MARK: - Application side

    /// `send`: takes a credit and queues `frames`, or says why that was not possible.
    func fastSend(_ frames: [ByteBuffer]) -> SendRoute {
        state.withLock { state in
            if state.isClosed { return .closed }
            guard state.slowSenders == 0, state.sendCredits > 0 else {
                state.slowSenders += 1
                return .slow
            }
            state.sendCredits -= 1
            state.sendQueue.append(frames)
            state.sendAppended += 1
            if state.sendWakePending { return .queued(wake: nil) }
            state.sendWakePending = true
            return .queued(wake: state.sendAppended)
        }
    }

    /// A slow-path `send` ended (it sent, failed or was cancelled).
    func slowSendFinished() {
        state.withLock { $0.slowSenders -= 1 }
    }

    /// `receive`: pops the oldest message of the receive queue.
    func fastReceive() -> ReceiveRoute {
        let limit = receiveLimit
        return state.withLock { state in
            guard !state.isClosed, let message = state.receiveQueue.popFirst() else { return .slow }
            if !state.refillPending, state.receiveQueue.count <= limit / 2 {
                state.refillPending = true
                return .message(message, refill: true)
            }
            return .message(message, refill: false)
        }
    }

    // MARK: - Event-loop side

    /// The wake-up task of the loop takes the messages that were queued when the task was created
    /// (the first `target - taken` ones) and appends them to `buffer`. It does not take later ones even
    /// though they are there: whatever was put on the loop's task queue before such a message was
    /// appended (a subscription the peer sent, say) must run before the message is routed, and it may
    /// sit behind this task. If messages are left, the result says how far a follow-up task, which the
    /// caller enqueues from the loop, will reach: that task is behind everything queued before those
    /// messages were appended. Nil when the queue is empty (no task is pending then).
    func takeSendQueue(upTo target: Int, into buffer: inout CircularBuffer<[ByteBuffer]>) -> Int? {
        state.withLock { state in
            let count = max(0, min(target - state.sendTaken, state.sendQueue.count))
            if count == state.sendQueue.count {
                swap(&state.sendQueue, &buffer)
            } else {
                for _ in 0..<count { buffer.append(state.sendQueue.removeFirst()) }
            }
            state.sendTaken += count
            guard state.sendQueue.isEmpty else { return state.sendAppended }
            state.sendWakePending = false
            return nil
        }
    }

    /// Takes everything (swapping the queue with `buffer`, which should be empty, so its storage gets
    /// reused). Only for a caller whose own task was queued after all these messages were appended:
    /// a slow-path sender (which counts as one, so no message is appended after it decided to wait).
    func takeAllSendQueue(into buffer: inout CircularBuffer<[ByteBuffer]>) {
        state.withLock { state in
            swap(&state.sendQueue, &buffer)
            state.sendTaken = state.sendAppended
            state.sendWakePending = false
        }
    }

    /// Sets the credits to what the type can take now (`capacity`), less what is already on its way
    /// (the send queue and the loop's stalled messages).
    func setSendCredits(capacity: Int, stalled: Int) {
        state.withLock { state in
            state.sendCredits = max(0, capacity - state.sendQueue.count - stalled)
        }
    }

    /// `close()`: from now on `send` throws `socketClosed` and `receive` takes the slow path (which
    /// throws too). Hands over the messages still in the send queue and drops the receive queue.
    func close(takingSendQueueInto buffer: inout CircularBuffer<[ByteBuffer]>) {
        state.withLock { state in
            state.isClosed = true
            state.sendCredits = 0
            swap(&state.sendQueue, &buffer)
            state.sendTaken = state.sendAppended
            state.sendWakePending = false
            state.receiveQueue.removeAll()
        }
    }

    /// Pops the oldest message of the receive queue (from the loop, for a call on the slow path).
    func popReceived() -> [ByteBuffer]? {
        state.withLock { $0.receiveQueue.popFirst() }
    }

    /// How many messages the loop may add to the receive queue now. Also forgets that a refill was
    /// asked for: the loop is doing it.
    func takeReceiveRoom() -> Int {
        let limit = receiveLimit
        return state.withLock { state in
            state.refillPending = false
            return state.isClosed ? 0 : max(0, limit - state.receiveQueue.count)
        }
    }

    func appendReceived(_ messages: [[ByteBuffer]]) {
        state.withLock { state in
            guard !state.isClosed else { return }
            state.receiveQueue.append(contentsOf: messages)
        }
    }
}
