import ZMQ

/// Parallel pipeline: a ventilator hands out tasks, workers do them, a sink collects the results.
///
/// PUSH sends each message to ONE of the PULL sockets connected to it, taking them in turn, so the work
/// is spread over the workers. PULL takes the messages of all the PUSH sockets connected to it. There are
/// three programs, and three kinds of connection:
///
///     taskvent (PUSH, binds 5557)  ->  taskwork (PULL, connects to 5557)
///     taskwork (PUSH, connects to 5558)  ->  tasksink (PULL, binds 5558)
///     taskvent (PUSH, connects to 5558)  ->  tasksink       (one message that says "start the clock")
///
/// Start the sink first, then as many workers as you like, then the ventilator:
///
///     zmq-examples tasksink
///     zmq-examples taskwork        (several times, in several terminals)
///     zmq-examples taskvent --workers 3
enum Pipeline {
    static let ventEndpoint = "tcp://127.0.0.1:5557"
    static let sinkEndpoint = "tcp://127.0.0.1:5558"

    /// `taskvent [endpoint] [--sink ENDPOINT] [--tasks N] [--workers N]`
    /// Waits until N workers (default 1) are connected, then sends N tasks (default 100). A task is a
    /// number: how many milliseconds of work it stands for.
    static func ventilator(_ arguments: Arguments) async throws {
        let bindEndpoint = arguments.endpoint(default: ventEndpoint)
        let sinkEndpoint = arguments.string("sink") ?? Pipeline.sinkEndpoint
        let taskCount = try arguments.int("tasks", default: 100)
        let workerCount = try arguments.int("workers", default: 1)

        let tasks = PushSocket()
        // Ask for the events BEFORE binding: a stream shows only what happens after the call.
        var events = tasks.events().makeAsyncIterator()
        let bound = try await tasks.bind(bindEndpoint)

        // The sink gets one message of its own to tell it that the batch starts.
        let sink = PushSocket()
        try await sink.connect(sinkEndpoint)

        // A PUSH socket gives the tasks to whoever is connected. If the first worker were alone for a
        // moment, it would get all of them, so wait until the workers have arrived.
        print("taskvent: waiting for \(workerCount) worker(s) on \(bound)")
        var workersReady = 0
        while workersReady < workerCount, let event = await events.next() {
            if case .handshakeSucceeded = event { workersReady += 1 }
        }

        try await sink.send("0")
        var totalMilliseconds = 0
        for _ in 0..<taskCount {
            let milliseconds = Int.random(in: 1...100)
            totalMilliseconds += milliseconds
            try await tasks.send("\(milliseconds)")
        }
        print("taskvent: sent \(taskCount) tasks, total expected cost \(totalMilliseconds) ms")

        // close() waits (up to the linger time, one second by default) until the queued messages are written.
        await tasks.close()
        await sink.close()
    }

    /// `taskwork [--vent ENDPOINT] [--sink ENDPOINT] [--idle SECONDS]`
    /// Takes tasks from the ventilator, "works" for as long as the task says, and reports to the sink.
    /// Stops when no task has arrived for SECONDS (default 5).
    static func worker(_ arguments: Arguments) async throws {
        let ventEndpoint = arguments.string("vent") ?? Pipeline.ventEndpoint
        let sinkEndpoint = arguments.string("sink") ?? Pipeline.sinkEndpoint
        let idle = try arguments.int("idle", default: 5)

        let tasks = PullSocket()
        try await tasks.connect(ventEndpoint)
        let results = PushSocket()
        try await results.connect(sinkEndpoint)
        print("taskwork: ready")

        var done = 0
        do {
            while true {
                // Throws ZMQError.timedOut when no task comes within `idle` seconds, and takes no message then.
                let task = try await tasks.receive(timeout: .seconds(idle))
                let milliseconds = Int(task.string ?? "") ?? 0
                try await Task.sleep(for: .milliseconds(milliseconds))   // the "work"
                try await results.send(Message(Frame()))                 // the result: one empty frame
                done += 1
            }
        } catch ZMQError.timedOut {
            print("taskwork: no task for \(idle) s, done \(done) tasks")
        }
        await tasks.close()
        await results.close()
    }

    /// `tasksink [endpoint] [--tasks N]`: collects N results (default 100) and prints how long it took.
    static func sink(_ arguments: Arguments) async throws {
        let endpoint = arguments.endpoint(default: sinkEndpoint)
        let taskCount = try arguments.int("tasks", default: 100)

        let results = PullSocket()
        let bound = try await results.bind(endpoint)
        print("tasksink: waiting for the start signal on \(bound)")

        _ = try await results.receive()   // the ventilator's "0"
        let start = ContinuousClock.now

        for number in stride(from: 1, through: taskCount, by: 1) {
            _ = try await results.receive()
            if number % 10 == 0 || number == taskCount { print("tasksink: \(number) of \(taskCount) results") }
        }
        let elapsed = start.duration(to: .now) / .milliseconds(1)
        print("tasksink: total elapsed time \(Int(elapsed)) ms")
        await results.close()
    }
}
