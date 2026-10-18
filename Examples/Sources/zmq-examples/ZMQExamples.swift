import Foundation

/// One subcommand of `zmq-examples`.
struct Command: Sendable {
    let name: String
    let summary: String
    /// Shown in the usage text, after the name.
    let usage: String
    /// How many values without a name the command takes.
    let positional: ClosedRange<Int>
    /// The `--name value` options of the command (`--seconds` is understood by all of them).
    let options: Set<String>
    /// The time limit in seconds, unless `--seconds` says otherwise. 0: no limit.
    let defaultSeconds: Int
    /// A server or publisher is meant to run until it is stopped, so reaching the limit is its normal end.
    /// A client that is still waiting at the limit did not get what it came for: that is a failure.
    let endsAtLimit: @Sendable (Arguments) -> Bool
    let run: @Sendable (Arguments) async throws -> Void
}

let commands: [Command] = [
    Command(
        name: "hwserver", summary: "REQ/REP hello world: answers every request with \"World\"",
        usage: "[endpoint] [--count N]", positional: 0...1, options: ["count"],
        defaultSeconds: 60, endsAtLimit: { _ in true }, run: HelloWorld.server),
    Command(
        name: "hwclient", summary: "REQ/REP hello world: sends \"Hello\" and prints the answers",
        usage: "[endpoint] [--count N]", positional: 0...1, options: ["count"],
        defaultSeconds: 20, endsAtLimit: { _ in false }, run: HelloWorld.client),
    Command(
        name: "wuserver", summary: "PUB/SUB weather updates: publishes about 100 updates a second",
        usage: "[endpoint]", positional: 0...1, options: [],
        defaultSeconds: 60, endsAtLimit: { _ in true }, run: WeatherUpdates.server),
    Command(
        name: "wuclient", summary: "PUB/SUB weather updates: subscribes to one zip code",
        usage: "[endpoint] [zipcode] [--count N]", positional: 0...2, options: ["count"],
        defaultSeconds: 20, endsAtLimit: { _ in false }, run: WeatherUpdates.client),
    Command(
        name: "wuproxy", summary: "XSUB/XPUB with ZMQProxy: forwards weather updates between publisher and subscribers",
        usage: "[endpoint] [--publisher ENDPOINT]", positional: 0...1, options: ["publisher"],
        defaultSeconds: 60, endsAtLimit: { _ in true }, run: WeatherProxy.run),
    Command(
        name: "taskvent", summary: "PUSH/PULL pipeline: the ventilator hands out tasks",
        usage: "[endpoint] [--sink ENDPOINT] [--tasks N] [--workers N]", positional: 0...1,
        options: ["sink", "tasks", "workers"],
        defaultSeconds: 60, endsAtLimit: { _ in false }, run: Pipeline.ventilator),
    Command(
        name: "taskwork", summary: "PUSH/PULL pipeline: a worker does tasks and reports to the sink",
        usage: "[--vent ENDPOINT] [--sink ENDPOINT] [--idle SECONDS]", positional: 0...0,
        options: ["vent", "sink", "idle"],
        defaultSeconds: 60, endsAtLimit: { _ in true }, run: Pipeline.worker),
    Command(
        name: "tasksink", summary: "PUSH/PULL pipeline: the sink collects the results",
        usage: "[endpoint] [--tasks N]", positional: 0...1, options: ["tasks"],
        defaultSeconds: 60, endsAtLimit: { _ in false }, run: Pipeline.sink),
    Command(
        name: "mspoller", summary: "reads from a PULL and a SUB socket in one loop (a task group)",
        usage: "[--vent ENDPOINT] [--weather ENDPOINT] [--zipcode ZIP] [--count N]", positional: 0...0,
        options: ["vent", "weather", "zipcode", "count"],
        defaultSeconds: 30, endsAtLimit: { _ in false }, run: MultiSocketReader.run),
    Command(
        name: "mtserver", summary: "ROUTER/DEALER with ZMQProxy: worker tasks and clients in one process",
        usage: "[endpoint] [--workers N] [--clients N] [--count N]", positional: 0...1,
        options: ["workers", "clients", "count"],
        // With --clients 0 it only serves, and the time limit is its end.
        defaultSeconds: 20, endsAtLimit: { (try? $0.int("clients", default: 3)) == 0 }, run: MultithreadedServer.run),
    Command(
        name: "curve-keygen", summary: "prints a new CURVE key pair",
        usage: "", positional: 0...0, options: [],
        defaultSeconds: 5, endsAtLimit: { _ in false }, run: CurveExample.keygen),
    Command(
        name: "curve-server", summary: "CURVE-secured REP: prints its public key, can limit the clients",
        usage: "[endpoint] [--secret-key KEY] [--allow CLIENT_KEY]... [--count N]", positional: 0...1,
        options: ["secret-key", "allow", "count"],
        defaultSeconds: 60, endsAtLimit: { _ in true }, run: CurveExample.server),
    Command(
        name: "curve-client", summary: "CURVE-secured REQ: needs the server's public key",
        usage: "SERVER_KEY [endpoint] [--secret-key KEY] [--count N]", positional: 1...2,
        options: ["secret-key", "count"],
        defaultSeconds: 30, endsAtLimit: { _ in false }, run: CurveExample.client),
    Command(
        name: "plain", summary: "PLAIN user name and password: one accepted login, one refused (one process)",
        usage: "", positional: 0...0, options: [],
        defaultSeconds: 30, endsAtLimit: { _ in false }, run: PlainExample.run),
    Command(
        name: "frames", summary: "messages and frames: text, bytes, empty frames (one process, PAIR over inproc)",
        usage: "", positional: 0...0, options: [],
        defaultSeconds: 10, endsAtLimit: { _ in false }, run: Frames.run),
    Command(
        name: "options", summary: "a tour of SocketOptions: high-water mark, conflate, immediate, routerMandatory",
        usage: "", positional: 0...0, options: [],
        defaultSeconds: 30, endsAtLimit: { _ in false }, run: OptionsTour.run),
    Command(
        name: "events", summary: "prints SocketEvents: connect where nothing listens, then a peer appears",
        usage: "[endpoint]", positional: 0...1, options: [],
        defaultSeconds: 30, endsAtLimit: { _ in false }, run: Events.run),
]

@main
enum ZMQExamples {
    static func main() async {
        // Lines should show up at once also when the output goes to a file or a pipe.
        setvbuf(stdout, nil, _IOLBF, 0)

        let words = Array(CommandLine.arguments.dropFirst())
        guard let name = words.first, name != "help", name != "--help" else {
            printUsage()
            exit(words.isEmpty ? 64 : 0)
        }
        guard let command = commands.first(where: { $0.name == name }) else {
            printError("zmq-examples: unknown command '\(name)'\n")
            printUsage()
            exit(64)
        }

        do {
            let arguments = try Arguments(
                Array(words.dropFirst()), allowed: command.options.union(["seconds"]),
                positional: command.positional)
            let seconds = try arguments.int("seconds", default: command.defaultSeconds)
            try await run(command, arguments, seconds: seconds)
        } catch let error as UsageError {
            printError("\(name): \(error)\nusage: zmq-examples \(name) \(command.usage) [--seconds S]")
            exit(64)
        } catch {
            printError("\(name): error: \(error)")
            exit(1)
        }
    }

    /// Runs the command; stops it after `seconds` (0: no limit) so that nothing is left running.
    static func run(_ command: Command, _ arguments: Arguments, seconds: Int) async throws {
        let finished: Void? =
            seconds == 0
            ? try await command.run(arguments)
            : try await timeout(after: .seconds(seconds)) { try await command.run(arguments) }
        if finished == nil {
            if command.endsAtLimit(arguments) {
                print("\(command.name): stopped after its time limit of \(seconds) s")
            } else {
                throw ExampleError("gave up after the time limit of \(seconds) s")
            }
        }
    }

    static func printUsage() {
        print("usage: zmq-examples COMMAND [arguments]   (every command also takes --seconds S: its time limit, 0 for none)\n")
        for command in commands {
            print("  \(command.name) \(command.usage)".trimmingCharacters(in: .whitespaces))
            print("      \(command.summary)")
        }
    }
}
