/// The words after the subcommand: values (an endpoint, a key) and `--name value` options.
struct Arguments: Sendable {
    private(set) var positional: [String] = []
    private var options: [String: [String]] = [:]

    /// Splits `words`. `allowed` is the list of option names the command understands. After a lone `--`
    /// every word is a value, because a CURVE key (Z85 text) can start with `--`.
    init(_ words: [String], allowed: Set<String>, positional count: ClosedRange<Int>) throws {
        var iterator = words.makeIterator()
        var optionsEnded = false
        while let word = iterator.next() {
            if word == "--", !optionsEnded {
                optionsEnded = true
                continue
            }
            guard word.hasPrefix("--"), !optionsEnded else {
                positional.append(word)
                continue
            }
            let name = String(word.dropFirst(2))
            guard allowed.contains(name) else { throw UsageError("unknown option \(word)") }
            guard let value = iterator.next() else { throw UsageError("\(word) needs a value") }
            options[name, default: []].append(value)
        }
        guard count.contains(positional.count) else {
            throw UsageError("expected \(count.lowerBound == count.upperBound ? "\(count.lowerBound)" : "\(count.lowerBound) to \(count.upperBound)") argument(s) without a name, got \(positional.count)")
        }
    }

    /// The value of `--name`, or nil. When the option is repeated: the last one.
    func string(_ name: String) -> String? { options[name]?.last }

    /// All values of a repeated option, in order.
    func strings(_ name: String) -> [String] { options[name] ?? [] }

    /// The value of `--name` as a whole number (0 or more).
    func int(_ name: String, default fallback: Int) throws -> Int {
        guard let text = string(name) else { return fallback }
        guard let number = Int(text), number >= 0 else {
            throw UsageError("--\(name) needs a whole number of 0 or more, not '\(text)'")
        }
        return number
    }

    /// The first value without a name, which every command that binds or connects takes as its endpoint.
    func endpoint(default fallback: String) -> String { positional.first ?? fallback }
}
