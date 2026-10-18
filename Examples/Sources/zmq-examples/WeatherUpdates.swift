import ZMQ

/// Publish/subscribe: weather updates, with a filter.
///
/// A publisher (PUB) sends messages to everybody who is connected and asked for them; it never waits for
/// anybody and never hears back. A subscriber (SUB) receives nothing until it subscribes, and then only
/// the messages whose FIRST FRAME STARTS WITH the subscribed bytes. That is why a message that is
/// meant to be filtered has the topic in a frame of its own, in front of the data.
///
///     zmq-examples wuserver                  in one terminal
///     zmq-examples wuclient 10003            in another: only the updates for zip code 10003
enum WeatherUpdates {
    static let defaultEndpoint = "tcp://127.0.0.1:5556"

    /// `wuserver [endpoint]`: publishes updates for ten zip codes, one every 10 ms, until its time limit.
    static func server(_ arguments: Arguments) async throws {
        let endpoint = arguments.endpoint(default: defaultEndpoint)

        let pub = PubSocket()
        let bound = try await pub.bind(endpoint)
        print("wuserver: publishing on \(bound)")

        while true {
            let zipCode = Int.random(in: 10001...10010)
            let temperature = Int.random(in: -20...45)
            let humidity = Int.random(in: 10...90)
            // Two frames: the topic, then the data. A subscriber to "10003" gets the whole message.
            try await pub.send(["\(zipCode)", "\(temperature) \(humidity)"])
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    /// `wuclient [endpoint] [zipcode] [--count N]`: averages the temperature of N updates (default 10).
    static func client(_ arguments: Arguments) async throws {
        let endpoint = arguments.endpoint(default: defaultEndpoint)
        let zipCode = arguments.positional.dropFirst().first ?? "10001"
        let count = try arguments.int("count", default: 10)

        let sub = SubSocket()
        // A SUB socket gets nothing until it subscribes. This may come before connect().
        try await sub.subscribe(Frame(zipCode))
        try await sub.connect(endpoint)

        var total = 0
        for _ in 0..<count {
            let update = try await sub.receive()
            let topic = update.frames[0].string ?? ""
            // The second frame is "temperature humidity". A message from another publisher may have only one frame.
            let data = update.frames.dropFirst().first?.string ?? ""
            let temperature = data.split(separator: " ").first.flatMap { Int($0) } ?? 0
            print("wuclient: \(topic) \(temperature) C")
            total += temperature
        }
        if count > 0 {
            print("wuclient: average temperature for zip code \(zipCode) was \(total / count) C")
        }
        await sub.close()
    }
}
