import ZMQ

/// A forwarder between publishers and subscribers: the "weather update proxy" of the ZeroMQ Guide.
///
/// Subscribers connect to the proxy instead of to the publisher. The proxy has an XSUB socket that connects to the
/// publisher and an XPUB socket that the subscribers connect to. XPUB tells the proxy which subscriptions its
/// subscribers have, and `ZMQProxy.run` passes them on to XSUB, so the publisher only sends what somebody wants.
///
///     zmq-examples wuserver                              publishes on 5556
///     zmq-examples wuproxy                               forwards 5556 to 5559
///     zmq-examples wuclient tcp://127.0.0.1:5559         subscribes through the proxy
enum WeatherProxy {
    static let defaultEndpoint = "tcp://127.0.0.1:5559"

    /// `wuproxy [endpoint] [--publisher ENDPOINT]`: `endpoint` is where the subscribers connect.
    static func run(_ arguments: Arguments) async throws {
        let subscribersEndpoint = arguments.endpoint(default: defaultEndpoint)
        let publisherEndpoint = arguments.string("publisher") ?? WeatherUpdates.defaultEndpoint

        let frontend = XSubSocket()
        try await frontend.connect(publisherEndpoint)
        let backend = XPubSocket()
        let bound = try await backend.bind(subscribersEndpoint)
        print("wuproxy: forwarding \(publisherEndpoint) to \(bound)")

        // Returns when one of the sockets is closed, and throws CancellationError when the task is cancelled.
        try await ZMQProxy.run(frontend: frontend, backend: backend)
    }
}
