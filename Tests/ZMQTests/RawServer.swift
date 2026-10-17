import NIOCore
import NIOPosix
import Synchronization
import ZMTP

/// A TCP listener on 127.0.0.1 that plays a ZMTP peer badly, for the tests of timeouts.
final class RawServer: Sendable {
    enum Behavior: Sendable {
        /// Accepts connections and never sends a byte.
        case silent
        /// Completes the greeting and READY as a PULL socket, then goes quiet: no PONGs, nothing.
        case silentAfterReady
        /// Like `silentAfterReady`, but sends one PING with this TTL (tenths of a second) right after READY.
        case pingOnceThenSilent(ttl: UInt16)

        var completesHandshake: Bool {
            if case .silent = self { return false }
            return true
        }
    }

    /// The TTLs (tenths of a second) of the PINGs the server received, in order.
    final class PingLog: Sendable {
        private let ttls = Mutex<[UInt16]>([])
        func add(_ ttl: UInt16) { ttls.withLock { $0.append(ttl) } }
        var all: [UInt16] { ttls.withLock { $0 } }
    }

    let endpoint: String
    let pings: PingLog
    private let channel: any Channel

    static func start(_ behavior: Behavior) async throws -> RawServer {
        let pings = PingLog()
        let channel = try await ServerBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(PeerHandler(behavior: behavior, pings: pings))
                }
            }
            .bind(host: "127.0.0.1", port: 0)
            .get()
        guard let port = channel.localAddress?.port else { throw TestTimeout() }
        return RawServer(endpoint: "tcp://127.0.0.1:\(port)", pings: pings, channel: channel)
    }

    private init(endpoint: String, pings: PingLog, channel: any Channel) {
        self.endpoint = endpoint
        self.pings = pings
        self.channel = channel
    }

    func stop() async {
        try? await channel.close()
    }
}

private final class PeerHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let behavior: RawServer.Behavior
    private let pings: RawServer.PingLog
    private var connection = ZMTPConnection(configuration: .init(socketType: .pull))
    /// After READY nothing more is written.
    private var quiet = false

    init(behavior: RawServer.Behavior, pings: RawServer.PingLog) {
        self.behavior = behavior
        self.pings = pings
    }

    func channelActive(context: ChannelHandlerContext) {
        guard behavior.completesHandshake else { return }
        for case .write(let buffer) in connection.start() {
            context.writeAndFlush(wrapOutboundOut(buffer), promise: nil)
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard behavior.completesHandshake else { return }
        for output in connection.receive(unwrapInboundIn(data)) {
            switch output {
            case .write(let buffer):
                if !quiet { context.writeAndFlush(wrapOutboundOut(buffer), promise: nil) }
            case .ready:
                quiet = true
                if case .pingOnceThenSilent(let ttl) = behavior {
                    var buffer = ByteBuffer()
                    connection.encodePing(ttl: ttl, context: ByteBuffer(), into: &buffer)
                    context.writeAndFlush(wrapOutboundOut(buffer), promise: nil)
                }
            case .pingReceived(let ttl):
                pings.add(ttl)
            case .closed:
                context.close(promise: nil)
            case .message, .subscribe, .cancel:
                break
            }
        }
    }
}
