import NIOCore
import NIOPosix

/// Owns the threads that run sockets. Each socket picks one event loop of the context and does all its
/// work there. Most programs use `ZMQContext.default`.
public final class ZMQContext: Sendable {
    /// A context on the shared NIO event loop group.
    public static let `default` = ZMQContext()

    let eventLoopGroup: any EventLoopGroup

    /// A context on the shared NIO event loop group.
    public init() {
        eventLoopGroup = MultiThreadedEventLoopGroup.singleton
    }

    /// A context that runs its sockets on `eventLoopGroup`. The group must outlive every socket.
    public init(eventLoopGroup: any EventLoopGroup) {
        self.eventLoopGroup = eventLoopGroup
    }
}
