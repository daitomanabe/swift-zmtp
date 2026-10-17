import NIOCore

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

extension NIOBSDSocket.Option {
    /// How long a TCP connection must be idle before keepalive probes start: `TCP_KEEPALIVE` on Darwin,
    /// `TCP_KEEPIDLE` on Linux. Takes seconds.
    static var tcpKeepAliveIdle: NIOBSDSocket.Option {
        #if canImport(Darwin)
        NIOBSDSocket.Option(rawValue: TCP_KEEPALIVE)
        #else
        NIOBSDSocket.Option(rawValue: TCP_KEEPIDLE)
        #endif
    }
}

extension SocketOptions {
    /// `tcpKeepAlive` as the idle time for the socket option: whole seconds, at least 1. nil when keepalive
    /// is not asked for.
    var tcpKeepAliveIdleSeconds: Int32? {
        guard let idle = tcpKeepAlive else { return nil }
        return Int32(clamping: max(idle.components.seconds, 1))
    }
}
