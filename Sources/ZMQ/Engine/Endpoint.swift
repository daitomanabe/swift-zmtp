import Foundation

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

/// Parsing of `tcp://HOST:PORT` and `ipc://PATH` endpoints.
enum Endpoint {
    struct TCP {
        var host: String
        /// 0 means "pick a free port" (bind only).
        var port: Int
    }

    /// Where `bind` listens.
    enum BindTarget {
        case tcp(TCP)
        /// A Unix domain socket path (for `ipc://*`, a fresh one in the temporary directory).
        case ipc(path: String)
    }

    /// Where `connect` goes.
    enum ConnectTarget {
        case tcp(host: String, port: Int)
        case ipc(path: String)
    }

    /// The longest path a Unix domain socket address can hold: 103 bytes on macOS, which is also the
    /// limit NIO enforces on every platform.
    static let maxIPCPathLength: Int = {
        let address = sockaddr_un()
        return min(MemoryLayout.size(ofValue: address.sun_path) - 1, 103)
    }()

    /// `tcp://IPv4:PORT`, `tcp://*:PORT` (PORT is a number or `*`), `ipc://PATH` or `ipc://*`.
    static func forBind(_ endpoint: String) throws -> BindTarget {
        let (scheme, address) = try splitScheme(endpoint)
        switch scheme {
        case "tcp":
            return .tcp(try tcpForBind(endpoint, address))
        case "ipc":
            if address == "*" { return .ipc(path: uniqueIPCPath()) }
            return .ipc(path: try ipcPath(endpoint, address))
        default:
            throw unsupportedOrUnknown(endpoint, scheme)
        }
    }

    /// `tcp://HOST:PORT` with a host name, an IPv4 address or an IPv6 address in brackets, or `ipc://PATH`.
    static func forConnect(_ endpoint: String) throws -> ConnectTarget {
        let (scheme, address) = try splitScheme(endpoint)
        switch scheme {
        case "tcp":
            let tcp = try tcpForConnect(endpoint, address)
            return .tcp(host: tcp.host, port: tcp.port)
        case "ipc":
            return .ipc(path: try ipcPath(endpoint, address))
        default:
            throw unsupportedOrUnknown(endpoint, scheme)
        }
    }

    // MARK: - TCP

    private static func tcpForBind(_ endpoint: String, _ address: Substring) throws -> TCP {
        let (host, portText) = try splitHostPort(endpoint, address)
        let ip: String
        if host == "*" {
            ip = "0.0.0.0"
        } else if isIPv4(host) {
            ip = host
        } else {
            throw ZMQError.invalidEndpoint("\(endpoint): bind needs an IPv4 address or *")
        }
        if portText == "*" { return TCP(host: ip, port: 0) }
        guard let port = Int(portText), (0...65535).contains(port) else {
            throw ZMQError.invalidEndpoint("\(endpoint): bad port")
        }
        return TCP(host: ip, port: port)
    }

    private static func tcpForConnect(_ endpoint: String, _ address: Substring) throws -> TCP {
        let (host, portText) = try splitHostPort(endpoint, address)
        guard host != "*", !host.contains(";"), !host.contains(where: \.isWhitespace) else {
            throw ZMQError.invalidEndpoint("\(endpoint): bad host")
        }
        guard let port = Int(portText), (1...65535).contains(port) else {
            throw ZMQError.invalidEndpoint("\(endpoint): bad port")
        }
        return TCP(host: host, port: port)
    }

    private static func splitHostPort(_ endpoint: String, _ address: Substring) throws -> (host: String, port: String) {
        guard let colon = address.lastIndex(of: ":") else {
            throw ZMQError.invalidEndpoint("\(endpoint): missing port")
        }
        var host = address[..<colon]
        let port = address[address.index(after: colon)...]
        if host.hasPrefix("["), host.hasSuffix("]") {
            host = host.dropFirst().dropLast()
        }
        guard !host.isEmpty, !port.isEmpty else {
            throw ZMQError.invalidEndpoint("\(endpoint): missing host or port")
        }
        return (String(host), String(port))
    }

    private static func isIPv4(_ text: String) -> Bool {
        let parts = text.split(separator: ".", omittingEmptySubsequences: false)
        return parts.count == 4 && parts.allSatisfy { !$0.isEmpty && $0.allSatisfy(\.isASCII) && UInt8($0) != nil }
    }

    // MARK: - IPC

    private static func ipcPath(_ endpoint: String, _ address: Substring) throws -> String {
        guard !address.isEmpty else { throw ZMQError.invalidEndpoint("\(endpoint): empty path") }
        let path = String(address)
        guard !path.utf8.contains(0) else { throw ZMQError.invalidEndpoint("\(endpoint): path contains NUL") }
        guard path.utf8.count <= maxIPCPathLength else {
            throw ZMQError.invalidEndpoint(
                "\(endpoint): path is \(path.utf8.count) bytes, at most \(maxIPCPathLength) fit in a socket address")
        }
        return path
    }

    /// A path in the temporary directory that no file uses now (for `ipc://*`).
    private static func uniqueIPCPath() -> String {
        var directory = NSTemporaryDirectory()
        // The system temporary directory can be long on macOS; a socket address holds ~100 bytes.
        if directory.utf8.count + "zmq-0123456789abcdef".utf8.count + 1 > maxIPCPathLength { directory = "/tmp" }
        if !directory.hasSuffix("/") { directory += "/" }
        var path = directory
        repeat {
            path = directory + "zmq-" + String(UInt64.random(in: 0...UInt64.max), radix: 16)
        } while FileManager.default.fileExists(atPath: path)
        return path
    }

    /// Removes a socket file or a plain file that a previous run left at `path`, so that `bind` can
    /// create the socket. Directories, links and other kinds of file stay.
    static func removeStaleFile(at path: String) {
        var info = stat()
        guard lstat(path, &info) == 0 else { return }
        let kind = info.st_mode & mode_t(S_IFMT)
        guard kind == mode_t(S_IFSOCK) || kind == mode_t(S_IFREG) else { return }
        unlink(path)
    }

    /// Identifies the socket file a bind created, so `close` removes only that file and not one that
    /// another socket put at the same path afterwards.
    struct FileIdentity: Equatable {
        var device: UInt64
        var inode: UInt64
    }

    static func socketFileIdentity(at path: String) -> FileIdentity? {
        var info = stat()
        guard lstat(path, &info) == 0, info.st_mode & mode_t(S_IFMT) == mode_t(S_IFSOCK) else { return nil }
        return FileIdentity(device: UInt64(truncatingIfNeeded: info.st_dev), inode: UInt64(info.st_ino))
    }

    /// Removes the socket file at `path` if it is still the file with `identity`.
    static func removeSocketFile(at path: String, ifIdentity identity: FileIdentity?) {
        guard let identity, socketFileIdentity(at: path) == identity else { return }
        unlink(path)
    }

    // MARK: - Shared

    /// Checks the scheme and returns the rest of the endpoint.
    private static func splitScheme(_ endpoint: String) throws -> (scheme: Substring, address: Substring) {
        guard let schemeEnd = endpoint.firstRange(of: "://") else {
            throw ZMQError.invalidEndpoint("\(endpoint): expected scheme://address")
        }
        return (endpoint[..<schemeEnd.lowerBound], endpoint[schemeEnd.upperBound...])
    }

    private static func unsupportedOrUnknown(_ endpoint: String, _ scheme: Substring) -> ZMQError {
        scheme == "inproc"
            ? .unsupportedTransport(endpoint)
            : .invalidEndpoint("\(endpoint): unknown transport \(scheme)")
    }
}
