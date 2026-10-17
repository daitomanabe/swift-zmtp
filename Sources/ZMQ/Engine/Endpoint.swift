/// Parsing of `tcp://HOST:PORT` endpoints.
enum Endpoint {
    struct TCP {
        var host: String
        /// 0 means "pick a free port" (bind only).
        var port: Int
    }

    /// `tcp://IPv4:PORT`, `tcp://*:PORT`; PORT is a number or `*`.
    static func tcpForBind(_ endpoint: String) throws -> TCP {
        let (host, portText) = try split(endpoint)
        let address: String
        if host == "*" {
            address = "0.0.0.0"
        } else if isIPv4(host) {
            address = host
        } else {
            throw ZMQError.invalidEndpoint("\(endpoint): bind needs an IPv4 address or *")
        }
        if portText == "*" { return TCP(host: address, port: 0) }
        guard let port = Int(portText), (0...65535).contains(port) else {
            throw ZMQError.invalidEndpoint("\(endpoint): bad port")
        }
        return TCP(host: address, port: port)
    }

    /// `tcp://HOST:PORT` with a host name, an IPv4 address or an IPv6 address in brackets.
    static func tcpForConnect(_ endpoint: String) throws -> TCP {
        let (host, portText) = try split(endpoint)
        guard host != "*", !host.contains(";"), !host.contains(where: \.isWhitespace) else {
            throw ZMQError.invalidEndpoint("\(endpoint): bad host")
        }
        guard let port = Int(portText), (1...65535).contains(port) else {
            throw ZMQError.invalidEndpoint("\(endpoint): bad port")
        }
        return TCP(host: host, port: port)
    }

    /// Checks the scheme and splits the rest into host and port text.
    private static func split(_ endpoint: String) throws -> (host: String, port: String) {
        guard let schemeEnd = endpoint.firstRange(of: "://") else {
            throw ZMQError.invalidEndpoint("\(endpoint): expected scheme://address")
        }
        let scheme = endpoint[..<schemeEnd.lowerBound]
        let address = endpoint[schemeEnd.upperBound...]
        switch scheme {
        case "tcp":
            break
        case "ipc", "inproc":
            throw ZMQError.unsupportedTransport(endpoint)
        default:
            throw ZMQError.invalidEndpoint("\(endpoint): unknown transport \(scheme)")
        }
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
}
