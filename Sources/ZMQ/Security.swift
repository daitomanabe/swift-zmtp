import ZMTP

/// A user name and password as a PLAIN client sent them.
public struct PlainCredentials: Sendable, Hashable {
    public var username: String
    public var password: String

    public init(username: String, password: String) {
        self.username = username
        self.password = password
    }
}

/// How a socket authenticates its connections (libzmq: the `ZMQ_PLAIN_*` options). Set with
/// `SocketOptions.security`; it applies to every `tcp://` and `ipc://` connection the socket makes or
/// accepts, whichever side calls `bind` or `connect`. Both ends of a connection must use the same
/// mechanism, or the handshake fails. `inproc://` connections ignore it.
///
/// PLAIN sends the password in the clear; use it only on a trusted network or under another layer of
/// protection.
public enum Security: Sendable {
    /// No authentication (the NULL mechanism). The default.
    case none
    /// PLAIN, client side: send this user name and password after connecting. Each may be at most 255
    /// bytes in UTF-8; `bind` and `connect` throw `ZMQError.invalidState` for longer ones.
    case plainClient(username: String, password: String)
    /// PLAIN, server side: call `authenticate` with the credentials of each connecting client. `false`
    /// refuses the client: it gets ERROR "400" and the connection closes (the client sees
    /// `SocketEvent.handshakeFailed`). The call may take a while; it counts against `handshakeTimeout`.
    /// A PLAIN server needs this function, like libzmq needs an authenticator for it.
    case plainServer(authenticate: @Sendable (PlainCredentials) async -> Bool)

    /// PLAIN, server side, with a fixed list of accounts (user name to password).
    public static func plainServer(accepting accounts: [String: String]) -> Security {
        .plainServer { credentials in
            guard let password = accounts[credentials.username] else { return false }
            return constantTimeEqual(Array(password.utf8), Array(credentials.password.utf8))
        }
    }
}

extension Security {
    /// The mechanism the ZMTP core runs for this setting.
    var mechanism: MechanismConfiguration {
        switch self {
        case .none: .null
        case .plainClient(let username, let password): .plainClient(username: username, password: password)
        case .plainServer: .plainServer
        }
    }

    /// Checks the settings that are not known to be wrong until a connection is made.
    func validate() throws(ZMQError) {
        guard case .plainClient(let username, let password) = self else { return }
        if username.utf8.count > 255 {
            throw .invalidState("the PLAIN user name is longer than 255 bytes")
        }
        if password.utf8.count > 255 {
            throw .invalidState("the PLAIN password is longer than 255 bytes")
        }
    }

    /// The function that decides about PLAIN clients (nil unless this is a PLAIN server).
    var plainAuthenticator: (@Sendable (PlainCredentials) async -> Bool)? {
        if case .plainServer(let authenticate) = self { return authenticate }
        return nil
    }
}

/// Compares two byte strings without stopping at the first difference.
private func constantTimeEqual(_ a: [UInt8], _ b: [UInt8]) -> Bool {
    var difference = a.count ^ b.count
    for index in 0..<max(a.count, b.count) {
        let x = index < a.count ? a[index] : 0
        let y = index < b.count ? b[index] : 0
        difference |= Int(x ^ y)
    }
    return difference == 0
}
