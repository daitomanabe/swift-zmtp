/// Which security mechanism a `ZMTPConnection` runs, and with what. The greeting announces the mechanism
/// by name; a peer that announces another one is closed with `ZMTPError.mechanismMismatch`.
///
/// Which side is the "server" of a mechanism is a matter of configuration, not of who connected: a
/// `plainServer` can connect and a `plainClient` can bind (like `PLAIN_SERVER` in libzmq).
public enum MechanismConfiguration: Sendable, Equatable {
    /// NULL (23/ZMTP): no authentication, no encryption.
    case null
    /// PLAIN (24/ZMTP-PLAIN), client side: sends the user name and password in HELLO. Both are sent as
    /// UTF-8 and may be at most 255 bytes each; longer ones close the connection at `start()` with
    /// `ZMTPError.invalidConfiguration`.
    case plainClient(username: String, password: String)
    /// PLAIN, server side: the connection asks the caller to check the credentials with
    /// `ZMTPConnection.Output.authenticate` and goes on with `completeAuthentication(allowed:)`.
    case plainServer
    // CURVE (25/ZMTP-CURVE) adds a client and a server case here; the server one also asks the caller
    // to authenticate, through a new `AuthenticationRequest` case.
}

/// What a server-side mechanism wants the caller to check before it goes on. Answer it with
/// `ZMTPConnection.completeAuthentication(allowed:)`.
public enum AuthenticationRequest: Sendable, Equatable {
    /// The credentials from a PLAIN client's HELLO (decoded as UTF-8, invalid bytes replaced).
    case plain(username: String, password: String)
}
