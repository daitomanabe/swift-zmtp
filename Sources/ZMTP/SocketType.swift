/// A ZeroMQ socket type, as named in the Socket-Type property of READY.
public enum SocketType: String, Sendable, CaseIterable {
    case pair = "PAIR", pub = "PUB", sub = "SUB", req = "REQ", rep = "REP"
    case dealer = "DEALER", router = "ROUTER", pull = "PULL", push = "PUSH"
    case xpub = "XPUB", xsub = "XSUB"

    /// Whether a socket of this type may talk to a socket of `peer` type (23/ZMTP, same table as libzmq).
    public func isCompatible(with peer: SocketType) -> Bool {
        switch self {
        case .req: peer == .rep || peer == .router
        case .rep: peer == .req || peer == .dealer
        case .dealer: peer == .rep || peer == .dealer || peer == .router
        case .router: peer == .req || peer == .dealer || peer == .router
        case .pub: peer == .sub || peer == .xsub
        case .xpub: peer == .sub || peer == .xsub
        case .sub: peer == .pub || peer == .xpub
        case .xsub: peer == .pub || peer == .xpub
        case .push: peer == .pull
        case .pull: peer == .push
        case .pair: peer == .pair
        }
    }

    /// READY carries an Identity property (libzmq does this for REQ, DEALER, ROUTER, even when empty).
    public var sendsIdentity: Bool {
        switch self {
        case .req, .dealer, .router: true
        default: false
        }
    }
}
