import NIOCore
import ZMTP

/// Drives one TCP connection: feeds received bytes to a `ZMTPConnection`, writes what it produces, and
/// reports the handshake, messages and the end of the connection to the `SocketCore`.
///
/// Reading is manual (`autoRead` is off). The handler reads while the handshake runs, and afterwards
/// only while the pipe's received-message queue has room, so a slow `receive()` side pushes back on
/// the sender through TCP.
final class ConnectionHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let core: SocketCore
    /// The pipe this connection belongs to when it is a connect-side connection; nil on the bind side
    /// until the handshake is done.
    private let connectPipeID: Int?
    private var connection: ZMTPConnection

    private(set) var channel: (any Channel)?
    private(set) var pipe: Pipe?
    /// The ZMTP handshake finished.
    private(set) var isReady = false
    /// The socket accepted this connection after the handshake: messages may flow.
    private(set) var isAttached = false
    private var started = false
    private var readInFlight = false

    init(core: SocketCore, connectPipeID: Int?) {
        self.core = core
        self.connectPipeID = connectPipeID
        self.connection = ZMTPConnection(
            configuration: .init(
                socketType: core.behavior.socketType,
                routingID: core.options.routingID?.frame.bytes ?? [],
                maxMessageSize: core.options.maxMessageSize
            ))
    }

    // MARK: Called by the core

    func attach(_ pipe: Pipe) {
        self.pipe = pipe
    }

    func markAttached() {
        isAttached = true
    }

    func encode(_ frames: [ByteBuffer], into buffer: inout ByteBuffer) {
        connection.encodeMessage(frames, into: &buffer)
    }

    func encodeSubscription(_ topic: ByteBuffer, subscribe: Bool, into buffer: inout ByteBuffer) {
        if subscribe {
            connection.encodeSubscribe(topic, into: &buffer)
        } else {
            connection.encodeCancel(topic, into: &buffer)
        }
    }

    /// Asks the transport for more bytes if nothing is pending and the pipe can take more messages.
    func resumeReadIfNeeded() {
        guard started, !readInFlight, let channel, channel.isActive else { return }
        if isReady {
            guard let pipe, core.mayRead(from: pipe) else { return }
        }
        readInFlight = true
        channel.read()
    }

    // MARK: ChannelInboundHandler

    func handlerAdded(context: ChannelHandlerContext) {
        channel = context.channel
        core.handlerAdded(self)
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        core.handlerRemoved(self)
    }

    func channelActive(context: ChannelHandlerContext) {
        guard core.connectionActive(self, connectPipeID: connectPipeID) else {
            context.close(promise: nil)
            return
        }
        started = true
        apply(connection.start(), context: context)
        context.fireChannelActive()
        resumeReadIfNeeded()
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        apply(connection.receive(unwrapInboundIn(data)), context: context)
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        context.flush()
        readInFlight = false
        resumeReadIfNeeded()
        context.fireChannelReadComplete()
    }

    func channelInactive(context: ChannelHandlerContext) {
        core.connectionClosed(self)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        context.close(promise: nil)
    }

    // MARK: Output

    private func apply(_ outputs: [ZMTPConnection.Output], context: ChannelHandlerContext) {
        var wrote = false
        for output in outputs {
            switch output {
            case .write(let buffer):
                context.write(wrapOutboundOut(buffer), promise: nil)
                wrote = true
            case .ready(let peer, _):
                isReady = true
                guard core.handshakeCompleted(self, peer: peer) else {
                    isReady = false
                    context.flush()
                    context.close(promise: nil)
                    return
                }
            case .message(let frames):
                if let pipe { core.messageReceived(frames, on: pipe) }
            case .subscribe(let topic):
                if let pipe { core.behavior.peerSubscribed(topic, on: pipe) }
            case .cancel(let topic):
                if let pipe { core.behavior.peerCancelled(topic, on: pipe) }
            case .closed:
                // Any ERROR command was emitted as a .write just before.
                context.flush()
                context.close(promise: nil)
                return
            }
        }
        if wrote { context.flush() }
    }
}
