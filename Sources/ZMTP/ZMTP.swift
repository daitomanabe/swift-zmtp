/// Wire constants of ZMTP 3.x (rfc.zeromq.org 23/ZMTP and 37/ZMTP).
public enum ZMTP {
    /// Length of the whole greeting, signature included.
    public static let greetingLength = 64
    /// Length of the signature that opens the greeting: 0xFF, 8 bytes of padding, 0x7F.
    public static let signatureLength = 10
    /// Major version this implementation speaks.
    public static let majorVersion: UInt8 = 3
    /// Minor version this implementation announces (3.1, like libzmq 4.3).
    public static let minorVersion: UInt8 = 1
    /// Largest frame body that uses the short (1 byte) size field.
    public static let maxShortFrameSize = 255
    /// Largest PING context allowed by 37/ZMTP.
    public static let maxPingContextLength = 16
}
