#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import NIOCore

/// One frame of a message: an immutable run of bytes. Copies of a frame share their storage.
public struct Frame: Sendable, Hashable, ExpressibleByStringLiteral {
    package private(set) var buffer: ByteBuffer

    /// An empty frame.
    public init() {
        buffer = ByteBuffer()
    }

    public init(_ bytes: some Sequence<UInt8>) {
        buffer = ByteBuffer(bytes: bytes)
    }

    /// The UTF-8 bytes of `string`.
    public init(_ string: String) {
        buffer = ByteBuffer(string: string)
    }

    public init(_ data: Data) {
        buffer = ByteBuffer(bytes: data)
    }

    public init(stringLiteral value: String) {
        self.init(value)
    }

    /// Wraps `buffer` without copying it.
    package init(buffer: ByteBuffer) {
        self.buffer = buffer
    }

    /// Number of bytes.
    public var count: Int { buffer.readableBytes }

    public var isEmpty: Bool { buffer.readableBytes == 0 }

    public var bytes: [UInt8] { Array(buffer.readableBytesView) }

    public var data: Data { Data(buffer.readableBytesView) }

    /// The bytes as UTF-8 text, or nil when they are not valid UTF-8.
    public var string: String? { String(validating: buffer.readableBytesView, as: UTF8.self) }

    public func withUnsafeBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
        try buffer.withUnsafeReadableBytes(body)
    }
}
