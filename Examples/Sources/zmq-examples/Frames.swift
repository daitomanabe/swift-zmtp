import ZMQ

/// Messages and frames, on a pair of PAIR sockets connected over `inproc://` (inside this process).
///
/// A `Frame` is a run of bytes. A `Message` is a list of frames that travel together: the receiver gets them
/// all, in order, or none. ZeroMQ does not know what the bytes mean; text, numbers and files are up to the
/// sender and the receiver. PAIR is the socket type for two peers that talk to each other both ways.
enum Frames {
    static func run(_ arguments: Arguments) async throws {
        let left = PairSocket()
        let right = PairSocket()
        try await left.bind("inproc://frames")
        try await right.connect("inproc://frames")

        let message = Message([
            Frame("temperature"),                  // text: the UTF-8 bytes of a String
            Frame([0x00, 0xFF, 0x10] as [UInt8]),  // any bytes: an array of UInt8 (or `Data`)
            Frame(),                               // an empty frame is a frame too
        ])
        try await left.send(message)

        let received = try await right.receive()
        for (index, frame) in received.frames.enumerated() {
            // `string` is nil when the bytes are not valid UTF-8; `bytes` and `data` always work.
            print("frame \(index): \(frame.count) bytes, text \(frame.string.map { "\"\($0)\"" } ?? "(not text)"), bytes \(frame.bytes)")
        }

        // The short way to send a single text frame: a string literal, also with interpolation, is a Message.
        // And `string` is the short way to read one back: the text of a message of exactly one UTF-8 frame.
        try await right.send("got \(received.frames.count) frames")
        print("left received \"\(try await left.receive().string ?? "")\"")

        // A message needs at least one frame.
        do {
            try await left.send(Message([]))
        } catch {
            print("sending a message without frames: \(error)")
        }

        await left.close()
        await right.close()
    }
}
