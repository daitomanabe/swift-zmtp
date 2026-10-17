# swift-zmtp

ZeroMQ in pure Swift. It speaks ZMTP 3.1 and talks to libzmq and pyzmq peers, with no C library to install:
no libzmq, no libsodium, no XCFramework. Sockets are built on SwiftNIO and used with async/await.

日本語は [README.ja.md](README.ja.md)。

> **Early version.** This is stage 1 of the [plan](PLAN.md): TCP, the NULL mechanism (no authentication), and the
> socket types PUSH, PULL, PUB, SUB, REQ, REP, DEALER, ROUTER and PAIR. The API will change before 1.0.
> IPC, inproc, heartbeats and XPUB/XSUB come next; PLAIN and CURVE after that.

## Why

Using libzmq from Swift means building a C++ library for every platform (an XCFramework for iOS, libsodium for CURVE),
handling raw pointers, and keeping libzmq's one-thread-per-socket rule while Swift tasks hop between threads.
The existing Swift packages all wrap libzmq. This one implements the protocol itself, the way JeroMQ (Java),
NetMQ (C#) and zmq.rs (Rust) do.

## Requirements

Swift 6. macOS 15 or iOS 18, or Linux.

## Installation

There is no tagged release yet.

```swift
.package(url: "https://github.com/daitomanabe/swift-zmtp.git", branch: "main")
```

```swift
.product(name: "ZMQ", package: "swift-zmtp")
```

## Usage

```swift
import ZMQ

let pull = PullSocket()
let endpoint = try await pull.bind("tcp://127.0.0.1:*")   // picks a free port
let push = PushSocket()
try await push.connect(endpoint)
try await push.send(["hello", "world"])                   // a two-frame message
let message = try await pull.receive()

let sub = SubSocket()
try await sub.subscribe("depth/")
for try await message in sub.messages { /* … */ }

let req = ReqSocket()
try await req.connect("tcp://192.168.0.10:5555")
let reply = try await req.request("ping")                 // send and wait for the reply in one call
```

Each socket type has only the operations it supports: `subscribe` exists on `SubSocket`, `RouterSocket.send(_:to:)`
takes a routing ID, and `ReqSocket.request(_:)` cannot be called out of order. Waiting calls end with
`CancellationError` when their task is cancelled. Options such as high-water marks, linger and the routing ID are set
with `SocketOptions` when the socket is created.

## Compatibility

Tested against libzmq 4.3.5 through pyzmq 27.2.0: every pair of the socket types above, with the Swift side binding
and with it connecting. Handshake bytes are checked against recorded libzmq traffic, including ZMTP 3.0 peers.

## Performance

One run each on an Apple M5 Max, in one process over loopback TCP (rough numbers; see `Benchmarks/`):

| Test | swift-zmtp | libzmq 4.3.5 |
|---|---|---|
| Throughput, 16 B × 200,000 | 86,557 msg/s | 6,711,338 msg/s |
| Throughput, 1 KB × 20,000 | 84,358 msg/s (86 MB/s) | 863,154 msg/s (884 MB/s) |
| REQ/REP round trip, 16 B | p50 41 µs, p99 99 µs | p50 58 µs, p99 141 µs |

Round trips are faster than libzmq. Throughput for small messages is far lower, because every `send` and `receive`
currently hops onto the socket's event loop; a fast path is planned ([PLAN.md](PLAN.md), stage 4).

## Layout

- `Sources/ZMTP`: the ZMTP 3.1 protocol without any I/O (greeting, framing, commands, handshake)
- `Sources/ZMQ`: the sockets, on SwiftNIO
- `Tests/ZMTPTests`, `Tests/ZMQTests`, `Tests/InteropTests`
- `Tools/interop`: a pyzmq peer for interop tests and the recorder of the libzmq fixtures
- `Benchmarks`: throughput and latency, with a libzmq baseline in C

## Development

```bash
swift build
```

```bash
Scripts/test.sh --filter ZMQTests
```

```bash
ZMQ_INTEROP=1 Scripts/test.sh --filter InteropTests
```

`Scripts/test.sh` passes its arguments to `swift test`. On a Mac with only the Command Line Tools (no Xcode) it adds the
Swift Testing paths that SwiftPM leaves out. The interop tests run the pyzmq peer with [uv](https://docs.astral.sh/uv/)
and only run when `ZMQ_INTEROP=1` is set. The plan and the design notes ([PLAN.md](PLAN.md),
[docs/DESIGN.md](docs/DESIGN.md)) are in Japanese.

## License

MIT. Written from the ZeroMQ specifications (rfc.zeromq.org 23/ZMTP, 37/ZMTP, 28/REQREP, 29/PUBSUB, 30/PIPELINE,
31/EXPAIR) and recorded libzmq traffic; no libzmq code was copied.
