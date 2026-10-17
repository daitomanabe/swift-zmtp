# swift-zmtp

ZeroMQ in pure Swift. It speaks ZMTP 3.1 and talks to libzmq and pyzmq peers, with no C library to install:
no libzmq, no libsodium, no XCFramework. Sockets are built on SwiftNIO and used with async/await.

日本語は [README.ja.md](README.ja.md)。

> **Early version.** Stages 1 and 2 of the [plan](PLAN.md) are done: the transports `tcp://`, `ipc://` and
> `inproc://`, the NULL mechanism (no authentication), heartbeats, socket events, and the socket types PUSH, PULL,
> PUB, SUB, XPUB, XSUB, REQ, REP, DEALER, ROUTER and PAIR, plus a proxy. The API will change before 1.0.
> PLAIN and CURVE come next.

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

Waiting on several sockets at once (what `zmq_poll` does in C) is a task group:

```swift
try await withThrowingTaskGroup(of: Void.self) { group in
    group.addTask { for try await message in control.messages { handleControl(message) } }
    group.addTask { for try await message in frames.messages { handleFrame(message) } }
    try await group.waitForAll()
}
```

Heartbeats and connection events:

```swift
var options = SocketOptions()
options.heartbeatInterval = .seconds(1)      // PING every second; no answer within a second closes and reconnects
let push = PushSocket(options: options)
Task { for await event in push.events() { print(event) } }   // connected, handshakeSucceeded, disconnected, …
try await push.connect("tcp://192.168.0.10:5555")
```

Each socket type has only the operations it supports: `subscribe` exists on `SubSocket`, `RouterSocket.send(_:to:)`
takes a routing ID, and `ReqSocket.request(_:)` cannot be called out of order. Waiting calls end with
`CancellationError` when their task is cancelled. Options such as high-water marks, linger, the routing ID, heartbeats,
`conflate`, `immediate`, `routerMandatory` and `tcpKeepAlive` are set with `SocketOptions` when the socket is created.
`ZMQProxy.run(frontend:backend:)` forwards between XSUB and XPUB, ROUTER and DEALER, or PULL and PUSH.

## Compatibility

Tested against libzmq 4.3.5 through pyzmq 27.2.0, with the Swift side binding and with it connecting: every pair of
the stage 1 socket types over TCP; PUSH/PULL and REQ/REP over IPC; XPUB and XSUB with libzmq SUB and XPUB peers; and
heartbeats both ways, including a libzmq peer that is frozen with `SIGSTOP` and resumed. Handshake bytes are checked
against recorded libzmq traffic, including ZMTP 3.0 peers. `inproc://` connects sockets of the same `ZMQContext` only.

## Performance

One run each on an Apple M5 Max, in one process over loopback TCP, Swift and libzmq back to back under the same
load (the machine was busy, load average about 17, so the numbers are rough; see `Benchmarks/`):

| Test | swift-zmtp | libzmq 4.3.5 |
|---|---|---|
| Throughput, 16 B × 200,000 | 1,920,871 msg/s | 3,996,892 msg/s |
| Throughput, 1 KB × 20,000 | 1,227,695 msg/s (1,257 MB/s) | 462,168 msg/s (473 MB/s) |
| REQ/REP round trip, 16 B | p50 148 µs, p99 514 µs | p50 168 µs, p99 396 µs |

`send` and `receive` normally do not hop onto the socket's event loop: they put messages into, or take them from, a
queue the event loop drains and fills in batches. Before this fast path, small-message throughput was about 1/78 of
libzmq's (86,557 msg/s on a quiet machine).

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
