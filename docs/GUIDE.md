# swift-zmtp guide

A practical guide to using swift-zmtp: ZeroMQ for Swift, written in Swift, with no libzmq to install. It assumes you
know what a socket is and have heard of ZeroMQ; if not, the [ZeroMQ Guide](https://zguide.zeromq.org/) is the best
introduction, and everything here has a matching pattern there.

Every Swift snippet below is code from the [`Examples/`](../Examples) package, which is built and run. Each snippet says which
subcommand of `zmq-examples` it comes from. To try one, build the examples once:

```bash
cd Examples
swift build
Z=.build/debug/zmq-examples
$Z help
```

The commands in this guide use `$Z` for that binary. [`Examples/README.md`](../Examples/README.md) lists all the programs
and how to start each pair.

Contents: [Installing](#1-installing) · [A first program](#2-a-first-program) · [Socket types](#3-socket-types-and-which-pairs-connect) ·
[Messages and frames](#4-messages-and-frames) · [Bind, connect, endpoints](#5-bind-connect-and-endpoints) ·
[Options](#6-options) · [Receiving](#7-receiving-and-waiting-on-several-sockets) · [Cancellation](#8-cancellation-and-time-limits) ·
[Events](#9-socket-events) · [Security](#10-security-plain-and-curve) · [Proxies](#11-proxies) ·
[Interoperating](#12-interoperating-with-libzmq-and-pyzmq) · [Limits](#13-limits)

## 1. Installing

You need Swift 6 and, today, macOS 15 (see [Limits](#13-limits) for Linux and iOS). Nothing else: no libzmq, no libsodium,
no XCFramework. There is no tagged release yet, so depend on the branch:

```swift
// Package.swift (not part of Examples: the Examples package uses a path dependency on the package above it)
.package(url: "https://github.com/daitomanabe/swift-zmtp.git", branch: "main")
```

```swift
// the target's dependencies
.product(name: "ZMQ", package: "swift-zmtp")
```

Then `import ZMQ`. The API will change before 1.0.

## 2. A first program

Request and reply: a client sends a request and waits for the answer; a server answers one request at a time.
The server, from `hwserver` (`HelloWorld.swift`):

```swift
let rep = RepSocket()
let bound = try await rep.bind(endpoint)
print("hwserver: listening on \(bound)")

var served = 0
while served < limit {
    let request = try await rep.receive()
    print("hwserver: received \(request.frames[0].string ?? "(binary data)")")
    try await rep.reply("World")
    served += 1
}
await rep.close()
```

The client, from `hwclient`:

```swift
let req = ReqSocket()
try await req.connect(endpoint)

for number in 0..<count {
    print("hwclient: sending Hello \(number)")
    let reply = try await req.request("Hello")
    print("hwclient: received \(reply.frames[0].string ?? "(binary data)") \(number)")
}
await req.close()
```

```bash
$Z hwserver          # one terminal
$Z hwclient          # another
```

Things to notice, which hold for every socket type:

- Every socket is a class that you create with `init(context:options:)` and use with `async` methods. Sockets are `Sendable`,
  so any task can use them.
- `bind` starts listening and returns the endpoint it bound. `connect` returns at once: the connection is made in the
  background, and made again when it breaks. You do not have to start the server first, and a request sent before the server is
  there waits in the socket.
- Each socket type has only the calls that make sense for it. A `ReqSocket` has `request(_:)`, which sends and waits for the
  reply in one call, so it cannot be used out of order. A `RepSocket` has `receive()` and `reply(_:)`, and `receive()` throws
  `ZMQError.invalidState` if the last request has not been answered.
- A string literal is a one-frame `Message`: `reply("World")`. Any other `String` needs `Message(text)`.
- `close()` lets queued messages go out first (up to `options.linger`). A socket that is simply released is closed at once.

## 3. Socket types and which pairs connect

| Pattern | Sockets | What it does |
|---|---|---|
| Request-reply | `ReqSocket`, `RepSocket` | The client sends a request and waits for one reply. The server takes one request at a time. |
| Publish-subscribe | `PubSocket`, `SubSocket` | The publisher sends to everyone subscribed; it never waits. A subscriber gets only the messages whose first frame starts with a prefix it subscribed to. |
| Pipeline | `PushSocket`, `PullSocket` | Each message goes to one of the connected PULL sockets, in turn. PULL takes from all its PUSH sockets in turn. |
| Asynchronous request-reply | `DealerSocket`, `RouterSocket` | DEALER sends to its peers in turn and receives from all, with no fixed order. ROUTER tells you who sent each message and sends to a peer by name (`RoutingID`). |
| Exclusive pair | `PairSocket` | Two sockets talking to each other, both ways. |
| Brokers | `XPubSocket`, `XSubSocket` | PUB and SUB that also pass subscriptions along. `XPubSocket.receive()` reports them. For proxies, see [Proxies](#11-proxies). |

A connection is accepted only between these types, as in libzmq (a mismatch fails the handshake and shows up as
`SocketEvent.handshakeFailed`):

| Socket | connects to |
|---|---|
| REQ | REP, ROUTER |
| REP | REQ, DEALER |
| DEALER | REP, DEALER, ROUTER |
| ROUTER | REQ, DEALER, ROUTER |
| PUB | SUB, XSUB |
| XPUB | SUB, XSUB |
| SUB | PUB, XPUB |
| XSUB | PUB, XPUB |
| PUSH | PULL |
| PULL | PUSH |
| PAIR | PAIR |

Which side binds does not matter to ZeroMQ. By habit the side that stays put binds (a server, a broker, the sink of a
pipeline), and the ones that come and go connect.

Some behaviour that surprises people the first time:

- **A SUB socket receives nothing until it subscribes**, and a subscriber that connects late misses what was published
  before. Subscribe with `subscribe(_:)`; `Frame()` (the default) means everything.
- **The filter is a prefix of the first frame.** Subscribing to `10001` also matches a topic `100012`. Put the topic in a
  frame of its own, in front of the data.
- **PUB never waits.** A subscriber whose queue is full misses messages, and a message that nobody subscribed to is dropped.
- **PUSH waits** while no peer can take a message. That is the pipeline's back pressure. A PUSH socket hands messages to
  whoever is connected when it sends, so if the first worker connects a moment before the others, it gets all the work.
  The ventilator in `taskvent` waits until its workers are there.
- **REQ and REP go in lockstep.** If a REQ socket never gets its reply, `request(_:)` waits, until its task is cancelled.
  Use a time limit ([Cancellation](#8-cancellation-and-time-limits)).

## 4. Messages and frames

A `Frame` is a run of bytes. A `Message` is a list of frames that arrive together and in order. ZeroMQ does not look inside
a frame. This is from `frames` (`Frames.swift`), which sends over a pair of PAIR sockets inside one process:

```swift
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
```

```
frame 0: 11 bytes, text "temperature", bytes [116, 101, 109, 112, 101, 114, 97, 116, 117, 114, 101]
frame 1: 3 bytes, text (not text), bytes [0, 255, 16]
frame 2: 0 bytes, text "", bytes []
```

- A message needs at least one frame: sending `Message([])` throws `ZMQError.invalidState`. The `Frame()` above is an
  empty frame, which is fine.
- Copies of a frame share their storage, and a received frame is a slice of the network buffer, so passing frames around is cheap.
- Several frames are the way to say "topic, then data" or "address, then body". The weather server in `wuserver`
  (`WeatherUpdates.swift`) publishes two frames:

```swift
// Two frames: the topic, then the data. A subscriber to "10003" gets the whole message.
try await pub.send(Message([Frame(String(zipCode)), Frame("\(temperature) \(humidity)")]))
```

  and the client in `wuclient` subscribes to the topic and reads both:

```swift
let sub = SubSocket()
// A SUB socket gets nothing until it subscribes. This may come before connect().
try await sub.subscribe(Frame(zipCode))
try await sub.connect(endpoint)
...
let update = try await sub.receive()
let topic = update.frames[0].string ?? ""
```

- A ROUTER hands you the sender with each message as a `RoutedMessage` (`routingID` and `message`) and sends with
  `send(_:to:)`. A REQ peer puts an empty frame in front of its message, and a reply to it through a ROUTER must have the
  same empty frame; `REP` and `ZMQProxy` do this for you.

## 5. Bind, connect, and endpoints

An endpoint is a string `transport://address`:

| Endpoint | Use |
|---|---|
| `tcp://127.0.0.1:5555` | TCP. To bind: an IPv4 address or `*` (all interfaces), and a port or `*` for "any free port". To connect: also a host name or an IPv6 address in brackets. |
| `ipc:///tmp/hw.ipc` | A Unix domain socket file, for processes on one machine. `bind` removes an old file of that name first and `close()` removes its own. On macOS the path may be 103 bytes at most. `ipc://*` makes a new name in the temporary directory. |
| `inproc://workers` | Between sockets of one process, with no system call in between. Both sockets must use the same `ZMQContext` (the default one unless you make another). |

`bind` returns the endpoint it actually bound. For `tcp://127.0.0.1:*` that is the one with the port the system chose, and it
is what you give to `connect`. The server in `hwserver` prints it. For tests and tools, a port that is free right now:

```swift
func unusedEndpoint() async throws -> String {
    let probe = PullSocket()
    let endpoint = try await probe.bind("tcp://127.0.0.1:*")   // "*" asks the system for a free port
    await probe.close()
    return endpoint
}
```

(from `Support.swift`; it is the first thing `events` does.) All three transports work the same way for the program;
`hwserver` and `hwclient` take any of them:

```bash
$Z hwserver ipc:///tmp/hw.ipc --count 3 &
$Z hwclient ipc:///tmp/hw.ipc --count 3
```

A socket can bind and connect any number of times, to different endpoints. `inproc` is how worker tasks reach a
broker, as in `mtserver` (`MultithreadedServer.swift`): the broker's DEALER binds the name, and each worker connects to it.

```swift
static let workersEndpoint = "inproc://workers"
...
try await dealer.bind(workersEndpoint)
...
try await rep.connect(workersEndpoint)
```

A connect that comes before the bind of its `inproc` name waits for it, as TCP waits for a server.
An `inproc` link ignores `options.security`; it never leaves the process.

## 6. Options

`SocketOptions` is a struct of settings. You fill it in and give it to the socket's initializer; it cannot be changed
afterwards.

```swift
var options = SocketOptions()
options.sendHighWaterMark = 2
let push = PushSocket(options: options)
```

(from `options`, `OptionsTour.swift`.) The `options` subcommand runs a short demonstration of the high-water mark,
`conflate`, `immediate` and `routerMandatory`; here is what it prints, one part at a time.

**High-water marks, linger, reconnecting, heartbeats, keepalive, and the size limit.**
`tunedOptions()` in `OptionsTour.swift` (used by `options`) sets all of these:

```swift
var options = SocketOptions()
// At most this many messages wait for each peer (see highWaterMark() for what happens at the limit).
options.sendHighWaterMark = 100
options.receiveHighWaterMark = 100
// close() waits this long for queued messages to be written. nil: as long as it takes.
options.linger = .seconds(2)
// A connecting socket that lost its peer retries after 100 ms, then 200 ms, 400 ms, ... up to 5 s.
options.reconnectInterval = .milliseconds(100)
options.reconnectIntervalMax = .seconds(5)
// Send a PING every 2 s. If nothing at all comes back within 4 s, the connection is closed
// (and a connecting socket makes a new one).
options.heartbeatInterval = .seconds(2)
options.heartbeatTimeout = .seconds(4)
// TCP keepalive probes after 30 s without traffic, for the operating system's own dead-peer detection.
options.tcpKeepAlive = .seconds(30)
// A frame bigger than this closes the connection instead of filling the memory.
options.maxMessageSize = 1_000_000
// The name by which a ROUTER knows this socket (DEALER and REQ).
options.routingID = RoutingID(Frame("worker-1"))
```

- **High-water marks** (default 1000) are the number of messages that may wait for one peer. At the send mark,
  `send` on PUSH, DEALER and REQ waits for room; PUB and ROUTER drop the message for that peer instead. At the receive mark the socket
  stops reading from that connection and TCP pushes back on the sender. 0 or less means no limit.
- **`linger`** (default 1 s) is how long `close()` waits for queued messages to be written. `nil` waits as long as it takes.
- **Reconnecting.** A connecting socket tries again when the connection fails or ends, after `reconnectInterval`
  (default 100 ms). If you set `reconnectIntervalMax`, the delay doubles after each failure up to that.
- **Heartbeats** detect a peer that is gone without saying goodbye (a cable pulled, a machine frozen), which TCP alone may take
  hours to notice. With `heartbeatInterval` set the socket sends a PING at that interval once the handshake is done; if nothing
  at all arrives for `heartbeatTimeout` (default: the same as the interval), it closes the connection, and a connecting
  socket connects again. `heartbeatTTL` tells the peer to give up on us after that long. Every peer answers PINGs, whether it sends any or
  not. Heartbeats need a ZMTP 3.1 peer; libzmq 4.3 is one.
- **`tcpKeepAlive`** turns on the operating system's TCP keepalive, starting after the connection has been idle this long.
  `handshakeTimeout` (default 30 s) closes a connection whose setup takes too long.
- **`maxMessageSize`** is per frame. A bigger frame closes the connection.
- **`routingID`** is the name a ROUTER sees for a REQ or DEALER peer. Without one the ROUTER makes up a name. Two connections with
  the same one are not allowed at the same time.

**Seeing the high-water mark** (from `options`, in `highWaterMark()`). It connects a PUSH socket with a send mark of 2 to an endpoint where
nothing listens, and sends three messages:

```swift
for number in 1...3 {
    let message = Message("message \(number)")
    let queued = try await timeout(after: .milliseconds(300)) { try await push.send(message) } != nil
    print("  message \(number): \(queued ? "queued" : "still waiting for room after 300 ms")")
}
```

```
  message 1: queued
  message 2: queued
  message 3: still waiting for room after 300 ms
```

**`conflate`** (PUSH, PULL, PUB, SUB, DEALER) keeps only the newest message in each queue. A slow reader of sensor values
or camera frames wants the latest one, not a backlog. From `options`, in `conflate()`: five messages are sent before the receiver is there.

```swift
var options = SocketOptions()
options.conflate = true
let push = PushSocket(options: options)
let endpoint = try await unusedEndpoint()
try await push.connect(endpoint)
for value in 1...5 { try await push.send(Message("value \(value)")) }   // never waits, keeps the newest
```

```
  received "value 5"
  anything else after that? no
```

**`immediate`** (connecting sockets; from `options`, in `immediate()`) makes a connection count as a destination only after its handshake. Without it,
`connect` creates the queue at once, so messages wait for a peer that is not there yet and survive reconnects.
With it, `send` waits until a peer is really there, and what was queued for a connection that ends is dropped:

```swift
for useImmediate in [false, true] {
    var options = SocketOptions()
    options.immediate = useImmediate
    let push = PushSocket(options: options)
    try await push.connect(endpoint)
    let queued = try await timeout(after: .milliseconds(300)) { try await push.send("hello") } != nil
    print("  immediate = \(useImmediate): \(queued ? "send returned, the message is queued" : "send is still waiting for a peer")")
    await push.close()
}
```

```
  immediate = false: send returned, the message is queued
  immediate = true: send is still waiting for a peer
```

**`routerMandatory`** (ROUTER; from `options`, in `routerMandatory()`) makes `send(_:to:)` report what it would otherwise hide. A routing ID that nobody has throws
`ZMQError.hostUnreachable`; a peer at its high-water mark makes the call wait (a send that waits holds up the ones behind it):

```swift
var options = SocketOptions()
options.routerMandatory = mandatory
let router = RouterSocket(options: options)
do {
    try await router.send("hello", to: RoutingID(Frame("nobody")))
    print("  routerMandatory = \(mandatory): no error, the message was dropped")
} catch ZMQError.hostUnreachable(let routingID) {
    print("  routerMandatory = \(mandatory): hostUnreachable, nobody is called \"\(routingID.frame.string ?? "")\"")
}
```

```
  routerMandatory = false: no error, the message was dropped
  routerMandatory = true: hostUnreachable, nobody is called "nobody"
```

## 7. Receiving, and waiting on several sockets

Every socket that receives has `receive()`, which waits for the next message. All of them except REP also have `messages`, an
`AsyncSequence` that calls `receive()` until the socket is closed and then ends without an error:

```swift
for try await message in tasks.messages {
    continuation.yield(.task(message.frames[0].string ?? ""))
}
```

(from `mspoller`, `MultiSocketReader.swift`.) `RouterSocket.receive()` gives a `RoutedMessage`, `XPubSocket.receive()` an
`XPubMessage` (a subscription change or a message), and the other sockets a `Message`.

C programs wait on several sockets with `zmq_poll`. Here, each socket gets a task, and a task group runs them side by side.
Start by passing all the messages to one loop, which is the shape that keeps your own state free of data races. This is
`mspoller`: it reads tasks from a ventilator and weather reports from a publisher, and stops after `count` messages in all.

```swift
// Both sockets feed one stream. `messages` ends when its socket is closed.
let (inputs, continuation) = AsyncStream.makeStream(of: Input.self)
await withThrowingTaskGroup(of: Void.self) { group in
    group.addTask {
        defer { continuation.finish() }   // if a reader ends, so does the loop below
        for try await message in tasks.messages {
            continuation.yield(.task(message.frames[0].string ?? ""))
        }
    }
    group.addTask {
        defer { continuation.finish() }
        for try await message in weather.messages {
            continuation.yield(.weather(message.frames.dropFirst().first?.string ?? ""))
        }
    }

    var seen = 0
    for await input in inputs {
        switch input {
        case .task(let milliseconds): print("mspoller: task of \(milliseconds) ms")
        case .weather(let data): print("mspoller: weather for \(zipCode): \(data)")
        }
        seen += 1
        if seen == count { break }
    }
    group.cancelAll()   // stops both readers
}
```

```bash
$Z wuserver &
$Z taskvent --workers 1 &
$Z mspoller
```

`Input` is a small enum of what can arrive (`.task(String)` and `.weather(String)`). A cancelled `receive` loses no
message, so stopping the readers with `cancelAll()` does not eat a message that was on its way.

If the sockets do independent things, give each task its own handling and no shared stream: the same group, with the work
in each `addTask` closure.

## 8. Cancellation and time limits

Every call that waits ends with `CancellationError` when its task is cancelled: `receive()`, `request(_:)`, a `send` that waits
for room or for a peer. Cancelling loses nothing:

- a cancelled `receive()` has not taken a message, so the next one gets it;
- a cancelled `request(_:)` frees the REQ socket for the next request, and the late reply, if it comes, is dropped;
- a cancelled `send` has sent nothing.

That makes a time limit a few lines of ordinary Swift. This is `timeout(after:_:)` from `Support.swift`, which all the
examples use:

```swift
func timeout<T: Sendable>(
    after limit: Duration, _ operation: @escaping @Sendable () async throws -> T
) async throws -> T? {
    try await withThrowingTaskGroup(of: T?.self) { group in
        group.addTask { try await operation() }
        group.addTask {
            try await Task.sleep(for: limit)
            return nil
        }
        defer { group.cancelAll() }
        return try await group.next() ?? nil
    }
}
```

It returns nil if the operation took longer than `limit`, and cancels it. The worker in `taskwork` (`Pipeline.swift`) stops
when no task has come for a while:

```swift
// receive() is cancelled when the timeout ends, and a cancelled receive loses no message.
while let task = try await timeout(after: .seconds(idle), { try await tasks.receive() }) {
```

and `curve-client` gives up on a request whose server never answers (for example because it refused the client):

```swift
guard let reply = try await timeout(after: .seconds(5), { try await req.request(request) }) else {
```

A REQ socket never gives up by itself: a server that does not answer keeps `request(_:)` waiting, so use a time limit for
every request that matters. An unreachable peer is not an error for `connect` either; the socket keeps trying
(see [Events](#9-socket-events) to watch it).

A loop that runs for the life of the program is cancelled the same way: keep its `Task` and call `cancel()`. In `plain`
(`PlainExample.swift`) the server loop is a task that is cancelled when the demonstration is over:

```swift
let server = Task {
    while true {
        _ = try await rep.receive()
        try await rep.reply("World")
    }
}
```

```swift
server.cancel()
await rep.close()
```

## 9. Socket events

A connecting socket does not fail when nobody listens; it keeps trying in the background. `events()` shows what happens to a
socket's connections, as an `AsyncStream<SocketEvent>`. Every call makes a new stream, so several parts of a program can
watch one socket. A stream shows what happens after the call (so ask for it before `bind` or `connect`), keeps the newest 256 events
when its reader is slow, and ends with `.closed`.

| Event | When |
|---|---|
| `listening(endpoint:)` | `bind` succeeded; the endpoint is the address actually bound |
| `bindFailed(endpoint:reason:)` | `bind` failed (address in use, no permission) |
| `accepted(endpoint:)` | a peer connected to a bound endpoint; its handshake is not done |
| `connected(endpoint:)` | our connection attempt reached the peer; its handshake is not done |
| `connectRetried(endpoint:after:)` | an attempt failed or a connection ended; the next attempt is after `after` |
| `handshakeSucceeded(endpoint:peerSocketType:)` | the connection is usable. `peerSocketType` is for example `"PUSH"` |
| `handshakeFailed(endpoint:reason:)` | wrong socket type, refused login, wrong key, handshake timeout |
| `disconnected(endpoint:)` | a connection that had been set up is gone |
| `closed` | the socket is closed; the last event |

From `events` (`Events.swift`): a PULL socket that connects to an endpoint where nothing listens yet.

```swift
let pull = PullSocket(options: options)
// Print every event of the socket until the last one (.closed) ends the stream.
let events = pull.events()
let log = Task {
    for await event in events { print("      event: \(event)") }
}
```

It prints this (`options` there are `reconnectInterval = 100 ms` and `reconnectIntervalMax = 1 s`). The PUSH socket
binds after about one and a half seconds, and the numbers 1 to 5 are the steps of the program:

```
1. PULL connects to tcp://127.0.0.1:63003, where nothing listens yet
      event: connectRetried(endpoint: "tcp://127.0.0.1:63003", after: 0.1 seconds)
      event: connectRetried(endpoint: "tcp://127.0.0.1:63003", after: 0.2 seconds)
      event: connectRetried(endpoint: "tcp://127.0.0.1:63003", after: 0.4 seconds)
      event: connectRetried(endpoint: "tcp://127.0.0.1:63003", after: 0.8 seconds)
      event: connectRetried(endpoint: "tcp://127.0.0.1:63003", after: 1.0 seconds)
2. a PUSH socket binds tcp://127.0.0.1:63003 and sends a message
      event: connected(endpoint: "tcp://127.0.0.1:63003")
      event: handshakeSucceeded(endpoint: "tcp://127.0.0.1:63003", peerSocketType: "PUSH")
3. PULL received "hello"
4. the PUSH socket closes
      event: disconnected(endpoint: "tcp://127.0.0.1:63003")
      event: connectRetried(endpoint: "tcp://127.0.0.1:63003", after: 0.1 seconds)
      ...
5. PULL closes
      event: closed
```

Events are also how you find out about a refused login or a wrong key. `request(_:)` will not throw for those; it waits, and
the socket retries (`handshakeFailed` each time). `curve-client` prints the first failure and `plain` the first one for
each client.

## 10. Security: PLAIN and CURVE

By default a connection has no security (the NULL mechanism). You choose another with `options.security`, on both sides;
both ends must use the same mechanism. A refused login or a wrong key makes the handshake fail: look for `handshakeFailed`
in the socket's events. `inproc` links ignore `security`.

### PLAIN

A user name and a password. **Nothing is encrypted and the password travels in the clear**, so use PLAIN only on a network you
trust or inside another protected channel. A server needs its accounts: user name to password.
From `plain` (`PlainExample.swift`):

```swift
var serverOptions = SocketOptions()
serverOptions.security = .plainServer(accepting: ["admin": "secret"])
let rep = RepSocket(options: serverOptions)
```

```swift
var clientOptions = SocketOptions()
clientOptions.security = .plainClient(username: "admin", password: password)
clientOptions.reconnectInterval = .seconds(1)
let req = ReqSocket(options: clientOptions)
```

```
password "secret": reply "World"
  handshake failed: the peer sent ERROR 400
password "guess": no reply within 2 s
```

For anything other than a fixed list, give `.plainServer` a function (`.plainServer { credentials in ... }` takes a
`PlainCredentials` and returns an `async` `Bool`). `accepting:` compares passwords in constant time; do the same in your own function.

### CURVE

CURVE (CurveZMQ) authenticates both sides and encrypts everything after the handshake with keys that live as long as one connection,
so recorded traffic stays unreadable even if a long-term key leaks later. Each peer has a key pair: a **public key**, which anybody may know, and a **secret
key**. The client must know the **server's public key** in advance; that is what protects it from connecting to an impostor.
The server finds out the client's public key during the handshake (after the client has shown that it owns the secret key),
and an `authenticate` function decides whether to accept it.

Keys are 32 bytes; people handle them as 40 characters of Z85 text. `curve-keygen` makes a pair (from `CurveExample.swift`):

```swift
let keys = CurveKeyPair.generate()
print("public key: \(keys.publicKey.z85)")
print("secret key: \(keys.secretKey.z85)")
```

A key that was saved earlier is read back with the secret key; the public key is derived from it (any text that is not a
valid key throws `ZMQError.invalidKey`):

```swift
guard let text else { return CurveKeyPair.generate() }
return CurveKeyPair(secretKey: try CurveSecretKey(z85: text))
```

The server (`curve-server`) with an `authenticate` function. It gets the public key of each client that has proved it owns it,
and says yes or no. A refused client gets ERROR 400 and the connection closes. Leave `authenticate` out to accept every key:

```swift
var options = SocketOptions()
options.security = .curveServer(keyPair: keys) { clientKey in
    // Called once for each client that has proved that it owns `clientKey`, before any message.
    // Returning false refuses it: the connection is closed. Leave `authenticate` out to accept everybody.
    let accepted = allowed.isEmpty || allowed.contains(clientKey)
    print("curve-server: client \(clientKey.z85) \(accepted ? "accepted" : "refused")")
    return accepted
}
let rep = RepSocket(options: options)
```

and the client (`curve-client`), which takes the server's public key from the command line:

```swift
let serverKey = try CurvePublicKey(z85: arguments.positional[0])
```

```swift
var options = SocketOptions()
options.security = .curveClient(serverKey: serverKey, keyPair: keys)
// A refused client tries again at once and then forever. Do not hammer a server that said no.
options.reconnectInterval = .seconds(1)
let req = ReqSocket(options: options)
```

Try it. Z85 keys contain characters that the shell treats specially (`?`, `<`, `>`, `{`, `&`, ...), so put every key in single quotes:

```bash
$Z curve-keygen                                             # a key pair for the client
$Z curve-server --allow 'CLIENT_PUBLIC_KEY'                 # prints the server's public key
$Z curve-client 'SERVER_PUBLIC_KEY' --secret-key 'CLIENT_SECRET_KEY'
```

Handling the keys:

- **The server's public key must reach the clients by a channel you trust** (a config file you ship, a deployment step). If an
  attacker can swap it, CURVE protects nothing.
- **Keep secret keys secret.** `CurveSecretKey` prints as `CurveSecretKey(redacted)`, and its text is only available as `z85`. Store it
  where only the program can read it (a file with mode 0600, the Keychain). Do not put it on a command line (other users see
  the arguments), in a log, or in the repository. The examples take `--secret-key` for trying things out only.
- **A fresh `CurveKeyPair.generate()` at every start gives encryption but no identity**: the server cannot tell which client it is.
  For an allow list the clients need keys that stay the same, saved as above, and the server needs to know their public keys.
  Public keys are not secret.
- **Servers need a stable key too**, or every client must be given the new key at every start. Save the secret key and give
  the program its Z85 text; the public key follows from it.
- If you use `authenticate`, it is `async` and runs in the handshake (its time counts against `handshakeTimeout`).
  Do not do slow work for an unknown caller.

The CURVE code is written in Swift (Salsa20, XSalsa20, Poly1305 and the NaCl box; X25519 comes from CryptoKit) and was
checked against libsodium's output and against libzmq in both directions. **It has not had an external security audit.**
Read [Limits](#13-limits) before you put it on a network you do not control.

## 11. Proxies

A proxy sits between two sockets and passes messages in both directions: the middle of a broker, a queue or a forwarder.
`ZMQProxy.run` has one version for each combination:

| Call | Use |
|---|---|
| `run(frontend: XSubSocket, backend: XPubSocket)` | publish-subscribe broker. Subscriptions flow from `backend` to `frontend`, so the publishers send only what somebody wants |
| `run(frontend: RouterSocket, backend: DealerSocket)` | request-reply broker. The client's address travels in front of each message, so replies find their way back |
| `run(frontend: PullSocket, backend: PushSocket)` | one stage of a pipeline. When no worker can take a message, the proxy stops reading, and the producers feel it |

`run` carries messages as long as both sockets are open. It returns when either one is closed and throws `CancellationError`
when its task is cancelled; every task it started has finished by then. The sockets stay yours: you bind, connect and
close them.

**A publish-subscribe forwarder**, from `wuproxy` (`WeatherProxy.swift`). The XSUB socket connects to the publisher; subscribers
connect to the XPUB socket, which tells the proxy about their subscriptions:

```swift
let frontend = XSubSocket()
try await frontend.connect(publisherEndpoint)
let backend = XPubSocket()
let bound = try await backend.bind(subscribersEndpoint)
print("wuproxy: forwarding \(publisherEndpoint) to \(bound)")

// Returns when one of the sockets is closed, and throws CancellationError when the task is cancelled.
try await ZMQProxy.run(frontend: frontend, backend: backend)
```

```bash
$Z wuserver &
$Z wuproxy &
$Z wuclient tcp://127.0.0.1:5559 10003
```

**A server with a pool of workers**, from `mtserver` (`MultithreadedServer.swift`). Clients connect to the ROUTER, the
DEALER hands requests to the workers in turn, and the workers are REP sockets on `inproc://`, each one written as if it were
the whole server:

```swift
let router = RouterSocket()
let dealer = DealerSocket()
let bound = try await router.bind(frontendEndpoint)
try await dealer.bind(workersEndpoint)
```

```swift
server.addTask { try await ZMQProxy.run(frontend: router, backend: dealer) }
```

```swift
let rep = RepSocket()
do {
    try await rep.connect(workersEndpoint)
    while true {
        let request = try await rep.receive()
        let text = request.frames[0].string ?? "(binary data)"
        try await rep.reply(Message("World (\(text)) from worker \(id)"))
    }
} catch {
    // Cancelled: the server is stopping.
}
await rep.close()
```

```
mtserver: client 2 got "World (Hello 0) from worker 3"
mtserver: client 3 got "World (Hello 0) from worker 2"
mtserver: client 1 got "World (Hello 0) from worker 1"
```

Run `$Z mtserver --clients 0` and the same server answers `hwclient` on port 5555.

A message that was in flight when a proxy ends can be lost.

## 12. Interoperating with libzmq and pyzmq

swift-zmtp speaks ZMTP 3.1 (and accepts 3.0 peers), so it talks to libzmq and anything built on it. It is tested against
libzmq 4.3.5 through pyzmq 27.2.0: every pair of the basic socket types over TCP, PUSH/PULL and REQ/REP over IPC, XPUB and XSUB,
heartbeats both ways, and PLAIN and CURVE in both directions, with logins and keys that are accepted and refused.

The Python client below talks to `hwserver`. It is the whole program:

```python
import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:5555")
for number in range(2):
    socket.send_string("Hello")
    print("pyzmq received", socket.recv_string(), number)
```

With [uv](https://docs.astral.sh/uv/) installed, save it as `client.py` and run:

```bash
$Z hwserver --count 2 &
uv run --with pyzmq==27.2.0 python3 client.py
```

```
pyzmq received World 0
pyzmq received World 1
```

The other direction works the same way: a pyzmq `REP` socket that binds `tcp://127.0.0.1:5555` and answers every request
with `send_string("World")` is served by `$Z hwclient`.

Notes for mixed systems:

- Messages are bytes. `send_string` and `recv_string` use UTF-8, as `Frame(_ string:)` and `Frame.string` do; for other data agree
  on a format with the other side.
- Subscriptions are prefixes of the first frame in both libraries. The publisher filters, and the SUB socket filters again, so the rules in [Socket types](#3-socket-types-and-which-pairs-connect) hold on both sides.
- For CURVE, the keys are the same 32 bytes, and the Z85 text is the same as the one `zmq_curve_keypair` makes. In pyzmq,
  `zmq.curve_keypair()` returns a pair as Z85 text, and the socket attributes `curve_publickey`, `curve_secretkey` and
  `curve_serverkey` take either the Z85 text or the raw bytes.
- Some libzmq options have no counterpart because they are fixed at creation here or because the library does not need
  them. The ones that matter in practice are in [Options](#6-options): high-water marks, linger, reconnect interval and
  maximum, heartbeats, `conflate`, `immediate`, `ROUTER_MANDATORY`, TCP keepalive, the routing ID (`ZMQ_ROUTING_ID`).

## 13. Limits

What this version does not do, or has not been checked for:

- **Socket types:** the draft sockets (CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER) and STREAM are not implemented.
- **Transports:** `tcp`, `ipc` and `inproc`. No WebSocket (ZWS), no PGM/EPGM/NORM (multicast), TIPC or VMCI. `tcp` bind takes
  IPv4 addresses and `*`, not host names or interface names.
- **Security:** NULL, PLAIN and CURVE. No GSSAPI. There is no ZAP handler socket (`inproc://zeromq.zap.01`); the `authenticate`
  functions take its place.
- **Old peers:** ZMTP 3.0 and 3.1 only. libzmq before 4.0 (ZMTP 1.0 and 2.0) is not supported.
- **No `zmq_poll`.** Wait on several sockets with a task group ([Receiving](#7-receiving-and-waiting-on-several-sockets)).
  There is no C API compatibility layer.
- **Real-time threads.** Do not call it from an audio callback or any thread that must not wait or allocate.
- **Options are fixed when the socket is created.** `SocketOptions` has no setters on a live socket. XPUB has no verbose mode: a
  repeated subscription is not reported again.
- **Platforms.** Everything here was built and tested on macOS only. The package declares iOS 18 and builds CURVE's X25519 with
  swift-crypto on Linux, but neither has been built or tested yet. Treat Linux and iOS as not verified.
- **No external audit.** The CURVE code (Salsa20, XSalsa20, Poly1305, the NaCl box, Z85) is checked against libsodium test
  vectors and against libzmq, and was reviewed inside the project, but no outside security expert has examined it. Secret keys are
  not wiped from memory when they are no longer needed. For a network you do not control, treat this as unaudited software.
- **The API is not stable** until 1.0.
