# swift-zmtp examples

Runnable ports of patterns from the [ZeroMQ Guide](https://zguide.zeromq.org/), written for this library's API.
One executable, `zmq-examples`, with a subcommand for each program. This is a separate Swift package that depends on the
one above it, so it also shows how an application uses `ZMQ`. The [guide](../docs/GUIDE.md) quotes this code.

```bash
cd Examples
swift build
Z=.build/debug/zmq-examples        # build once, then run the binary in each terminal
$Z help                            # every subcommand, with its arguments
```

Like the Guide, the pairs use fixed ports on 127.0.0.1: 5555 (hello world, `mtserver`), 5556 (weather), 5557 and 5558
(pipeline), 5559 (weather proxy). Every subcommand that binds or connects takes an endpoint, so you can use others,
for example `$Z hwserver 'tcp://127.0.0.1:*'` (the server prints the port it got) or `$Z hwserver ipc:///tmp/hw.ipc`.

Every subcommand ends on its own, so nothing is left running by mistake. It has a time limit (the default is in
`help`; `--seconds S` changes it, `--seconds 0` removes it) and most have a message count (`--count N`). A server that
reaches its limit just stops; a client that does not get its answer in time exits with status 1.

## The programs

Start the server (or the part that binds) first, in one terminal, then the other part in a second terminal. Where it
matters, the order is stated.

**Request and reply** (`HelloWorld.swift`). REQ sends "Hello", REP answers "World".

```bash
$Z hwserver
$Z hwclient                  # 10 requests; --count N for another number
```

**Publish and subscribe** (`WeatherUpdates.swift`). PUB sends random weather reports for ten zip codes, 100 a second.
SUB asks for one zip code; the topic is the first frame of each message.

```bash
$Z wuserver
$Z wuclient 10003            # [endpoint] [zipcode]; averages 10 updates, --count N for more
```

**Publish and subscribe through a proxy** (`WeatherProxy.swift`). XSUB and XPUB with `ZMQProxy.run` between a publisher
and its subscribers. The subscribers connect to 5559, not to the publisher.

```bash
$Z wuserver
$Z wuproxy
$Z wuclient tcp://127.0.0.1:5559 10003
```

**Pipeline** (`Pipeline.swift`). PUSH and PULL. The ventilator hands 100 "tasks" (milliseconds of pretend work) to the
workers, which report to the sink. Start the sink, then any number of workers (each in its own terminal), then the ventilator,
which waits until `--workers` workers (default 1) are connected. Add workers and watch the total time fall.

```bash
$Z tasksink
$Z taskwork                  # several times; each stops after 5 s without a task (--idle S)
$Z taskvent --workers 3
```

**Reading several sockets in one loop** (`MultiSocketReader.swift`). `mspoller` takes tasks from a ventilator and weather
updates from the weather server, with a task group instead of `zmq_poll`. It is a worker for the ventilator.

```bash
$Z wuserver
$Z taskvent --workers 1
$Z mspoller                  # [--vent ENDPOINT] [--weather ENDPOINT] [--zipcode ZIP]; stops after --count N (20) messages
```

**A server with many workers** (`MultithreadedServer.swift`). ROUTER and DEALER with `ZMQProxy.run`, three REP workers on
`inproc://workers`, and three REQ clients, all in one process. One command is enough:

```bash
$Z mtserver                  # --workers N --clients N --count N (requests per client)
```

With `--clients 0` it only serves (until its time limit), and `hwclient` can use it as it uses `hwserver`.

**CURVE** (`CurveExample.swift`). Encrypted and authenticated REQ/REP. The server prints its public key; the client needs it.
Z85 keys contain characters the shell treats specially (`?`, `<`, `>`, `{`, `}`, `&` and more), so **put every key in
single quotes**. A key can also start with `--`; put `--` in front of the arguments then (`curve-client -- '--abc…'`).

```bash
$Z curve-server                                 # prints: server public key '...'
$Z curve-client 'SERVER_PUBLIC_KEY'             # any client key is accepted
```

To accept only known clients, make the client a key pair first and give the server its public key:

```bash
$Z curve-keygen                                 # public key / secret key
$Z curve-server --allow 'CLIENT_PUBLIC_KEY'
$Z curve-client 'SERVER_PUBLIC_KEY' --secret-key 'CLIENT_SECRET_KEY'
```

A client with another key is refused and the client says so (`handshakeFailed`, ERROR 400). `--secret-key` on the
command line is for trying things out: other users of the machine can see it in the process list.

**PLAIN** (`PlainExample.swift`). One process: a server with one account, a client that logs in with the right password
and one with the wrong one.

```bash
$Z plain
```

**Events** (`Events.swift`). One process. A PULL socket connects where nothing listens, retries with growing delays, then a
PUSH socket appears; the socket's `SocketEvent`s are printed as they happen.

```bash
$Z events
```

**Frames** (`Frames.swift`) and **options** (`OptionsTour.swift`). One process each. `frames` sends a message of text,
binary and empty frames over `inproc://`. `options` shows what the high-water mark, `conflate`, `immediate` and
`routerMandatory` do, and has one set of options for a connection that must survive a bad network.

```bash
$Z frames
$Z options
```

## Smoke test

Each program can be checked in a few seconds with explicit endpoints on free ports and small counts, for example:

```bash
$Z hwserver tcp://127.0.0.1:47555 --count 3 & SERVER=$!
$Z hwclient tcp://127.0.0.1:47555 --count 3
wait $SERVER
```

(Check that a port is free first with `lsof -i:47555 -P`.)

## Files

| File | |
|---|---|
| `ZMQExamples.swift` | the subcommands, the time limit, the usage text |
| `Arguments.swift`, `Support.swift` | argument parsing; `timeout(after:_:)`, `unusedEndpoint()` |
| `HelloWorld.swift`, `WeatherUpdates.swift`, `WeatherProxy.swift`, `Pipeline.swift` | the Guide's first examples |
| `MultiSocketReader.swift`, `MultithreadedServer.swift` | several sockets in one loop; ROUTER/DEALER with a proxy |
| `CurveExample.swift`, `PlainExample.swift` | security |
| `Events.swift`, `Frames.swift`, `OptionsTour.swift` | events, messages, options |
