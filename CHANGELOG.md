# Changelog

## 0.1.0 — 2026-10-18

First release. ZeroMQ in pure Swift: ZMTP 3.1 on SwiftNIO with async/await, tested against libzmq 4.3.5 (through
pyzmq 27.2.0) on macOS.

- **Transports:** `tcp://`, `ipc://` (Unix domain sockets) and `inproc://` (sockets of one `ZMQContext`).
- **Socket types:** PUSH, PULL, PUB, SUB, XPUB, XSUB, REQ, REP, DEALER, ROUTER, PAIR, and `ZMQProxy.run` for
  XSUB↔XPUB, ROUTER↔DEALER and PULL→PUSH.
- **Security:** NULL, PLAIN and CURVE (CurveZMQ). The CURVE primitives (Salsa20, XSalsa20, Poly1305, the NaCl box) and
  Z85 are written in Swift and checked against libsodium output and against libzmq; X25519 comes from CryptoKit.
  Authentication is an async closure.
- **Connections:** reconnect with optional backoff, heartbeats (PING/PONG with TTL), handshake timeout, socket events
  (`events()`), high-water marks with back-pressure, linger, `conflate`, `immediate`, `routerMandatory`, `tcpKeepAlive`.
- **API:** typed sockets (only the operations a type supports), `messages` async sequences, cancellation of every waiting
  call, `receive(timeout:)`, `send(_:timeout:)`, `request(_:timeout:)`, string interpolation into messages.
- **Speed:** `send` and `receive` normally skip the event-loop hop; small-message throughput is about half of libzmq's,
  round trips are as fast or faster (see `Benchmarks/`).
- **libzmq quirks handled:** subscriptions are a set per peer on the publisher side (libzmq's SUB sends SUBSCRIBE on
  every subscribe call), and PLAIN's ERROR from a libzmq server (whose name reads `5E 'RROR'` on the wire) is understood.

Not in this release: draft sockets (CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER), STREAM, ZWS, multicast transports,
GSSAPI, ZMTP 1.0/2.0 peers. Linux and iOS are declared in the package but not built or tested yet. The CURVE code has
not had an external security audit. The API may change before 1.0.
