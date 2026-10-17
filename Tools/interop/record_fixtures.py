#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyzmq==27.2.0"]
# ///
"""Record the bytes that libzmq peers exchange, as test fixtures.

A tap (a plain TCP relay on 127.0.0.1) sits between a connecting and a binding
socket and keeps every byte in both directions. Each scenario writes one JSON
file to Tests/ZMTPTests/Fixtures/:

  clientToServer / serverToClient   the whole stream of each side, in hex
  chunks                            the same bytes in the order the tap relayed
                                    them (shows libzmq sending its greeting in parts)
  events                            what the applications sent and received

The client is the connecting side, the server the binding side. Both are
libzmq (pyzmq 27.2.0 bundles libzmq 4.3.5), except in zmtp30-sub, where the
server is a fake ZMTP 3.0 peer written here.

Run from anywhere:
  uv run Tools/interop/record_fixtures.py [--only NAME ...]

Only curve-push-pull changes from run to run (its keys and nonces are random).
"""
import argparse
import json
import socket
import struct
import threading
import time
from pathlib import Path

import zmq
from zmq.auth.thread import ThreadAuthenticator

FIXTURES = Path(__file__).resolve().parents[2] / "Tests" / "ZMTPTests" / "Fixtures"
WAIT = 5.0  # seconds before a step that never happens fails the scenario


class Tap:
    """TCP relay on 127.0.0.1 that records both directions in relay order."""

    def __init__(self, upstream_port):
        self.upstream_port = upstream_port
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.listener.settimeout(WAIT)
        self.port = self.listener.getsockname()[1]
        self.chunks = []  # [(side, bytes)], side is the sender: "client" or "server"
        self.changed = threading.Condition()
        self.pumps = []
        self.acceptor = threading.Thread(target=self._accept, daemon=True)
        self.acceptor.start()

    def _accept(self):
        client, _ = self.listener.accept()
        server = socket.create_connection(("127.0.0.1", self.upstream_port))
        for side, source, target in (("client", client, server), ("server", server, client)):
            pump = threading.Thread(target=self._pump, args=(side, source, target), daemon=True)
            pump.start()
            self.pumps.append(pump)

    def _pump(self, side, source, target):
        while True:
            try:
                data = source.recv(65536)
            except OSError:
                data = b""
            if not data:
                break
            try:
                target.sendall(data)
                relayed = True
            except OSError:
                relayed = False
            with self.changed:
                self.chunks.append((side, data))
                self.changed.notify_all()
            if not relayed:
                break
        for sock in (source, target):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def stream(self, side):
        return b"".join(data for sender, data in self.chunks if sender == side)

    def wait_for(self, side, needle):
        """Block until the bytes relayed from `side` contain `needle`."""
        end = time.monotonic() + WAIT
        with self.changed:
            while needle not in self.stream(side):
                left = end - time.monotonic()
                if left <= 0:
                    raise TimeoutError(f"{side} never sent {needle!r}")
                self.changed.wait(left)

    def finish(self):
        self.acceptor.join(WAIT)
        for pump in self.pumps:
            pump.join(WAIT)
        self.listener.close()


class Log:
    """What the applications did, in order."""

    def __init__(self):
        self.events = []

    def send(self, sock, side, frames):
        sock.send_multipart(frames)
        self.events.append({"side": side, "action": "send", "frames": hexes(frames)})

    def recv(self, sock, side):
        if not sock.poll(int(WAIT * 1000)):
            raise TimeoutError(f"{side} received nothing")
        frames = sock.recv_multipart()
        self.events.append({"side": side, "action": "recv", "frames": hexes(frames)})
        return frames

    def option(self, sock, side, name, value):
        sock.setsockopt(getattr(zmq, name), value)
        self.events.append({"side": side, "action": "setsockopt", "option": name, "value": printable(value)})


def hexes(frames):
    return [frame.hex() for frame in frames]


def printable(value):
    return value.decode() if isinstance(value, bytes) else value


def apply_options(sock, options):
    for name, value in options.items():
        sock.setsockopt(getattr(zmq, name), value)


def fixture(name, description, client, server, log, tap, **extra):
    return {
        "name": name,
        "description": description,
        "libzmq": zmq.zmq_version(),
        "pyzmq": zmq.__version__,
        "client": client,
        "server": server,
        **extra,
        "events": log.events,
        "clientToServer": tap.stream("client").hex(),
        "serverToClient": tap.stream("server").hex(),
        "chunks": [{"from": side, "hex": data.hex()} for side, data in tap.chunks],
    }


def side_info(socket_type, role, options):
    return {"socketType": socket_type, "role": role,
            "options": {name: printable(value) for name, value in options.items()}}


def run_pair(name, description, client_type, server_type, body,
             client_options=None, server_options=None, plain_passwords=None, **extra):
    """Bind `server_type`, connect `client_type` through a tap, run `body`."""
    client_options = client_options or {}
    server_options = server_options or {}
    context = zmq.Context()
    authenticator = None
    if plain_passwords:
        authenticator = ThreadAuthenticator(context)
        authenticator.start()
        authenticator.configure_plain(domain="*", passwords=plain_passwords)
    server = context.socket(getattr(zmq, server_type))
    apply_options(server, server_options)
    server.bind("tcp://127.0.0.1:*")
    port = int(server.getsockopt_string(zmq.LAST_ENDPOINT).rsplit(":", 1)[1])
    tap = Tap(port)
    client = context.socket(getattr(zmq, client_type))
    apply_options(client, client_options)
    client.connect(f"tcp://127.0.0.1:{tap.port}")
    log = Log()
    try:
        body(client, server, tap, log)
    finally:
        client.close(linger=0)
        server.close(linger=0)
        tap.finish()
        if authenticator:
            authenticator.stop()
        context.term()
    return fixture(name, description,
                   side_info(client_type, "connect", client_options),
                   side_info(server_type, "bind", server_options), log, tap, **extra)


# --- scenarios -------------------------------------------------------------

def null_push_pull():
    def body(client, server, tap, log):
        for frames in ([b"hello"], [b"part1", b"part2"], [b"x" * 300]):
            log.send(client, "client", frames)
        for _ in range(3):
            log.recv(server, "server")
    return run_pair("null-push-pull",
                    "NULL. A single frame, two frames (MORE), and a 300-byte frame (long size).",
                    "PUSH", "PULL", body)


def null_pub_sub():
    def body(client, server, tap, log):
        log.option(client, "client", "SUBSCRIBE", b"w")
        tap.wait_for("client", b"SUBSCRIBEw")
        time.sleep(0.1)  # let the PUB's I/O thread apply the subscription
        log.send(server, "server", [b"x skipped"])  # filtered by the PUB: never on the wire
        log.send(server, "server", [b"w hello"])
        log.recv(client, "client")
        log.option(client, "client", "UNSUBSCRIBE", b"w")
        tap.wait_for("client", b"CANCELw")
    return run_pair("null-pub-sub",
                    "NULL, ZMTP 3.1 on both sides. SUBSCRIBE and CANCEL go as commands; "
                    "the PUB filters, so 'x skipped' never reaches the wire.",
                    "SUB", "PUB", body)


def null_req_rep():
    def body(client, server, tap, log):
        log.send(client, "client", [b"ping"])
        log.recv(server, "server")
        log.send(server, "server", [b"pong"])
        log.recv(client, "client")
    return run_pair("null-req-rep",
                    "NULL. REQ and REP put an empty delimiter frame before the body on the wire.",
                    "REQ", "REP", body)


def null_dealer_router():
    def body(client, server, tap, log):
        log.send(client, "client", [b"hello"])
        identity, _ = log.recv(server, "server")
        log.send(server, "server", [identity, b"world"])
        log.recv(client, "client")
    return run_pair("null-dealer-router",
                    "NULL. The DEALER sets ROUTING_ID, which travels as the Identity property of READY.",
                    "DEALER", "ROUTER", body, client_options={"ROUTING_ID": b"dealer-1"})


def null_pair():
    def body(client, server, tap, log):
        log.send(client, "client", [b"hi"])
        log.recv(server, "server")
        log.send(server, "server", [b"hey"])
        log.recv(client, "client")
    return run_pair("null-pair", "NULL. One message each way.", "PAIR", "PAIR", body)


def null_heartbeat():
    def body(client, server, tap, log):
        log.send(client, "client", [b"hello"])
        log.recv(server, "server")
        tap.wait_for("server", b"\x04PONG")
    return run_pair("null-heartbeat",
                    "NULL. The client sends PING (TTL in tenths of a second) and the server answers PONG.",
                    "PUSH", "PULL", body,
                    client_options={"HEARTBEAT_IVL": 100, "HEARTBEAT_TTL": 500, "HEARTBEAT_TIMEOUT": 300})


def plain_push_pull():
    def body(client, server, tap, log):
        log.send(client, "client", [b"hello"])
        log.recv(server, "server")
    return run_pair("plain-push-pull",
                    "PLAIN (24/ZMTP-PLAIN): HELLO, WELCOME, INITIATE, READY. A ZAP handler accepts admin/secret.",
                    "PUSH", "PULL", body,
                    client_options={"PLAIN_USERNAME": b"admin", "PLAIN_PASSWORD": b"secret"},
                    server_options={"PLAIN_SERVER": 1},
                    plain_passwords={"admin": "secret"})


def curve_push_pull():
    server_public, server_secret = zmq.curve_keypair()
    client_public, client_secret = zmq.curve_keypair()

    def body(client, server, tap, log):
        log.send(client, "client", [b"hello"])
        log.recv(server, "server")
    return run_pair("curve-push-pull",
                    "CURVE (25/ZMTP-CURVE, 26/CurveZMQ): HELLO, WELCOME, INITIATE, READY, then one MESSAGE. "
                    "No ZAP handler, so any client key is accepted. Keys are Z85 test keys made for this recording; "
                    "short-term keys and nonces are random.",
                    "PUSH", "PULL", body,
                    client_options={"CURVE_SERVERKEY": server_public, "CURVE_PUBLICKEY": client_public,
                                    "CURVE_SECRETKEY": client_secret},
                    server_options={"CURVE_SERVER": 1, "CURVE_SECRETKEY": server_secret},
                    keys={"serverPublic": server_public.decode(), "serverSecret": server_secret.decode(),
                          "clientPublic": client_public.decode(), "clientSecret": client_secret.decode()})


def zmtp_frame(body, more=False, command=False):
    flags = (0x01 if more else 0) | (0x04 if command else 0)
    if len(body) > 255:
        return bytes([flags | 0x02]) + struct.pack(">Q", len(body)) + body
    return bytes([flags, len(body)]) + body


def zmtp_command(name, properties=()):
    body = bytes([len(name)]) + name
    for key, value in properties:
        body += bytes([len(key)]) + key + struct.pack(">I", len(value)) + value
    return zmtp_frame(body, command=True)


def zmtp30_sub():
    """libzmq SUB against a fake ZMTP 3.0 PUB: subscriptions become 0x01 / 0x00 messages."""
    listener = socket.create_server(("127.0.0.1", 0))
    listener.settimeout(WAIT)
    tap = Tap(listener.getsockname()[1])
    context = zmq.Context()
    client = context.socket(zmq.SUB)
    client.connect(f"tcp://127.0.0.1:{tap.port}")
    log = Log()
    peer = None
    try:
        peer, _ = listener.accept()
        greeting = (b"\xff" + bytes(8) + b"\x7f" + bytes([3, 0]) + b"NULL".ljust(20, b"\x00")
                    + b"\x00" + bytes(31))
        peer.sendall(greeting + zmtp_command(b"READY", [(b"Socket-Type", b"PUB")]))
        log.option(client, "client", "SUBSCRIBE", b"w")
        tap.wait_for("client", b"\x00\x02\x01w")
        peer.sendall(zmtp_frame(b"w hello"))
        log.events.append({"side": "server", "action": "send", "frames": hexes([b"w hello"])})
        log.recv(client, "client")
        log.option(client, "client", "UNSUBSCRIBE", b"w")
        tap.wait_for("client", b"\x00\x02\x00w")
    finally:
        client.close(linger=0)
        if peer:
            peer.close()
        tap.finish()
        listener.close()
        context.term()
    return fixture("zmtp30-sub",
                   "libzmq SUB (ZMTP 3.1) against a fake ZMTP 3.0 PUB. libzmq sends the subscription as a "
                   "message starting with 0x01 and the cancel with 0x00, not as commands.",
                   side_info("SUB", "connect", {}),
                   {"socketType": "PUB", "role": "bind", "options": {},
                    "implementation": "fake ZMTP 3.0 peer in Tools/interop/record_fixtures.py"},
                   log, tap)


SCENARIOS = {
    "null-push-pull": null_push_pull,
    "null-pub-sub": null_pub_sub,
    "null-req-rep": null_req_rep,
    "null-dealer-router": null_dealer_router,
    "null-pair": null_pair,
    "null-heartbeat": null_heartbeat,
    "plain-push-pull": plain_push_pull,
    "curve-push-pull": curve_push_pull,
    "zmtp30-sub": zmtp30_sub,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", nargs="+", choices=sorted(SCENARIOS), metavar="NAME",
                        help="record only these: " + ", ".join(SCENARIOS))
    args = parser.parse_args()
    FIXTURES.mkdir(parents=True, exist_ok=True)
    for name in args.only or SCENARIOS:
        recorded = SCENARIOS[name]()
        path = FIXTURES / f"{name}.json"
        path.write_text(json.dumps(recorded, indent=2) + "\n")
        print(f"{name}: client {len(recorded['clientToServer']) // 2} bytes, "
              f"server {len(recorded['serverToClient']) // 2} bytes, {len(recorded['chunks'])} chunks")


if __name__ == "__main__":
    main()
