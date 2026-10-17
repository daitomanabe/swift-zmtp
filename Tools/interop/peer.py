#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyzmq==27.2.0"]
# ///
"""A libzmq peer for interop tests (pyzmq 27.2.0 bundles libzmq 4.3.5).

Prints one JSON object per line on stdout:
  {"event": "ready", "endpoints": [...], "libzmq": "4.3.5", "pyzmq": "27.2.0", "pid": N}
  {"event": "subscription", "frames": [hex]}     XPUB with --wait-sub
  {"event": "sent", "frames": [hex, ...]}
  {"event": "recv", "frames": [hex, ...]}
  {"event": "step", "action": "..."}              after each --then step
  {"event": "done"} | {"event": "timeout"} | {"event": "error", "message": "..."}
Exit status: 0 done, 1 error, 2 timeout.

Frames are hex. A message on the command line is its frames in hex joined by
commas: "68656c6c6f" is one frame, ",6869" is an empty frame and then "hi".

Steps run in this order: bind / connect, ready, --wait-sub, each --send,
--recv N, --echo N, then each --then in the order given. Bind to
"tcp://127.0.0.1:*" to get a free port; the "ready" line carries the port that
was picked (and the process id, for tests that stop and continue the peer).

--then ACTION runs more steps after the ones above, in command-line order:
  pause:SECONDS     wait, still serving the socket (the peer's --timeout still applies)
  subscribe:HEX     SUB: add a subscription (HEX may be empty)
  unsubscribe:HEX   SUB: take one subscription back
  send:MESSAGE      send a message (frames in hex joined by commas)
  recv:N            receive N messages, printing a "recv" event for each
  drain:SECONDS     receive whatever arrives for SECONDS (a "recv" event for each; none is
                    fine). For tests that show nothing is delivered, e.g. after a refused login.
  offer:SECONDS:MESSAGE
                    try to send MESSAGE without blocking every 20 ms for SECONDS (a socket
                    with no ready peer refuses it; that is fine), then print one
                    {"event": "offered", "sent": N, "refused": M}. A bound PUSH would block
                    in "send:" forever while its peer's login is refused.

ZAP (a libzmq server that checks PLAIN passwords or CURVE client keys). The authenticator is
pyzmq's ThreadAuthenticator, started before the socket is created and stopped after it is closed:
  --zap-plain USER:PASS   PLAIN: accept only this account (may be given more than once)
  --zap-curve any         CURVE: accept every client key
  --zap-curve HEXKEY      CURVE: accept only this client public key (32 bytes in hex; may be given
                          more than once). Server side also needs --sockopt PLAIN_SERVER=1 or
                          --sockopt CURVE_SERVER=1.
CURVE keys go in raw, e.g. --sockopt-hex CURVE_SECRETKEY=<64 hex digits>.

Examples:
  uv run Tools/interop/peer.py --type PULL --bind 'tcp://127.0.0.1:*' --recv 1
  uv run Tools/interop/peer.py --type REQ --connect tcp://127.0.0.1:5555 --send 70696e67 --recv 1
  uv run Tools/interop/peer.py --type XPUB --bind 'tcp://127.0.0.1:*' --wait-sub 1 --send 772068656c6c6f
  uv run Tools/interop/peer.py --type PULL --bind 'tcp://127.0.0.1:*' --sockopt PLAIN_SERVER=1 \\
      --zap-plain admin:secret --recv 1
  uv run Tools/interop/peer.py --type PUSH --connect tcp://127.0.0.1:5555 \\
      --sockopt HEARTBEAT_IVL=100 --sockopt PLAIN_USERNAME=admin --sockopt PLAIN_PASSWORD=secret --send 6869
  uv run Tools/interop/peer.py --type SUB --connect tcp://127.0.0.1:5555 --subscribe 61 \\
      --then pause:0.4 --then subscribe:61 --then pause:0.4 --then unsubscribe:61 --then unsubscribe:61
"""
import argparse
import json
import os
import sys
import time

import zmq
import zmq.auth
import zmq.auth.thread
from zmq.utils import z85


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)


def hexes(frames):
    return [frame.hex() for frame in frames]


def parse_message(text):
    return [bytes.fromhex(part) for part in text.split(",")]


def set_option(sock, spec, hex_value=False):
    name, _, value = spec.partition("=")
    option = getattr(zmq, name.upper())
    if hex_value:
        sock.setsockopt(option, bytes.fromhex(value))
    elif value.lstrip("-").isdigit():
        sock.setsockopt(option, int(value))
    else:
        sock.setsockopt(option, value.encode())


def recv_frames(sock, deadline):
    remaining_ms = int((deadline - time.monotonic()) * 1000)
    if remaining_ms <= 0 or not sock.poll(remaining_ms):
        raise TimeoutError
    return sock.recv_multipart()


class AllowedClientKeys:
    """A ZAP credentials provider for CURVE: allows exactly the given client public keys."""

    def __init__(self, hex_keys):
        # pyzmq hands the callback the client key as Z85 text (bytes).
        self.keys = {z85.encode(bytes.fromhex(key)) for key in hex_keys}

    def callback(self, domain, key):
        return key in self.keys


def start_authenticator(context, zap_plain, zap_curve):
    """Starts a ThreadAuthenticator for --zap-plain / --zap-curve; None when neither is given."""
    if not zap_plain and not zap_curve:
        return None
    auth = zmq.auth.thread.ThreadAuthenticator(context)
    auth.start()
    if zap_plain:
        accounts = {}
        for spec in zap_plain:
            user, separator, password = spec.partition(":")
            if not separator:
                raise ValueError(f"--zap-plain expects USER:PASS, not {spec!r}")
            accounts[user] = password
        auth.configure_plain(domain="*", passwords=accounts)
    if zap_curve:
        if "any" in zap_curve:
            if len(zap_curve) > 1:
                raise ValueError("--zap-curve any cannot be combined with keys")
            auth.configure_curve(domain="*", location=zmq.auth.CURVE_ALLOW_ANY)
        else:
            auth.configure_curve_callback(domain="*", credentials_provider=AllowedClientKeys(zap_curve))
    return auth


def run_step(sock, step, deadline):
    """One --then action: pause:SECONDS, subscribe:HEX, unsubscribe:HEX, send:MESSAGE, recv:N, drain:SECONDS or offer:SECONDS:MESSAGE."""
    action, _, value = step.partition(":")
    if action == "pause":
        if time.monotonic() + float(value) > deadline:
            raise TimeoutError
        # Not a plain sleep: libzmq handles a socket's pending commands (a connection accepted on a
        # bound socket gets its pipe, a SUB sends its subscriptions) only in calls made by the thread
        # that owns the socket. Reading EVENTS is such a call and does not touch any message.
        end = time.monotonic() + float(value)
        while time.monotonic() < end:
            sock.getsockopt(zmq.EVENTS)
            time.sleep(0.01)
    elif action == "subscribe":
        sock.setsockopt(zmq.SUBSCRIBE, bytes.fromhex(value))
    elif action == "unsubscribe":
        sock.setsockopt(zmq.UNSUBSCRIBE, bytes.fromhex(value))
    elif action == "send":
        frames = parse_message(value)
        sock.send_multipart(frames)
        emit("sent", frames=hexes(frames))
    elif action == "recv":
        for _ in range(int(value)):
            emit("recv", frames=hexes(recv_frames(sock, deadline)))
    elif action == "drain":
        if time.monotonic() + float(value) > deadline:
            raise TimeoutError
        end = time.monotonic() + float(value)
        while time.monotonic() < end:
            # poll() also lets libzmq handle the socket's pending commands (see "pause").
            if sock.poll(10):
                emit("recv", frames=hexes(sock.recv_multipart()))
    elif action == "offer":
        seconds, _, text = value.partition(":")
        if time.monotonic() + float(seconds) > deadline:
            raise TimeoutError
        frames = parse_message(text)
        end = time.monotonic() + float(seconds)
        sent = refused = 0
        while time.monotonic() < end:
            try:
                sock.send_multipart(frames, flags=zmq.NOBLOCK)
                sent += 1
            except zmq.Again:
                refused += 1
            time.sleep(0.02)
        emit("offered", sent=sent, refused=refused)
    else:
        raise ValueError(f"unknown --then action {step!r}")
    emit("step", action=step)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--type", required=True, help="PUSH, PULL, PUB, SUB, XPUB, XSUB, REQ, REP, DEALER, ROUTER, PAIR")
    parser.add_argument("--bind", action="append", default=[], metavar="ENDPOINT")
    parser.add_argument("--connect", action="append", default=[], metavar="ENDPOINT")
    parser.add_argument("--sockopt", action="append", default=[], metavar="NAME=VALUE",
                        help="socket option; an integer, or else UTF-8 bytes")
    parser.add_argument("--sockopt-hex", action="append", default=[], metavar="NAME=HEX",
                        help="socket option given as bytes in hex")
    parser.add_argument("--subscribe", action="append", default=[], metavar="HEX",
                        help="SUB topic prefix in hex; '' subscribes to everything")
    parser.add_argument("--wait-sub", type=int, default=0, metavar="N",
                        help="XPUB: wait for N subscription messages before sending")
    parser.add_argument("--send", action="append", default=[], metavar="MESSAGE")
    parser.add_argument("--recv", type=int, default=0, metavar="N")
    parser.add_argument("--echo", type=int, default=0, metavar="N",
                        help="receive N messages and send each one back unchanged")
    parser.add_argument("--then", action="append", default=[], metavar="ACTION",
                        help="more steps after --echo, in order: pause:SECONDS, subscribe:HEX, "
                             "unsubscribe:HEX, send:MESSAGE, recv:N, drain:SECONDS, offer:SECONDS:MESSAGE")
    parser.add_argument("--zap-plain", action="append", default=[], metavar="USER:PASS",
                        help="start a ZAP authenticator that accepts this PLAIN account")
    parser.add_argument("--zap-curve", action="append", default=[], metavar="any|HEXKEY",
                        help="start a ZAP authenticator that accepts any CURVE client key, or only this one")
    parser.add_argument("--timeout", type=float, default=10.0, help="seconds for the whole run")
    parser.add_argument("--linger-ms", type=int, default=1000)
    args = parser.parse_args()

    deadline = time.monotonic() + args.timeout
    context = zmq.Context()
    sock = context.socket(getattr(zmq, args.type.upper()))
    sock.setsockopt(zmq.LINGER, args.linger_ms)
    sock.setsockopt(zmq.SNDTIMEO, int(args.timeout * 1000))
    authenticator = None
    status = 0
    try:
        # Before anything binds or connects: a server asks the ZAP handler during its handshake.
        authenticator = start_authenticator(context, args.zap_plain, args.zap_curve)
        for spec in args.sockopt:
            set_option(sock, spec)
        for spec in args.sockopt_hex:
            set_option(sock, spec, hex_value=True)
        for topic in args.subscribe:
            sock.setsockopt(zmq.SUBSCRIBE, bytes.fromhex(topic))
        endpoints = []
        for endpoint in args.bind:
            sock.bind(endpoint)
            endpoints.append(sock.getsockopt_string(zmq.LAST_ENDPOINT))
        for endpoint in args.connect:
            sock.connect(endpoint)
            endpoints.append(endpoint)
        emit("ready", endpoints=endpoints, libzmq=zmq.zmq_version(), pyzmq=zmq.__version__, pid=os.getpid())

        for _ in range(args.wait_sub):
            emit("subscription", frames=hexes(recv_frames(sock, deadline)))
        for text in args.send:
            frames = parse_message(text)
            sock.send_multipart(frames)
            emit("sent", frames=hexes(frames))
        for _ in range(args.recv):
            emit("recv", frames=hexes(recv_frames(sock, deadline)))
        for _ in range(args.echo):
            frames = recv_frames(sock, deadline)
            emit("recv", frames=hexes(frames))
            sock.send_multipart(frames)
            emit("sent", frames=hexes(frames))
        for step in args.then:
            run_step(sock, step, deadline)
    except (TimeoutError, zmq.Again):
        emit("timeout")
        status = 2
    except (zmq.ZMQError, AttributeError, ValueError) as error:
        emit("error", message=str(error))
        status = 1
    finally:
        sock.close()
        # The authenticator's ZAP socket belongs to the context; term() would wait for it.
        if authenticator is not None:
            authenticator.stop()
        context.term()
    if status == 0:
        emit("done")
    return status


if __name__ == "__main__":
    sys.exit(main())
