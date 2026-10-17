#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyzmq==27.2.0"]
# ///
"""A libzmq peer for interop tests (pyzmq 27.2.0 bundles libzmq 4.3.5).

Prints one JSON object per line on stdout:
  {"event": "ready", "endpoints": [...], "libzmq": "4.3.5", "pyzmq": "27.2.0"}
  {"event": "subscription", "frames": [hex]}     XPUB with --wait-sub
  {"event": "sent", "frames": [hex, ...]}
  {"event": "recv", "frames": [hex, ...]}
  {"event": "done"} | {"event": "timeout"} | {"event": "error", "message": "..."}
Exit status: 0 done, 1 error, 2 timeout.

Frames are hex. A message on the command line is its frames in hex joined by
commas: "68656c6c6f" is one frame, ",6869" is an empty frame and then "hi".

Steps run in this order: bind / connect, ready, --wait-sub, each --send,
--recv N, --echo N. Bind to "tcp://127.0.0.1:*" to get a free port; the
"ready" line carries the port that was picked.

Examples:
  uv run Tools/interop/peer.py --type PULL --bind 'tcp://127.0.0.1:*' --recv 1
  uv run Tools/interop/peer.py --type REQ --connect tcp://127.0.0.1:5555 --send 70696e67 --recv 1
  uv run Tools/interop/peer.py --type XPUB --bind 'tcp://127.0.0.1:*' --wait-sub 1 --send 772068656c6c6f
  uv run Tools/interop/peer.py --type PUSH --connect tcp://127.0.0.1:5555 \\
      --sockopt HEARTBEAT_IVL=100 --sockopt PLAIN_USERNAME=admin --sockopt PLAIN_PASSWORD=secret --send 6869
"""
import argparse
import json
import sys
import time

import zmq


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
    parser.add_argument("--timeout", type=float, default=10.0, help="seconds for the whole run")
    parser.add_argument("--linger-ms", type=int, default=1000)
    args = parser.parse_args()

    deadline = time.monotonic() + args.timeout
    context = zmq.Context()
    sock = context.socket(getattr(zmq, args.type.upper()))
    sock.setsockopt(zmq.LINGER, args.linger_ms)
    sock.setsockopt(zmq.SNDTIMEO, int(args.timeout * 1000))
    status = 0
    try:
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
        emit("ready", endpoints=endpoints, libzmq=zmq.zmq_version(), pyzmq=zmq.__version__)

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
    except (TimeoutError, zmq.Again):
        emit("timeout")
        status = 2
    except (zmq.ZMQError, AttributeError, ValueError) as error:
        emit("error", message=str(error))
        status = 1
    finally:
        sock.close()
        context.term()
    if status == 0:
        emit("done")
    return status


if __name__ == "__main__":
    sys.exit(main())
