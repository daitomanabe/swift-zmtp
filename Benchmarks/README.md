# Benchmarks

A small throughput and latency benchmark: the Swift library (`zmq-bench`) and libzmq (`libzmq_bench.c`), same two tests, same output.
Both run in one process over `tcp://127.0.0.1:*` with default socket options (high-water mark 1000).

- `thr <count> <size>`: PUSH sends `count` single-frame messages to a PULL. Time from the first receive to the last (`count - 1` messages); prints msg/s and MB/s (1 MB = 10^6 bytes).
- `lat <count> <size>`: REQ/REP round trips of `size` bytes after 500 warm-up round trips; prints mean, p50 and p99 in microseconds.
- Each run also prints one JSON line (`"impl":"swift"` or `"libzmq"`).

```sh
# Swift (from Benchmarks/)
swift run -c release zmq-bench thr 200000 16
swift run -c release zmq-bench lat 10000 16

# libzmq (needs `brew install zeromq pkg-config`), from Benchmarks/
mkdir -p .build
clang -O2 libzmq_bench.c $(pkg-config --cflags --libs libzmq) -o .build/libzmq_bench
.build/libzmq_bench thr 200000 16
.build/libzmq_bench lat 10000 16
```

Run them one at a time with nothing else heavy running. A Swift run that gives no result within 120 s stops with an error.
