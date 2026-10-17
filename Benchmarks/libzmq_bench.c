/*
 * libzmq side of zmq-bench: the same two tests as Sources/zmq-bench/Bench.swift, over
 * tcp://127.0.0.1:*, in one process with two threads.
 *
 *   libzmq_bench thr <count> <size>   PULL (main thread) binds, PUSH (second thread) connects.
 *                                     Time from the first receive to the last; count-1 messages timed.
 *   libzmq_bench lat <count> <size>   REP (second thread) echoes, REQ (main thread) connects.
 *                                     500 warm-up round trips, then count timed round trips.
 *
 * Socket options are libzmq's defaults (HWM 1000), except linger, so a failed run cannot hang.
 */
#include <zmq.h>

#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define WARMUP 500

static void die(const char *what)
{
    fprintf(stderr, "error: %s: %s\n", what, zmq_strerror(zmq_errno()));
    exit(1);
}

#define CHECK(call)                                                                                \
    do {                                                                                           \
        if ((call) < 0)                                                                            \
            die(#call);                                                                            \
    } while (0)

static uint64_t now_ns(void) { return clock_gettime_nsec_np(CLOCK_MONOTONIC_RAW); }

static void *must(void *p, const char *what)
{
    if (!p)
        die(what);
    return p;
}

static void set_linger(void *socket)
{
    int linger = 1000;
    CHECK(zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof linger));
}

/* Binds `socket` to tcp://127.0.0.1:* and returns the endpoint actually bound. */
static void bind_any(void *socket, char *endpoint, size_t capacity)
{
    CHECK(zmq_bind(socket, "tcp://127.0.0.1:*"));
    size_t length = capacity;
    CHECK(zmq_getsockopt(socket, ZMQ_LAST_ENDPOINT, endpoint, &length));
}

struct worker {
    void *ctx;
    void *socket; /* lat: the bound REP socket; thr: the PUSH socket, set by its thread */
    char endpoint[256];
    long count;
    size_t size;
    pthread_t thread;
};

static void start(struct worker *w, void *(*body)(void *))
{
    if (pthread_create(&w->thread, NULL, body, w) != 0) {
        fprintf(stderr, "error: pthread_create\n");
        exit(1);
    }
}

/* ---- thr ---- */

static void *push_thread(void *arg)
{
    struct worker *w = arg;
    char *payload = malloc(w->size);
    memset(payload, 'a', w->size);
    void *push = must(zmq_socket(w->ctx, ZMQ_PUSH), "zmq_socket(PUSH)");
    set_linger(push);
    CHECK(zmq_connect(push, w->endpoint));
    for (long i = 0; i < w->count; i++) {
        if (zmq_send(push, payload, w->size, 0) != (int)w->size)
            die("zmq_send");
    }
    /* Not closed here: libzmq 4.3.5 on macOS loses data still in flight when the sender closes right
     * after its last send (the receiver stalls). The main thread closes it after the receive loop. */
    w->socket = push;
    free(payload);
    return NULL;
}

static void run_thr(long count, size_t size)
{
    struct worker w = {.ctx = must(zmq_ctx_new(), "zmq_ctx_new"), .count = count, .size = size};
    void *pull = must(zmq_socket(w.ctx, ZMQ_PULL), "zmq_socket(PULL)");
    set_linger(pull);
    bind_any(pull, w.endpoint, sizeof w.endpoint);
    start(&w, push_thread);

    char *buffer = malloc(size);
    if (zmq_recv(pull, buffer, size, 0) != (int)size)
        die("zmq_recv");
    uint64_t start_ns = now_ns();
    for (long i = 1; i < count; i++) {
        if (zmq_recv(pull, buffer, size, 0) != (int)size)
            die("zmq_recv");
    }
    uint64_t elapsed_ns = now_ns() - start_ns;

    pthread_join(w.thread, NULL);
    free(buffer);
    CHECK(zmq_close(w.socket));
    CHECK(zmq_close(pull));
    CHECK(zmq_ctx_term(w.ctx));

    double seconds = (double)elapsed_ns / 1e9;
    double msg_per_sec = (double)(count - 1) / seconds;
    double mb_per_sec = msg_per_sec * (double)size / 1e6;
    printf("libzmq  thr count=%ld size=%zu: %.0f msg/s, %.2f MB/s (%.3f s)\n", count, size, msg_per_sec,
           mb_per_sec, seconds);
    printf("{\"impl\":\"libzmq\",\"test\":\"thr\",\"count\":%ld,\"size\":%zu,\"msgPerSec\":%.0f,"
           "\"mbPerSec\":%.2f}\n",
           count, size, msg_per_sec, mb_per_sec);
}

/* ---- lat ---- */

static void *rep_thread(void *arg)
{
    struct worker *w = arg;
    char *buffer = malloc(w->size);
    for (long i = 0; i < WARMUP + w->count; i++) {
        if (zmq_recv(w->socket, buffer, w->size, 0) != (int)w->size)
            die("zmq_recv");
        if (zmq_send(w->socket, buffer, w->size, 0) != (int)w->size)
            die("zmq_send");
    }
    free(buffer); /* the REP socket is closed by the main thread, after the last reply has arrived */
    return NULL;
}

static int compare_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y;
}

/* Nearest-rank percentile, as in the Swift side. */
static double percentile_us(const uint64_t *sorted, long n, double p)
{
    long index = (long)ceil(p * (double)n) - 1;
    if (index < 0)
        index = 0;
    return (double)sorted[index] / 1e3;
}

static void run_lat(long count, size_t size)
{
    struct worker w = {.ctx = must(zmq_ctx_new(), "zmq_ctx_new"), .count = count, .size = size};
    w.socket = must(zmq_socket(w.ctx, ZMQ_REP), "zmq_socket(REP)");
    set_linger(w.socket);
    bind_any(w.socket, w.endpoint, sizeof w.endpoint);
    start(&w, rep_thread); /* the REP socket now belongs to the second thread */

    void *req = must(zmq_socket(w.ctx, ZMQ_REQ), "zmq_socket(REQ)");
    set_linger(req);
    CHECK(zmq_connect(req, w.endpoint));

    char *payload = malloc(size);
    char *reply = malloc(size);
    memset(payload, 'a', size);
    uint64_t *rtts = malloc(sizeof(uint64_t) * (size_t)count);
    for (long i = 0; i < WARMUP + count; i++) {
        uint64_t begin = now_ns();
        if (zmq_send(req, payload, size, 0) != (int)size)
            die("zmq_send");
        if (zmq_recv(req, reply, size, 0) != (int)size)
            die("zmq_recv");
        uint64_t end = now_ns();
        if (i >= WARMUP)
            rtts[i - WARMUP] = end - begin;
    }

    pthread_join(w.thread, NULL);
    CHECK(zmq_close(req));
    CHECK(zmq_close(w.socket));
    CHECK(zmq_ctx_term(w.ctx));

    qsort(rtts, (size_t)count, sizeof(uint64_t), compare_u64);
    double sum = 0;
    for (long i = 0; i < count; i++)
        sum += (double)rtts[i];
    double mean = sum / (double)count / 1e3;
    double p50 = percentile_us(rtts, count, 0.50);
    double p99 = percentile_us(rtts, count, 0.99);
    free(rtts);
    free(payload);
    free(reply);

    printf("libzmq  lat count=%ld size=%zu: mean %.2f us, p50 %.2f us, p99 %.2f us\n", count, size, mean,
           p50, p99);
    printf("{\"impl\":\"libzmq\",\"test\":\"lat\",\"count\":%ld,\"size\":%zu,\"meanUs\":%.2f,"
           "\"p50Us\":%.2f,\"p99Us\":%.2f}\n",
           count, size, mean, p50, p99);
}

int main(int argc, char **argv)
{
    long count = argc == 4 ? atol(argv[2]) : 0;
    long size = argc == 4 ? atol(argv[3]) : 0;
    if (count < 2 || size < 1 || (strcmp(argv[1], "thr") != 0 && strcmp(argv[1], "lat") != 0)) {
        fprintf(stderr, "usage: libzmq_bench thr|lat <count >= 2> <size >= 1>\n");
        return 64;
    }
    if (strcmp(argv[1], "thr") == 0)
        run_thr(count, (size_t)size);
    else
        run_lat(count, (size_t)size);
    return 0;
}
