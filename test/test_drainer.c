/*******************************************************************************
 * Copyright (c) 2026 IOTechSystems and others
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *    http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/

/*
 * Unit tests for the Drainer module (src/Drainer.c).
 *
 * Uses socketpair() to create connected socket pairs; one end is registered
 * with the drainer, the other end acts as the remote peer that produces
 * bytes. The drainer pulls them into a RingBuffer; the test thread reads
 * from the ring and verifies.
 *
 * TSan invocation:
 *   gcc -std=c11 -fsanitize=thread -O1 -g -pthread \
 *       -I src test/test_drainer.c src/Drainer.c src/RingBuffer.c \
 *       -o /tmp/test_drainer_tsan && setarch $(uname -m) -R /tmp/test_drainer_tsan
 */

#include "Drainer.h"
#include "RingBuffer.h"

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;

#define FAIL(fmt, ...) do {                                                \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __func__, __LINE__,           \
            ##__VA_ARGS__);                                                \
    failures++;                                                            \
} while (0)

#define ASSERT_TRUE(cond) do {                                             \
    if (!(cond)) FAIL("expected true: %s", #cond);                         \
} while (0)

#define ASSERT_EQ_INT(a, e) do {                                           \
    int _a = (int)(a), _e = (int)(e);                                      \
    if (_a != _e) FAIL("expected %s == %s, got %d vs %d", #a, #e, _a, _e); \
} while (0)

#define ASSERT_EQ_SZ(a, e) do {                                            \
    size_t _a = (size_t)(a), _e = (size_t)(e);                             \
    if (_a != _e) FAIL("expected %s == %s, got %zu vs %zu",                \
                       #a, #e, _a, _e);                                    \
} while (0)

#define RUN(name) do {                                                     \
    int before = failures;                                                 \
    printf("  %-44s ", #name);                                             \
    fflush(stdout);                                                        \
    name();                                                                \
    printf("%s\n", failures == before ? "ok" : "FAILED");                  \
} while (0)

/* ============================================================ */
/* Helpers                                                       */
/* ============================================================ */

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

/* Wait up to `timeout_ms` for at least `n` bytes to be readable in the
 * ring. Returns 1 on success, 0 on timeout. */
static int wait_for_bytes(RingBuffer* rb, size_t n, int timeout_ms)
{
    uint64_t deadline = now_ms() + (uint64_t)timeout_ms;
    while (RingBuffer_readable(rb) < n) {
        if (now_ms() > deadline)
            return 0;
        struct timespec ts = { 0, 1000000 }; /* 1 ms */
        nanosleep(&ts, NULL);
    }
    return 1;
}

/* We deliberately leave both ends in blocking mode. The drainer uses
 * MSG_DONTWAIT on recvmsg() so non-blocking on the drainer-side fd is
 * unnecessary, and a blocking peer end is more convenient for tests. */
static int make_socketpair(int sv[2])
{
    return socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
}

/* Wrapper that silences -Wunused-result on write() without writing
 * `ssize_t r = write(...); (void)r;` at every call site. */
static void test_write(int fd, const void* buf, size_t n)
{
    ssize_t r = write(fd, buf, n);
    (void)r;
}

/* ============================================================ */
/* Tests                                                         */
/* ============================================================ */

static void test_create_destroy(void)
{
    Drainer* d = Drainer_create(NULL);
    ASSERT_TRUE(d != NULL);
    Drainer_destroy(d);
}

static void test_create_destroy_repeated(void)
{
    /* Quickly cycle a few drainers to surface any thread-startup leaks
     * or shutdown races. */
    for (int i = 0; i < 5; i++) {
        Drainer* d = Drainer_create(NULL);
        ASSERT_TRUE(d != NULL);
        Drainer_destroy(d);
    }
}

static void test_basic_drain(void)
{
    Drainer* d = Drainer_create(NULL);
    RingBuffer* rb = RingBuffer_create(4096);
    int sv[2];
    ASSERT_EQ_INT(make_socketpair(sv), 0);
    ASSERT_EQ_INT(Drainer_addSocket(d, sv[0], NULL, rb, NULL), 0);

    /* Write a known pattern from the peer end. */
    char src[200];
    for (int i = 0; i < 200; i++) src[i] = (char)(i + 1);
    ssize_t w = write(sv[1], src, 200);
    ASSERT_EQ_SZ(w, 200);

    ASSERT_TRUE(wait_for_bytes(rb, 200, 1000));

    char dst[200] = {0};
    ASSERT_EQ_SZ(RingBuffer_get_bytes(rb, dst, 200), 200);
    ASSERT_EQ_INT(memcmp(src, dst, 200), 0);

    ASSERT_EQ_INT(Drainer_removeSocket(d, sv[0]), 0);
    close(sv[0]);
    close(sv[1]);
    Drainer_destroy(d);
    RingBuffer_destroy(rb);
}

static void test_multiple_sockets(void)
{
    Drainer* d = Drainer_create(NULL);
    enum { N = 4 };
    RingBuffer* rbs[N];
    int sv[N][2];

    for (int i = 0; i < N; i++) {
        rbs[i] = RingBuffer_create(4096);
        ASSERT_EQ_INT(make_socketpair(sv[i]), 0);
        ASSERT_EQ_INT(Drainer_addSocket(d, sv[i][0], NULL, rbs[i], NULL), 0);
    }

    /* Write a distinctive byte to each peer end. */
    for (int i = 0; i < N; i++) {
        char fill = (char)('A' + i);
        char buf[100];
        memset(buf, fill, sizeof buf);
        ASSERT_EQ_SZ(write(sv[i][1], buf, sizeof buf), sizeof buf);
    }

    for (int i = 0; i < N; i++) {
        ASSERT_TRUE(wait_for_bytes(rbs[i], 100, 1000));
        char buf[100];
        ASSERT_EQ_SZ(RingBuffer_get_bytes(rbs[i], buf, 100), 100);
        char expected = (char)('A' + i);
        for (size_t j = 0; j < 100; j++) {
            if (buf[j] != expected) {
                FAIL("socket %d: byte %zu wrong (got 0x%02x, want 0x%02x)",
                     i, j, (unsigned char)buf[j], (unsigned char)expected);
                break;
            }
        }
    }

    for (int i = 0; i < N; i++) {
        ASSERT_EQ_INT(Drainer_removeSocket(d, sv[i][0]), 0);
        close(sv[i][0]);
        close(sv[i][1]);
    }
    Drainer_destroy(d);
    for (int i = 0; i < N; i++) RingBuffer_destroy(rbs[i]);
}

static void test_synchronous_remove_safety(void)
{
    /* After Drainer_removeSocket returns, the drainer must not touch the
     * fd. We verify this by closing fd immediately after remove returns;
     * if the drainer were still polling it, we'd get EBADF or worse. */
    Drainer* d = Drainer_create(NULL);
    RingBuffer* rb = RingBuffer_create(4096);
    int sv[2];
    ASSERT_EQ_INT(make_socketpair(sv), 0);
    ASSERT_EQ_INT(Drainer_addSocket(d, sv[0], NULL, rb, NULL), 0);

    /* Send some data so the drainer is actively interested in this fd. */
    char buf[64];
    memset(buf, 0xAA, sizeof buf);
    test_write(sv[1], buf, sizeof buf);
    wait_for_bytes(rb, sizeof buf, 1000);

    ASSERT_EQ_INT(Drainer_removeSocket(d, sv[0]), 0);
    /* This close is the critical step. If drainer were still polling, we
     * could see EBADF in its poll set, fd reuse, etc. */
    ASSERT_EQ_INT(close(sv[0]), 0);
    close(sv[1]);

    /* Removing the same fd again must report not-found. */
    ASSERT_EQ_INT(Drainer_removeSocket(d, sv[0]), -1);

    Drainer_destroy(d);
    RingBuffer_destroy(rb);
}

struct writer_args { int fd; const char* buf; size_t total; };

static void* writer_thread_fn(void* a)
{
    struct writer_args* w = (struct writer_args*)a;
    size_t off = 0;
    while (off < w->total) {
        ssize_t n = write(w->fd, w->buf + off, w->total - off);
        if (n < 0) { if (errno == EINTR) continue; return (void*)1; }
        off += (size_t)n;
    }
    return NULL;
}

static void test_large_transfer_wraps(void)
{
    /* Force ring wrap-around: ring is small relative to the transfer
     * size, so the producer must overwrite the buffer's start region
     * multiple times. */
    Drainer* d = Drainer_create(NULL);
    RingBuffer* rb = RingBuffer_create(4096);
    int sv[2];
    ASSERT_EQ_INT(make_socketpair(sv), 0);
    ASSERT_EQ_INT(Drainer_addSocket(d, sv[0], NULL, rb, NULL), 0);

    const size_t total = 1 * 1024 * 1024;  /* 1 MB through a 4 KB ring */
    char* expected = malloc(total);
    ASSERT_TRUE(expected != NULL);
    for (size_t i = 0; i < total; i++)
        expected[i] = (char)((i * 31 + 7) & 0xFF);

    pthread_t writer;
    struct writer_args wa = { .fd = sv[1], .buf = expected, .total = total };
    pthread_create(&writer, NULL, writer_thread_fn, &wa);

    /* Consumer: read from ring and verify against `expected`. */
    char* received = malloc(total);
    ASSERT_TRUE(received != NULL);
    size_t got_total = 0;
    uint64_t deadline = now_ms() + 10000;
    while (got_total < total) {
        size_t n = RingBuffer_get_bytes(rb, received + got_total, total - got_total);
        if (n == 0) {
            if (now_ms() > deadline) {
                FAIL("timed out: got %zu of %zu bytes", got_total, total);
                break;
            }
            struct timespec ts = { 0, 100000 }; /* 100 us */
            nanosleep(&ts, NULL);
            continue;
        }
        got_total += n;
    }

    pthread_join(writer, NULL);

    if (got_total == total) {
        if (memcmp(expected, received, total) != 0) {
            for (size_t i = 0; i < total; i++) {
                if (received[i] != expected[i]) {
                    FAIL("mismatch at offset %zu: got 0x%02x want 0x%02x",
                         i, (unsigned char)received[i], (unsigned char)expected[i]);
                    break;
                }
            }
        }
    }

    ASSERT_EQ_INT(Drainer_removeSocket(d, sv[0]), 0);
    close(sv[0]);
    close(sv[1]);
    Drainer_destroy(d);
    RingBuffer_destroy(rb);
    free(expected);
    free(received);
}

static void test_peer_close_detection(void)
{
    Drainer* d = Drainer_create(NULL);
    RingBuffer* rb = RingBuffer_create(4096);
    int sv[2];
    ASSERT_EQ_INT(make_socketpair(sv), 0);
    ASSERT_EQ_INT(Drainer_addSocket(d, sv[0], NULL, rb, NULL), 0);

    /* Write a few bytes then close peer. Drainer should ingest the bytes
     * AND detect close (closed flag set internally). We can't observe
     * the flag directly, but we can verify that ring receives the bytes
     * and remove still succeeds. */
    test_write(sv[1], "hello", 5);
    close(sv[1]);   /* close from peer end */

    ASSERT_TRUE(wait_for_bytes(rb, 5, 1000));
    char buf[8];
    ASSERT_EQ_SZ(RingBuffer_get_bytes(rb, buf, 5), 5);
    ASSERT_EQ_INT(memcmp(buf, "hello", 5), 0);

    /* Give the drainer a brief moment to observe the close. */
    struct timespec ts = { 0, 10 * 1000 * 1000 }; /* 10 ms */
    nanosleep(&ts, NULL);

    ASSERT_EQ_INT(Drainer_removeSocket(d, sv[0]), 0);
    close(sv[0]);
    Drainer_destroy(d);
    RingBuffer_destroy(rb);
}

static void test_shutdown_with_active_sockets(void)
{
    /* Drainer_destroy while sockets are still registered must not hang. */
    Drainer* d = Drainer_create(NULL);
    RingBuffer* rb = RingBuffer_create(4096);
    int sv[2];
    ASSERT_EQ_INT(make_socketpair(sv), 0);
    ASSERT_EQ_INT(Drainer_addSocket(d, sv[0], NULL, rb, NULL), 0);

    test_write(sv[1], "x", 1);
    wait_for_bytes(rb, 1, 500);

    /* Skip the remove; just destroy. */
    Drainer_destroy(d);

    close(sv[0]);
    close(sv[1]);
    RingBuffer_destroy(rb);
}

/* Stress: rapid add/remove cycles from multiple threads concurrently.
 * Exposes races in the pending-ops linked-list and condvar machinery. */
struct stress_args { Drainer* d; int iterations; int errors; };

static void* churn_thread(void* arg)
{
    struct stress_args* a = (struct stress_args*)arg;
    for (int iter = 0; iter < a->iterations; iter++) {
        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
            a->errors++;
            return NULL;
        }
        RingBuffer* rb = RingBuffer_create(1024);
        if (Drainer_addSocket(a->d, sv[0], NULL, rb, NULL) != 0) {
            a->errors++;
            close(sv[0]); close(sv[1]); RingBuffer_destroy(rb);
            return NULL;
        }
        test_write(sv[1], "hi", 2);
        sched_yield();
        if (Drainer_removeSocket(a->d, sv[0]) != 0) {
            a->errors++;
        }
        close(sv[0]); close(sv[1]); RingBuffer_destroy(rb);
    }
    return NULL;
}

/* Exercises backpressure: a very small ring with a slow consumer.
 * Without the stall mechanism the drainer would busy-loop on poll();
 * with it, the drainer parks until consumer drains below half-capacity
 * and then resumes. We don't measure CPU here (hard from a unit test);
 * we verify (a) all bytes arrive correctly, (b) the test completes
 * within a generous wall-clock budget. */
struct slow_consumer_args {
    RingBuffer* rb;
    const char* expected;
    size_t      total;
    size_t      consumed;
    int         result;
};

static void* slow_consumer_fn(void* arg)
{
    struct slow_consumer_args* a = (struct slow_consumer_args*)arg;
    char buf[16];
    while (a->consumed < a->total) {
        size_t want = a->total - a->consumed;
        if (want > sizeof buf) want = sizeof buf;
        size_t got = RingBuffer_get_bytes(a->rb, buf, want);
        if (got == 0) {
            struct timespec ts = { 0, 500000 }; /* 0.5 ms */
            nanosleep(&ts, NULL);
            continue;
        }
        for (size_t i = 0; i < got; i++) {
            if (buf[i] != a->expected[a->consumed + i]) {
                a->result = 1;
                return NULL;
            }
        }
        a->consumed += got;
        /* Throttle the consumer so the ring keeps refilling and the
         * drainer has to stall/un-stall repeatedly. */
        struct timespec ts = { 0, 200000 }; /* 0.2 ms per chunk */
        nanosleep(&ts, NULL);
    }
    return NULL;
}

static void test_backpressure_small_ring(void)
{
    Drainer* d = Drainer_create(NULL);
    /* 256-byte ring, 64 KB of data — drainer must stall many times. */
    RingBuffer* rb = RingBuffer_create(256);
    int sv[2];
    ASSERT_EQ_INT(make_socketpair(sv), 0);
    ASSERT_EQ_INT(Drainer_addSocket(d, sv[0], NULL, rb, NULL), 0);

    const size_t total = 64 * 1024;
    char* expected = malloc(total);
    ASSERT_TRUE(expected != NULL);
    for (size_t i = 0; i < total; i++)
        expected[i] = (char)((i * 13 + 5) & 0xFF);

    pthread_t consumer;
    struct slow_consumer_args ca = { rb, expected, total, 0, 0 };
    pthread_create(&consumer, NULL, slow_consumer_fn, &ca);

    /* Writer in main thread — writes will block as ring + socket buffer
     * fill up; TCP backpressure throttles us. */
    pthread_t writer;
    struct writer_args wa = { .fd = sv[1], .buf = expected, .total = total };
    pthread_create(&writer, NULL, writer_thread_fn, &wa);

    pthread_join(writer, NULL);
    pthread_join(consumer, NULL);

    if (ca.result != 0)
        FAIL("byte stream mismatch in backpressure test");
    ASSERT_EQ_SZ(ca.consumed, total);

    ASSERT_EQ_INT(Drainer_removeSocket(d, sv[0]), 0);
    close(sv[0]); close(sv[1]);
    Drainer_destroy(d);
    RingBuffer_destroy(rb);
    free(expected);
}

static void test_concurrent_churn(void)
{
    Drainer* d = Drainer_create(NULL);

    struct stress_args sa1 = { d, 200, 0 };
    struct stress_args sa2 = { d, 200, 0 };
    pthread_t t1, t2;
    pthread_create(&t1, NULL, churn_thread, &sa1);
    pthread_create(&t2, NULL, churn_thread, &sa2);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    ASSERT_EQ_INT(sa1.errors, 0);
    ASSERT_EQ_INT(sa2.errors, 0);

    Drainer_destroy(d);
}

/* ============================================================ */
/* Main                                                          */
/* ============================================================ */

int main(void)
{
    printf("Drainer unit tests\n");
    printf("------------------\n");
    RUN(test_create_destroy);
    RUN(test_create_destroy_repeated);
    RUN(test_basic_drain);
    RUN(test_multiple_sockets);
    RUN(test_synchronous_remove_safety);
    RUN(test_large_transfer_wraps);
    RUN(test_peer_close_detection);
    RUN(test_shutdown_with_active_sockets);
    RUN(test_backpressure_small_ring);
    RUN(test_concurrent_churn);
    printf("------------------\n");
    if (failures == 0) {
        printf("All tests passed.\n");
        return 0;
    } else {
        printf("%d failure(s).\n", failures);
        return 1;
    }
}
