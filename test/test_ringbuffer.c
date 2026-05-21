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
 * Unit tests for the SPSC byte ring buffer (src/RingBuffer.c).
 *
 * For a normal debug run, this is built by the project's CMake setup and
 * invoked via `ctest -R ringbuffer-unit`.
 *
 * For correctness validation of memory ordering (mandatory before merging
 * any change to RingBuffer.c), build and run under ThreadSanitizer:
 *
 *   gcc -std=c11 -fsanitize=thread -O1 -g -pthread \
 *       -I src test/test_ringbuffer.c src/RingBuffer.c \
 *       -o /tmp/test_ringbuffer_tsan && /tmp/test_ringbuffer_tsan
 *
 * On newer Linux kernels TSan can refuse to start with "unexpected memory
 * mapping" due to high ASLR entropy; wrap the invocation in setarch:
 *
 *   setarch $(uname -m) -R /tmp/test_ringbuffer_tsan
 *
 * Pass = exit code 0 AND no TSan warnings on stderr.
 */

#include "RingBuffer.h"

#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define FAIL(fmt, ...) do {                                                \
    fprintf(stderr, "FAIL %s:%d: " fmt "\n", __func__, __LINE__,           \
            ##__VA_ARGS__);                                                \
    failures++;                                                            \
} while (0)

#define ASSERT_TRUE(cond) do {                                             \
    if (!(cond)) FAIL("expected true: %s", #cond);                         \
} while (0)

#define ASSERT_EQ_SZ(actual, expected) do {                                \
    size_t _a = (size_t)(actual);                                          \
    size_t _e = (size_t)(expected);                                        \
    if (_a != _e)                                                          \
        FAIL("expected %s == %s, got %zu vs %zu",                          \
             #actual, #expected, _a, _e);                                  \
} while (0)

#define ASSERT_EQ_INT(actual, expected) do {                               \
    int _a = (int)(actual);                                                \
    int _e = (int)(expected);                                              \
    if (_a != _e)                                                          \
        FAIL("expected %s == %s, got %d vs %d",                            \
             #actual, #expected, _a, _e);                                  \
} while (0)

#define RUN(name) do {                                                     \
    int before = failures;                                                 \
    printf("  %-40s ", #name);                                             \
    fflush(stdout);                                                        \
    name();                                                                \
    printf("%s\n", failures == before ? "ok" : "FAILED");                  \
} while (0)

/* ============================================================ */
/* Single-threaded tests                                        */
/* ============================================================ */

static void test_create_rejects_invalid_capacity(void)
{
    ASSERT_TRUE(RingBuffer_create(0) == NULL);
    ASSERT_TRUE(RingBuffer_create(1) == NULL);   /* below minimum */
    ASSERT_TRUE(RingBuffer_create(3) == NULL);   /* not power of two */
    ASSERT_TRUE(RingBuffer_create(7) == NULL);
    ASSERT_TRUE(RingBuffer_create(1000) == NULL);
    ASSERT_TRUE(RingBuffer_create(1023) == NULL);
}

static void test_create_accepts_valid_capacity(void)
{
    RingBuffer* rb;
    rb = RingBuffer_create(2);     ASSERT_TRUE(rb != NULL); RingBuffer_destroy(rb);
    rb = RingBuffer_create(4);     ASSERT_TRUE(rb != NULL); RingBuffer_destroy(rb);
    rb = RingBuffer_create(1024);  ASSERT_TRUE(rb != NULL); RingBuffer_destroy(rb);
    rb = RingBuffer_create(1<<20); ASSERT_TRUE(rb != NULL); RingBuffer_destroy(rb);
}

static void test_empty_state(void)
{
    RingBuffer* rb = RingBuffer_create(64);
    ASSERT_TRUE(rb != NULL);
    ASSERT_EQ_SZ(RingBuffer_capacity(rb), 64);
    ASSERT_EQ_SZ(RingBuffer_readable(rb), 0);
    ASSERT_EQ_SZ(RingBuffer_writable(rb), 64);

    char c;
    ASSERT_EQ_INT(RingBuffer_get_byte(rb, &c), 0);
    ASSERT_EQ_SZ(RingBuffer_get_bytes(rb, &c, 1), 0);

    RingBuffer_destroy(rb);
}

static void test_fill_drain_no_wrap(void)
{
    RingBuffer* rb = RingBuffer_create(64);
    ASSERT_TRUE(rb != NULL);

    char src[40];
    for (int i = 0; i < 40; i++) src[i] = (char)(i + 1);

    /* Write via iovec API */
    struct iovec iov[2];
    int n = RingBuffer_writable_iovecs(rb, iov, 40);
    ASSERT_EQ_INT(n, 1);
    ASSERT_EQ_SZ(iov[0].iov_len, 40);
    memcpy(iov[0].iov_base, src, 40);
    RingBuffer_advance_writer(rb, 40);

    ASSERT_EQ_SZ(RingBuffer_readable(rb), 40);
    ASSERT_EQ_SZ(RingBuffer_writable(rb), 24);

    /* Read via get_bytes */
    char dst[40] = {0};
    ASSERT_EQ_SZ(RingBuffer_get_bytes(rb, dst, 40), 40);
    ASSERT_EQ_INT(memcmp(src, dst, 40), 0);

    ASSERT_EQ_SZ(RingBuffer_readable(rb), 0);
    ASSERT_EQ_SZ(RingBuffer_writable(rb), 64);

    RingBuffer_destroy(rb);
}

static void test_fill_to_capacity(void)
{
    RingBuffer* rb = RingBuffer_create(16);
    ASSERT_TRUE(rb != NULL);

    struct iovec iov[2];
    int n = RingBuffer_writable_iovecs(rb, iov, 100);  /* ask for more than capacity */
    ASSERT_EQ_INT(n, 1);
    ASSERT_EQ_SZ(iov[0].iov_len, 16);     /* clamped to capacity */
    memset(iov[0].iov_base, 0xAB, 16);
    RingBuffer_advance_writer(rb, 16);

    ASSERT_EQ_SZ(RingBuffer_writable(rb), 0);
    ASSERT_EQ_SZ(RingBuffer_readable(rb), 16);

    /* Now full: iovec API returns 0 entries */
    n = RingBuffer_writable_iovecs(rb, iov, 100);
    ASSERT_EQ_INT(n, 0);

    RingBuffer_destroy(rb);
}

static void test_wrap_around_content(void)
{
    /* Capacity 16. Fill 12, drain 12 (head=12, tail=12). Then write 12 more,
     * which must wrap. Read all 12 back, verify byte sequence. */
    RingBuffer* rb = RingBuffer_create(16);
    ASSERT_TRUE(rb != NULL);

    char tmp[16];
    for (int i = 0; i < 12; i++) tmp[i] = (char)i;

    /* First batch: write 12, drain 12 */
    struct iovec iov[2];
    int n = RingBuffer_writable_iovecs(rb, iov, 12);
    ASSERT_EQ_INT(n, 1);
    memcpy(iov[0].iov_base, tmp, 12);
    RingBuffer_advance_writer(rb, 12);

    char drained[12];
    ASSERT_EQ_SZ(RingBuffer_get_bytes(rb, drained, 12), 12);

    /* Second batch: 12 bytes with new pattern. Should wrap (head=12, capacity=16). */
    for (int i = 0; i < 12; i++) tmp[i] = (char)(0x80 | i);

    n = RingBuffer_writable_iovecs(rb, iov, 12);
    ASSERT_EQ_INT(n, 2);
    ASSERT_EQ_SZ(iov[0].iov_len, 4);    /* 16-12 = 4 until end */
    ASSERT_EQ_SZ(iov[1].iov_len, 8);    /* 12 - 4 = 8 from start */
    memcpy(iov[0].iov_base, tmp, 4);
    memcpy(iov[1].iov_base, tmp + 4, 8);
    RingBuffer_advance_writer(rb, 12);

    char read_back[12];
    ASSERT_EQ_SZ(RingBuffer_get_bytes(rb, read_back, 12), 12);
    ASSERT_EQ_INT(memcmp(tmp, read_back, 12), 0);

    RingBuffer_destroy(rb);
}

static void test_iovecs_capped_by_max_bytes(void)
{
    RingBuffer* rb = RingBuffer_create(64);
    ASSERT_TRUE(rb != NULL);

    struct iovec iov[2];
    int n = RingBuffer_writable_iovecs(rb, iov, 20);
    ASSERT_EQ_INT(n, 1);
    ASSERT_EQ_SZ(iov[0].iov_len, 20);

    /* max_bytes = 0 returns 0 even when ring is fully free */
    n = RingBuffer_writable_iovecs(rb, iov, 0);
    ASSERT_EQ_INT(n, 0);

    RingBuffer_destroy(rb);
}

static void test_get_bytes_partial(void)
{
    RingBuffer* rb = RingBuffer_create(64);
    ASSERT_TRUE(rb != NULL);

    struct iovec iov[2];
    RingBuffer_writable_iovecs(rb, iov, 10);
    memset(iov[0].iov_base, 0x55, 10);
    RingBuffer_advance_writer(rb, 10);

    char dst[100];
    /* Ask for 100, only 10 available */
    ASSERT_EQ_SZ(RingBuffer_get_bytes(rb, dst, 100), 10);
    ASSERT_EQ_SZ(RingBuffer_readable(rb), 0);
    for (int i = 0; i < 10; i++)
        ASSERT_EQ_INT((unsigned char)dst[i], 0x55);

    RingBuffer_destroy(rb);
}

static void test_get_byte_sequential(void)
{
    RingBuffer* rb = RingBuffer_create(8);
    ASSERT_TRUE(rb != NULL);

    /* Write 5 bytes wrapping past the end by reading and re-writing */
    char src[20];
    for (int i = 0; i < 20; i++) src[i] = (char)(i + 1);

    /* Write 5, read 5, write 5, read 5, etc. — total 20 bytes through the ring */
    int written = 0, read_count = 0;
    char read_buf[20];
    while (written < 20) {
        struct iovec iov[2];
        int n = RingBuffer_writable_iovecs(rb, iov, 5);
        ASSERT_TRUE(n >= 1);
        size_t to_write = iov[0].iov_len + (n > 1 ? iov[1].iov_len : 0);
        memcpy(iov[0].iov_base, src + written, iov[0].iov_len);
        if (n > 1)
            memcpy(iov[1].iov_base, src + written + iov[0].iov_len, iov[1].iov_len);
        RingBuffer_advance_writer(rb, to_write);
        written += (int)to_write;

        char c;
        while (RingBuffer_get_byte(rb, &c)) {
            read_buf[read_count++] = c;
        }
    }
    ASSERT_EQ_INT(read_count, 20);
    ASSERT_EQ_INT(memcmp(src, read_buf, 20), 0);

    RingBuffer_destroy(rb);
}

static void test_reset(void)
{
    RingBuffer* rb = RingBuffer_create(32);
    ASSERT_TRUE(rb != NULL);

    struct iovec iov[2];
    RingBuffer_writable_iovecs(rb, iov, 20);
    memset(iov[0].iov_base, 0xCC, 20);
    RingBuffer_advance_writer(rb, 20);

    char c;
    RingBuffer_get_byte(rb, &c);    /* tail = 1 */
    ASSERT_EQ_SZ(RingBuffer_readable(rb), 19);

    RingBuffer_reset(rb);
    ASSERT_EQ_SZ(RingBuffer_readable(rb), 0);
    ASSERT_EQ_SZ(RingBuffer_writable(rb), 32);

    /* Ring is reusable after reset */
    RingBuffer_writable_iovecs(rb, iov, 8);
    memset(iov[0].iov_base, 0xDD, 8);
    RingBuffer_advance_writer(rb, 8);
    ASSERT_EQ_SZ(RingBuffer_readable(rb), 8);

    RingBuffer_destroy(rb);
}

/* ============================================================ */
/* Two-thread stress tests                                       */
/* ============================================================ */

/* xorshift64 — deterministic stream so producer & consumer can both
 * generate the expected byte sequence independently. */
static uint64_t xorshift64(uint64_t* s)
{
    uint64_t x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *s = x;
    return x;
}

typedef struct {
    RingBuffer* rb;
    size_t      total_bytes;
    uint64_t    seed;
    int         inject_yield;
    int         result;     /* 0 = ok, non-zero = mismatch */
} StressArgs;

static void* producer_fn(void* arg)
{
    StressArgs* a = (StressArgs*)arg;
    uint64_t state = a->seed;
    size_t produced = 0;

    while (produced < a->total_bytes) {
        struct iovec iov[2];
        size_t want = a->total_bytes - produced;
        if (want > 4096) want = 4096;

        int n = RingBuffer_writable_iovecs(a->rb, iov, want);
        if (n == 0) {
            if (a->inject_yield) sched_yield();
            continue;
        }

        size_t total = 0;
        for (int i = 0; i < n; i++) {
            char* p = (char*)iov[i].iov_base;
            for (size_t j = 0; j < iov[i].iov_len; j++)
                p[j] = (char)(xorshift64(&state) & 0xFF);
            total += iov[i].iov_len;
        }
        RingBuffer_advance_writer(a->rb, total);
        produced += total;

        if (a->inject_yield && (produced & 0x3FF) == 0)
            sched_yield();
    }
    return NULL;
}

static void* consumer_fn(void* arg)
{
    StressArgs* a = (StressArgs*)arg;
    uint64_t state = a->seed;
    size_t consumed = 0;
    char buf[4096];

    while (consumed < a->total_bytes) {
        size_t want = a->total_bytes - consumed;
        if (want > sizeof(buf)) want = sizeof(buf);

        size_t got = RingBuffer_get_bytes(a->rb, buf, want);
        if (got == 0) {
            if (a->inject_yield) sched_yield();
            continue;
        }

        for (size_t i = 0; i < got; i++) {
            char expected = (char)(xorshift64(&state) & 0xFF);
            if (buf[i] != expected) {
                a->result = 1;
                fprintf(stderr, "FAIL consumer: byte mismatch at offset %zu "
                                "(got 0x%02x, expected 0x%02x)\n",
                        consumed + i, (unsigned char)buf[i],
                        (unsigned char)expected);
                return NULL;
            }
        }
        consumed += got;

        if (a->inject_yield && (consumed & 0x3FF) == 0)
            sched_yield();
    }
    return NULL;
}

static void run_stress(size_t ring_capacity, size_t total_bytes, int yield)
{
    RingBuffer* rb = RingBuffer_create(ring_capacity);
    ASSERT_TRUE(rb != NULL);

    StressArgs prod = { rb, total_bytes, 0xDEADBEEFCAFEBABEULL, yield, 0 };
    StressArgs cons = { rb, total_bytes, 0xDEADBEEFCAFEBABEULL, yield, 0 };

    pthread_t pt, ct;
    if (pthread_create(&pt, NULL, producer_fn, &prod) != 0) {
        FAIL("pthread_create producer");
        RingBuffer_destroy(rb);
        return;
    }
    if (pthread_create(&ct, NULL, consumer_fn, &cons) != 0) {
        FAIL("pthread_create consumer");
        pthread_join(pt, NULL);
        RingBuffer_destroy(rb);
        return;
    }
    pthread_join(pt, NULL);
    pthread_join(ct, NULL);

    if (cons.result != 0)
        FAIL("byte stream mismatch in stress test");

    RingBuffer_destroy(rb);
}

static void test_concurrent_throughput(void)
{
    /* Small ring forces frequent wrap-around; ~64 MB stream gives plenty of
     * opportunities for any ordering bug to surface. */
    run_stress(/*capacity*/ 4096, /*total*/ 64 * 1024 * 1024, /*yield*/ 0);
}

static void test_concurrent_with_jitter(void)
{
    /* Tiny ring + sched_yield expose ordering issues that a tight loop hides
     * by keeping the ring nearly empty or nearly full most of the time. */
    run_stress(/*capacity*/ 256, /*total*/ 8 * 1024 * 1024, /*yield*/ 1);
}

/* ============================================================ */
/* Main                                                          */
/* ============================================================ */

int main(void)
{
    printf("RingBuffer unit tests\n");
    printf("---------------------\n");

    printf("Single-threaded:\n");
    RUN(test_create_rejects_invalid_capacity);
    RUN(test_create_accepts_valid_capacity);
    RUN(test_empty_state);
    RUN(test_fill_drain_no_wrap);
    RUN(test_fill_to_capacity);
    RUN(test_wrap_around_content);
    RUN(test_iovecs_capped_by_max_bytes);
    RUN(test_get_bytes_partial);
    RUN(test_get_byte_sequential);
    RUN(test_reset);

    printf("Two-thread stress:\n");
    RUN(test_concurrent_throughput);
    RUN(test_concurrent_with_jitter);

    printf("---------------------\n");
    if (failures == 0) {
        printf("All tests passed.\n");
        return 0;
    } else {
        printf("%d failure(s).\n", failures);
        return 1;
    }
}
