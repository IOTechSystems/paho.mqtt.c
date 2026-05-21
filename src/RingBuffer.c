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

#include "RingBuffer.h"

#include <stdatomic.h>
#include <stdalign.h>
#include <stdlib.h>
#include <string.h>

#define RB_CACHELINE 64

/*
 * Indices are monotonically increasing 64-bit counters. Modulo is applied
 * only at access time via `& mask`. This avoids the classic SPSC pitfall
 * where head == tail must distinguish empty from full (which would require
 * wasting one slot or carrying a separate flag).
 *
 * Free / used are derived as:
 *   used = head - tail
 *   free = capacity - used
 *
 * The producer owns `head`; the consumer owns `tail`. Each is on its own
 * cache line to avoid false sharing.
 */
struct RingBuffer {
    char*  buffer;
    size_t capacity;
    size_t mask;

    alignas(RB_CACHELINE) _Atomic size_t head;
    char _pad1[RB_CACHELINE - sizeof(_Atomic size_t)];

    alignas(RB_CACHELINE) _Atomic size_t tail;
    char _pad2[RB_CACHELINE - sizeof(_Atomic size_t)];
};

static int is_power_of_two(size_t n)
{
    return n != 0 && (n & (n - 1)) == 0;
}

RingBuffer* RingBuffer_create(size_t capacity)
{
    if (capacity < 2 || !is_power_of_two(capacity))
        return NULL;

    RingBuffer* rb = aligned_alloc(RB_CACHELINE, sizeof(RingBuffer));
    if (rb == NULL)
        return NULL;

    rb->buffer = malloc(capacity);
    if (rb->buffer == NULL) {
        free(rb);
        return NULL;
    }
    rb->capacity = capacity;
    rb->mask     = capacity - 1;
    atomic_init(&rb->head, (size_t)0);
    atomic_init(&rb->tail, (size_t)0);
    return rb;
}

void RingBuffer_destroy(RingBuffer* rb)
{
    if (rb == NULL)
        return;
    free(rb->buffer);
    free(rb);
}

void RingBuffer_reset(RingBuffer* rb)
{
    atomic_store_explicit(&rb->head, (size_t)0, memory_order_relaxed);
    atomic_store_explicit(&rb->tail, (size_t)0, memory_order_relaxed);
}

size_t RingBuffer_capacity(const RingBuffer* rb)
{
    return rb->capacity;
}

/* --- Producer side --- */

size_t RingBuffer_writable(const RingBuffer* rb)
{
    /* Producer reads its own head with relaxed (it published it itself);
     * acquires tail to see consumer's progress. */
    size_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    return rb->capacity - (head - tail);
}

int RingBuffer_writable_iovecs(const RingBuffer* rb,
                               struct iovec iov[2],
                               size_t max_bytes)
{
    size_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    size_t free = rb->capacity - (head - tail);

    if (free > max_bytes)
        free = max_bytes;
    if (free == 0)
        return 0;

    size_t head_idx = head & rb->mask;
    size_t until_end = rb->capacity - head_idx;

    if (free <= until_end) {
        iov[0].iov_base = rb->buffer + head_idx;
        iov[0].iov_len  = free;
        return 1;
    }

    iov[0].iov_base = rb->buffer + head_idx;
    iov[0].iov_len  = until_end;
    iov[1].iov_base = rb->buffer;
    iov[1].iov_len  = free - until_end;
    return 2;
}

void RingBuffer_advance_writer(RingBuffer* rb, size_t n)
{
    size_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    /* Release so that the byte writes (which are plain stores into the
     * buffer) are visible to the consumer once it observes the new head. */
    atomic_store_explicit(&rb->head, head + n, memory_order_release);
}

/* --- Consumer side --- */

size_t RingBuffer_readable(const RingBuffer* rb)
{
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    return head - tail;
}

int RingBuffer_get_byte(RingBuffer* rb, char* c)
{
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    if (head == tail)
        return 0;
    *c = rb->buffer[tail & rb->mask];
    atomic_store_explicit(&rb->tail, tail + 1, memory_order_release);
    return 1;
}

size_t RingBuffer_get_bytes(RingBuffer* rb, char* dst, size_t bytes)
{
    size_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    size_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    size_t available = head - tail;

    if (bytes > available)
        bytes = available;
    if (bytes == 0)
        return 0;

    size_t tail_idx = tail & rb->mask;
    size_t until_end = rb->capacity - tail_idx;

    if (bytes <= until_end) {
        memcpy(dst, rb->buffer + tail_idx, bytes);
    } else {
        memcpy(dst, rb->buffer + tail_idx, until_end);
        memcpy(dst + until_end, rb->buffer, bytes - until_end);
    }

    atomic_store_explicit(&rb->tail, tail + bytes, memory_order_release);
    return bytes;
}
