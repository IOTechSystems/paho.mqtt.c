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
 * Single-Producer, Single-Consumer (SPSC) lock-free byte ring buffer.
 *
 * Concurrency contract:
 *   - Exactly ONE producer thread calls the writer-side functions
 *     (RingBuffer_writable, RingBuffer_writable_iovecs, RingBuffer_advance_writer).
 *   - Exactly ONE consumer thread calls the reader-side functions
 *     (RingBuffer_readable, RingBuffer_get_byte, RingBuffer_get_bytes).
 *   - RingBuffer_create / RingBuffer_destroy / RingBuffer_reset must be
 *     called with no concurrent access from either side.
 *
 * Using this with more than one producer or more than one consumer is
 * a programming error and will silently corrupt the ring.
 */

#ifndef RINGBUFFER_H
#define RINGBUFFER_H

#include <stddef.h>
#include <sys/uio.h>

typedef struct RingBuffer RingBuffer;

/* Allocates a ring buffer of the requested capacity in bytes. The capacity
 * must be a power of two and at least 2. Returns NULL on invalid capacity
 * or allocation failure. */
RingBuffer* RingBuffer_create(size_t capacity);

/* Frees the ring buffer. Caller must ensure no concurrent access. */
void RingBuffer_destroy(RingBuffer* rb);

/* Resets head and tail to zero. Caller must ensure no concurrent access
 * (intended for use between disconnect and reconnect). */
void RingBuffer_reset(RingBuffer* rb);

/* Capacity (in bytes) the ring was created with. */
size_t RingBuffer_capacity(const RingBuffer* rb);

/* --- Producer side --- */

/* Number of bytes currently free for the producer to write. */
size_t RingBuffer_writable(const RingBuffer* rb);

/* Fills `iov` with up to 2 entries describing contiguous free regions
 * totalling no more than `max_bytes`. The first entry, if any, describes
 * the region immediately after the current head; a second entry, if any,
 * describes the wrap-around region starting at offset 0.
 *
 * Returns the number of iovec entries written (0 if the ring is full or
 * max_bytes is 0; otherwise 1 or 2).
 *
 * Producer uses this with recvmsg() to scatter-write into the ring in
 * a single syscall even when the free region wraps.
 */
int RingBuffer_writable_iovecs(const RingBuffer* rb,
                               struct iovec iov[2],
                               size_t max_bytes);

/* Advances the writer's head by `n` bytes. Must be called only after
 * `n` bytes have been written into the regions returned by the most
 * recent RingBuffer_writable_iovecs (or otherwise into the ring's free
 * space). `n` must not exceed the writable size at the time of the call.
 */
void RingBuffer_advance_writer(RingBuffer* rb, size_t n);

/* --- Consumer side --- */

/* Number of bytes currently available to read. */
size_t RingBuffer_readable(const RingBuffer* rb);

/* Reads a single byte. Returns 1 on success (byte stored in *c), 0 if
 * the ring is empty. */
int RingBuffer_get_byte(RingBuffer* rb, char* c);

/* Reads up to `bytes` bytes into `dst`. Returns the number of bytes
 * actually copied, which may be less than `bytes` if the ring did not
 * have that much available. */
size_t RingBuffer_get_bytes(RingBuffer* rb, char* dst, size_t bytes);

#endif /* RINGBUFFER_H */
