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
 * Drainer thread: a single producer that reads bytes from registered
 * sockets directly into per-socket SPSC byte rings. The consumer side
 * (typically the MQTTAsync receive thread) reads from the rings via the
 * RingBuffer consumer API.
 *
 * Supports both plain TCP and SSL (OpenSSL) sockets:
 *   - For TCP, the drainer uses recvmsg() with an iovec, scattering
 *     directly into the ring's free region in a single syscall.
 *   - For SSL, the drainer loops SSL_read() while SSL_pending() > 0
 *     (correctness requirement: OpenSSL's internal decrypted buffer is
 *     not visible to poll()). Wrap-around is handled by re-querying the
 *     ring's contiguous free region between iterations.
 *
 * Scope of this milestone:
 *   - TCP and SSL drain paths implemented; SSL_ERROR_WANT_WRITE during
 *     read (TLS renegotiation) currently bails out for that cycle and
 *     retries on next poll. Coordinating with the send thread to
 *     service the pending write is deferred to integration.
 *   - No backpressure-stall machinery yet: if a ring fills, the drainer
 *     keeps polling the socket but does not read until space appears.
 *     Produces a poll() busy-loop in that edge case; the consumer is
 *     expected to drain promptly. Proper stall/re-arm signalling will
 *     land in a follow-up.
 *   - No drainer_status / error propagation. Peer-close (or fatal SSL
 *     error) marks the socket inactive inside the drainer; the caller
 *     is still responsible for calling Drainer_removeSocket() and
 *     close()ing the fd.
 *
 * Concurrency:
 *   - Drainer_create / Drainer_destroy must be called by exactly one
 *     thread (typically the same thread that owns the MQTTAsync globals).
 *   - Drainer_addSocket / Drainer_removeSocket are safe to call
 *     concurrently with each other and with the drainer thread itself.
 *     Both are synchronous: they return only after the drainer has
 *     applied the change. After Drainer_removeSocket(fd) returns, the
 *     caller may safely close(fd) and free the ring.
 */

#ifndef DRAINER_H
#define DRAINER_H

#include "RingBuffer.h"

typedef struct Drainer Drainer;

/* Edge-triggered callback invoked by the drainer when a ring transitions
 * from empty to non-empty. Used to wake a consumer that may be blocked
 * in poll() waiting for ring data. Called with no drainer locks held;
 * the callback must be brief and re-entrancy-safe (in practice: write
 * to an eventfd/pipe and return). May be NULL to disable wakeups. */
typedef void (*DrainerWakeConsumer)(void);

/* Create a drainer and start its thread.
 *   wake_consumer: optional edge-triggered "data available" hook
 *                  (called when a ring transitions empty->non-empty).
 * Returns NULL on failure. */
Drainer* Drainer_create(DrainerWakeConsumer wake_consumer);

/* Stop the drainer's thread and free all internal resources. Caller is
 * expected to have removed all sockets first; any still-registered
 * sockets are silently dropped from the drainer's set but the caller's
 * fd and ring are not touched. */
void Drainer_destroy(Drainer* d);

/* Register a socket with the drainer.
 *   fd:           underlying socket file descriptor (used for poll()).
 *   ssl:          if non-NULL, an OpenSSL SSL* whose BIO is bound to `fd`;
 *                 drainer will use SSL_read() instead of recvmsg(). Pass
 *                 NULL for plain TCP. Typed as void* so non-SSL builds
 *                 need no OpenSSL headers; cast to SSL* internally.
 *   ring:         destination buffer; drainer becomes its sole writer.
 *   closed_flag:  if non-NULL, pointer to an int the drainer atomically
 *                 stores 1 into when it detects peer close or a fatal
 *                 recv/SSL error. The wake_consumer callback also fires
 *                 so the consumer can observe the change. May be NULL
 *                 if the caller doesn't care about close detection.
 *
 * Caller retains ownership of all four. The handshake (SSL or TCP) must
 * be complete before this call.
 *
 * Returns 0 on success, -1 on failure (e.g. drainer shutting down). */
int Drainer_addSocket(Drainer* d, int fd, void* ssl, RingBuffer* ring,
                      int* closed_flag);

/* Remove a socket. Synchronous: the drainer guarantees no further reads
 * or accesses to `fd` or its ring once this returns. Caller is then
 * free to close(fd) and free the ring.
 * Returns 0 on success, -1 if fd was not registered. */
int Drainer_removeSocket(Drainer* d, int fd);

#endif /* DRAINER_H */
