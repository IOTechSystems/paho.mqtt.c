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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE   /* needed for pthread_setname_np when built standalone */
#endif
#include "Drainer.h"

#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#if defined(OPENSSL)
#include <openssl/ssl.h>
#endif

#define DRAINER_CHUNK_CAP (64 * 1024)
#define DRAINER_SSL_CHUNK_CAP (32 * 1024)   /* > TLS record max (~16 KB) */

/* When at least one socket is stalled (ring full), the drainer uses this
 * timeout so it periodically re-checks whether the consumer has drained
 * enough room to re-arm. With no cross-thread signal from the consumer,
 * this is a fixed-rate poll; 10 ms = 100 wakes/sec while stalled, which
 * is negligible CPU. When no socket is stalled, the drainer blocks
 * indefinitely in poll(). */
#define DRAINER_STALL_POLL_MS 10

typedef struct DrainedSocket {
    int         fd;
    void*       ssl;          /* SSL* when OpenSSL is enabled, NULL for TCP */
    RingBuffer* ring;
    int*        closed_flag;  /* caller-owned; drainer atomically stores 1 on close */
    int         closed;       /* drainer-internal: excludes fd from poll set */
    int         stalled;      /* drainer-internal: ring full → excluded from poll set */
} DrainedSocket;

typedef enum { OP_ADD, OP_REMOVE } DrainerOpType;

/* Ops are stack-allocated by the caller and live until the drainer marks
 * them done. The caller waits on the condvar while holding the mutex,
 * so an op cannot be freed while the drainer is still touching it. */
typedef struct DrainerOp {
    DrainerOpType     type;
    int               fd;
    void*             ssl;          /* only for OP_ADD */
    RingBuffer*       ring;         /* only for OP_ADD */
    int*              closed_flag;  /* only for OP_ADD */
    int               done;
    int               result;
    struct DrainerOp* next;
} DrainerOp;

struct Drainer {
    pthread_t       thread;
    int             evfd;            /* eventfd for wakeup */
    DrainerWakeConsumer wake_consumer;

    /* Drainer-thread-private state — no lock needed. */
    DrainedSocket*  sockets;
    size_t          num_sockets;
    size_t          cap_sockets;

    /* Control plane: protected by op_mutex. */
    pthread_mutex_t op_mutex;
    pthread_cond_t  op_done;
    DrainerOp*      pending_ops;     /* LIFO; drainer reverses to FIFO before applying */
    int             tostop;
};

static void  drain_socket(Drainer* d, DrainedSocket* s);
static void  apply_add(Drainer* d, DrainerOp* op);
static void  apply_remove(Drainer* d, DrainerOp* op);
static void  process_pending_ops(Drainer* d);
static void* drainer_thread_main(void* arg);

Drainer* Drainer_create(DrainerWakeConsumer wake_consumer)
{
    Drainer* d = calloc(1, sizeof *d);
    if (!d)
        return NULL;
    printf("Creating drainer\n");
    d->wake_consumer = wake_consumer;
    d->evfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (d->evfd < 0)
        goto fail_evfd;
    if (pthread_mutex_init(&d->op_mutex, NULL) != 0)
        goto fail_mutex;
    if (pthread_cond_init(&d->op_done, NULL) != 0)
        goto fail_cond;
    if (pthread_create(&d->thread, NULL, drainer_thread_main, d) != 0)
        goto fail_thread;

    return d;

fail_thread:
    pthread_cond_destroy(&d->op_done);
fail_cond:
    pthread_mutex_destroy(&d->op_mutex);
fail_mutex:
    close(d->evfd);
fail_evfd:
    free(d);
    return NULL;
}

void Drainer_destroy(Drainer* d)
{
    if (!d)
        return;

    pthread_mutex_lock(&d->op_mutex);
    d->tostop = 1;
    pthread_mutex_unlock(&d->op_mutex);

    uint64_t v = 1;
    ssize_t  wr = write(d->evfd, &v, sizeof v);
    (void)wr; /* eventfd write only fails on EAGAIN/EBADF; both are benign here */

    pthread_join(d->thread, NULL);

    pthread_cond_destroy(&d->op_done);
    pthread_mutex_destroy(&d->op_mutex);
    close(d->evfd);
    free(d->sockets);
    free(d);
}

/* Submit an op and block until the drainer applies it. */
static int submit_op(Drainer* d, DrainerOp* op)
{
    pthread_mutex_lock(&d->op_mutex);
    if (d->tostop) {
        pthread_mutex_unlock(&d->op_mutex);
        return -1;
    }
    op->next = d->pending_ops;
    d->pending_ops = op;
    pthread_mutex_unlock(&d->op_mutex);

    uint64_t v = 1;
    ssize_t  wr = write(d->evfd, &v, sizeof v);
    (void)wr; /* eventfd write only fails on EAGAIN/EBADF; both are benign here */

    pthread_mutex_lock(&d->op_mutex);
    while (!op->done)
        pthread_cond_wait(&d->op_done, &d->op_mutex);
    pthread_mutex_unlock(&d->op_mutex);

    return op->result;
}

int Drainer_addSocket(Drainer* d, int fd, void* ssl, RingBuffer* ring,
                      int* closed_flag)
{
    DrainerOp op = { .type = OP_ADD, .fd = fd, .ssl = ssl, .ring = ring,
                     .closed_flag = closed_flag };
    return submit_op(d, &op);
}

int Drainer_removeSocket(Drainer* d, int fd)
{
    DrainerOp op = { .type = OP_REMOVE, .fd = fd };
    return submit_op(d, &op);
}

/* --- Drainer-thread-side helpers (no locks needed, single-threaded) --- */

static void apply_add(Drainer* d, DrainerOp* op)
{
    if (d->num_sockets >= d->cap_sockets) {
        size_t new_cap = d->cap_sockets ? d->cap_sockets * 2 : 8;
        DrainedSocket* ns = realloc(d->sockets, new_cap * sizeof *ns);
        if (!ns) {
            op->result = -1;
            return;
        }
        d->sockets = ns;
        d->cap_sockets = new_cap;
    }
    d->sockets[d->num_sockets].fd           = op->fd;
    d->sockets[d->num_sockets].ssl          = op->ssl;
    d->sockets[d->num_sockets].ring         = op->ring;
    d->sockets[d->num_sockets].closed_flag  = op->closed_flag;
    d->sockets[d->num_sockets].closed       = 0;
    d->sockets[d->num_sockets].stalled      = 0;
    d->num_sockets++;
    op->result = 0;
}

static void apply_remove(Drainer* d, DrainerOp* op)
{
    for (size_t i = 0; i < d->num_sockets; i++) {
        if (d->sockets[i].fd == op->fd) {
            d->sockets[i] = d->sockets[d->num_sockets - 1];
            d->num_sockets--;
            op->result = 0;
            return;
        }
    }
    op->result = -1;
}

static void process_pending_ops(Drainer* d)
{
    pthread_mutex_lock(&d->op_mutex);
    DrainerOp* head = d->pending_ops;
    d->pending_ops = NULL;
    pthread_mutex_unlock(&d->op_mutex);

    /* Caller prepended, so list is LIFO. Reverse for FIFO. */
    DrainerOp* fifo = NULL;
    while (head) {
        DrainerOp* next = head->next;
        head->next = fifo;
        fifo = head;
        head = next;
    }

    for (DrainerOp* op = fifo; op != NULL; op = op->next) {
        if (op->type == OP_ADD)
            apply_add(d, op);
        else
            apply_remove(d, op);
    }

    /* Mark done and broadcast under the mutex. Holding the mutex during
     * the entire signal-and-set ensures no waiter can return and free its
     * op (which would invalidate `next` pointers we still need). */
    pthread_mutex_lock(&d->op_mutex);
    for (DrainerOp* op = fifo; op != NULL; op = op->next)
        op->done = 1;
    pthread_cond_broadcast(&d->op_done);
    pthread_mutex_unlock(&d->op_mutex);
}

#if defined(OPENSSL)
/* Drain decrypted plaintext from an SSL connection into the ring.
 *
 * SSL_pending() reports OpenSSL's internal decrypted-but-unread buffer.
 * If we leave bytes there, poll() will not fire for them on the next
 * cycle (they're already past the kernel boundary), so we must drain
 * them now. This loop is a correctness requirement, not an optimisation.
 *
 * SSL_read takes a single contiguous buffer, so we fill iov[0] from the
 * ring's first contiguous free region. Wrap-around is handled by
 * re-querying iovecs on the next iteration — once we advance head past
 * the buffer end, the new iov[0] is the start-of-ring region. */
static void drain_ssl(DrainedSocket* s)
{
    SSL* ssl = (SSL*)s->ssl;
    while (1) {
        struct iovec iov[2];
        int n = RingBuffer_writable_iovecs(s->ring, iov, DRAINER_SSL_CHUNK_CAP);
        if (n == 0)
            return; /* ring full */

        int got = SSL_read(ssl, iov[0].iov_base, (int)iov[0].iov_len);
        if (got > 0) {
            RingBuffer_advance_writer(s->ring, (size_t)got);
            if (SSL_pending(ssl) <= 0)
                return;
            continue;
        }

        int err = SSL_get_error(ssl, got);
        switch (err) {
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
                /* WANT_WRITE during read indicates TLS renegotiation
                 * needs to send. Drainer is read-only; defer to the
                 * send thread. For this milestone we just bail and
                 * retry on the next poll cycle. */
                return;
            case SSL_ERROR_ZERO_RETURN:
                s->closed = 1;
                return;
            case SSL_ERROR_SYSCALL:
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                    return;
                s->closed = 1;
                return;
            default:
                /* SSL_ERROR_SSL or anything else: fatal */
                s->closed = 1;
                return;
        }
    }
}
#endif /* OPENSSL */

static void drain_socket(Drainer* d, DrainedSocket* s)
{
    size_t before = RingBuffer_readable(s->ring);

#if defined(OPENSSL)
    if (s->ssl) {
        drain_ssl(s);
        if (RingBuffer_writable(s->ring) == 0)
            s->stalled = 1;
        goto check_wake;
    }
#endif

    {
        struct iovec iov[2];
        int n = RingBuffer_writable_iovecs(s->ring, iov, DRAINER_CHUNK_CAP);
        if (n == 0) {
            /* Ring full → backpressure. Mark socket stalled so we
             * exclude it from the poll set; we'll re-include it once
             * the consumer has drained enough room (hysteresis applied
             * in the un-stall pass below). */
            s->stalled = 1;
            goto check_wake;
        }

        struct msghdr msg;
        memset(&msg, 0, sizeof msg);
        msg.msg_iov = iov;
        msg.msg_iovlen = n;

        ssize_t got = recvmsg(s->fd, &msg, MSG_DONTWAIT);
        if (got > 0) {
            RingBuffer_advance_writer(s->ring, (size_t)got);
            /* If that one call filled the ring, stall preemptively so
             * we don't busy-loop next iteration. */
            if (RingBuffer_writable(s->ring) == 0)
                s->stalled = 1;
        } else if (got == 0) {
            s->closed = 1;
        } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            s->closed = 1;
        }
    }

check_wake:
    /* Edge-triggered consumer wake: fire when (a) the ring transitions
     * from empty to non-empty, OR (b) we just detected close. Either
     * way the consumer has new work; otherwise stay silent to avoid
     * syscall spam. */
    {
        int new_data = (before == 0 && RingBuffer_readable(s->ring) > 0);
        int new_close = 0;
        if (s->closed && s->closed_flag &&
            __atomic_load_n(s->closed_flag, __ATOMIC_RELAXED) == 0)
        {
            __atomic_store_n(s->closed_flag, 1, __ATOMIC_RELEASE);
            new_close = 1;
        }
        if ((new_data || new_close) && d->wake_consumer)
            d->wake_consumer();
    }
}

static void* drainer_thread_main(void* arg)
{
    Drainer* d = (Drainer*)arg;
    pthread_setname_np(pthread_self(), "paho-drainer");

    struct pollfd* pollfds = NULL;
    size_t*        socket_idx = NULL;     /* maps pollfd[i+1] -> sockets[k] */
    size_t         scratch_cap = 0;

    while (1) {
        /* Rebuild poll set every iteration: simpler than tracking
         * incremental changes, and the cost is O(N) per cycle which is
         * negligible at our client counts. */
        size_t needed = 1 + d->num_sockets;
        if (needed > scratch_cap) {
            size_t new_cap = scratch_cap ? scratch_cap * 2 : 8;
            while (new_cap < needed) new_cap *= 2;
            struct pollfd* np = realloc(pollfds, new_cap * sizeof *np);
            if (!np)
                break;
            pollfds = np;
            size_t* nidx = realloc(socket_idx, new_cap * sizeof *nidx);
            if (!nidx)
                break;
            socket_idx = nidx;
            scratch_cap = new_cap;
        }

        pollfds[0].fd      = d->evfd;
        pollfds[0].events  = POLLIN;
        pollfds[0].revents = 0;

        /* Un-stall pass with hysteresis: a socket whose ring has now
         * dropped below half-capacity is ready to recv again. Re-arming
         * at a low watermark (rather than "any free byte") avoids
         * thrash where one packet fills the ring, we drain one byte,
         * we recv and fill again, etc. */
        int any_stalled = 0;
        for (size_t i = 0; i < d->num_sockets; i++) {
            DrainedSocket* s = &d->sockets[i];
            if (s->closed || !s->stalled)
                continue;
            size_t cap = RingBuffer_capacity(s->ring);
            if (RingBuffer_writable(s->ring) >= cap / 2)
                s->stalled = 0;
            else
                any_stalled = 1;
        }

        size_t pollfd_count = 1;
        for (size_t i = 0; i < d->num_sockets; i++) {
            if (d->sockets[i].closed || d->sockets[i].stalled)
                continue;
            pollfds[pollfd_count].fd      = d->sockets[i].fd;
            pollfds[pollfd_count].events  = POLLIN;
            pollfds[pollfd_count].revents = 0;
            socket_idx[pollfd_count - 1] = i;
            pollfd_count++;
        }

        /* If any socket is stalled we need to wake periodically to
         * re-check whether the consumer has drained it. Otherwise
         * block indefinitely until something fires. */
        int poll_timeout = any_stalled ? DRAINER_STALL_POLL_MS : -1;
        int rc = poll(pollfds, pollfd_count, poll_timeout);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            break; /* fatal poll error */
        }

        /* Drain ready sockets BEFORE processing ops, so the socket_idx
         * mapping remains valid (ops may shuffle the sockets array). */
        for (size_t i = 1; i < pollfd_count; i++) {
            if (pollfds[i].revents & (POLLIN | POLLHUP | POLLERR)) {
                drain_socket(d, &d->sockets[socket_idx[i - 1]]);
            }
        }

        if (pollfds[0].revents & POLLIN) {
            uint64_t v;
            ssize_t r = read(d->evfd, &v, sizeof v);
            (void)r; /* may legitimately return EAGAIN under coalescing */

            process_pending_ops(d);

            pthread_mutex_lock(&d->op_mutex);
            int stop = d->tostop;
            pthread_mutex_unlock(&d->op_mutex);
            if (stop)
                break;
        }
    }

    free(pollfds);
    free(socket_idx);
    return NULL;
}
