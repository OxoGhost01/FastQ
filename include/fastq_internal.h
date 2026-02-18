#ifndef FASTQ_INTERNAL_H
#define FASTQ_INTERNAL_H

#include "fastq.h"
#include <hiredis/hiredis.h>
#include <pthread.h>

/* ── Redis internals ────────────────────────────────────────────── */

/* Access the raw hiredis context (for internal use only). */
redisContext *fastq_redis_get_ctx(fastq_redis_t *r);

/* ── Connection pool ────────────────────────────────────────────── */

typedef struct fastq_pool_t fastq_pool_t;

fastq_pool_t   *fastq_pool_create(const char *host, int port, int size);
void            fastq_pool_destroy(fastq_pool_t *pool);
redisContext   *fastq_pool_acquire(fastq_pool_t *pool);
void            fastq_pool_release(fastq_pool_t *pool, redisContext *ctx);

/* ── Queue internals (needed by worker threads) ─────────────────── */

fastq_pool_t   *fastq_queue_get_pool(fastq_queue_t *q);
const char     *fastq_queue_get_name(fastq_queue_t *q);

/* Pop using a specific Redis connection (for worker threads). */
fastq_job_t    *fastq_pop_with_ctx(fastq_queue_t *q, redisContext *ctx,
                                    int timeout_sec);
/* Done/fail using a specific Redis connection. */
fastq_err_t     fastq_job_done_with_ctx(fastq_queue_t *q, redisContext *ctx,
                                         fastq_job_t *job);
fastq_err_t     fastq_job_fail_with_ctx(fastq_queue_t *q, redisContext *ctx,
                                         fastq_job_t *job, const char *err);

/* ── ID generation ──────────────────────────────────────────────── */

char *fastq_generate_id(void);

#endif /* FASTQ_INTERNAL_H */
