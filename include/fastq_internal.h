#ifndef FASTQ_INTERNAL_H
#define FASTQ_INTERNAL_H

#include "fastq.h"
#include <hiredis/hiredis.h>
#include <pthread.h>

redisContext *fastq_redis_get_ctx(fastq_redis_t *r);

typedef struct fastq_pool_t fastq_pool_t;

fastq_pool_t *fastq_pool_create(const char *host, int port, int size);
void fastq_pool_destroy(fastq_pool_t *pool);
redisContext *fastq_pool_acquire(fastq_pool_t *pool);
void fastq_pool_release(fastq_pool_t *pool, redisContext *ctx);

fastq_pool_t *fastq_queue_get_pool(fastq_queue_t *q);
fastq_redis_t *fastq_queue_get_redis(fastq_queue_t *q);
const char *fastq_queue_get_name(fastq_queue_t *q);

fastq_job_t *fastq_pop_with_ctx(fastq_queue_t *q, redisContext *ctx, int timeout_sec);
fastq_err_t fastq_job_done_with_ctx(fastq_queue_t *q, redisContext *ctx, fastq_job_t *job);
fastq_err_t fastq_job_fail_with_ctx(fastq_queue_t *q, redisContext *ctx, fastq_job_t *job, const char *err);

/* Batch pop: pops up to max_count jobs (non-blocking after the first).
   Caller owns the returned array and each job in it.
   Returns number of jobs popped (0 if queue empty). */
int fastq_pop_batch_with_ctx(fastq_queue_t *q, redisContext *ctx, fastq_job_t **out, int max_count, int first_timeout_sec, long deadline_us);

/* Check if job has a registered chain or workflow continuation and trigger it.
   ctx: the Redis connection already acquired by the caller. */
void fastq_chain_trigger(fastq_queue_t *q, redisContext *ctx, const fastq_job_t *job);

char *fastq_generate_id(void);

long fastq_now_us(void);

#endif
