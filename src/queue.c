#include "fastq.h"
#include "fastq_internal.h"
#include <hiredis/hiredis.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <ctype.h>

#define KEY_MAX 256
#define RETRY_BASE_DELAY 5
#define DELAYED_CHECK_INTERVAL 1
#define NAME_MAX_LEN 64
#define BATCH_MAX 1000

/* Stack-allocated job hash key. */
#define JOB_HKEY(buf, job_id) \
    snprintf((buf), sizeof(buf), "fastq:job:%s", (job_id))

struct fastq_queue_t {
    fastq_redis_t *redis;
    fastq_pool_t *pool;
    bool owns_pool;
    char *name;
    char key_high[KEY_MAX];
    char key_normal[KEY_MAX];
    char key_low[KEY_MAX];
    char key_delayed[KEY_MAX];
    char key_dead[KEY_MAX];
    char key_done[KEY_MAX];
    _Atomic time_t last_delayed_check;
};

/* Input validation */

fastq_err_t fastq_validate_name(const char *name)
{
    if (!name) return FASTQ_ERR_INVALID;
    size_t len = strlen(name);
    if (len == 0 || len > NAME_MAX_LEN) return FASTQ_ERR_INVALID;
    for (size_t i = 0; i < len; i++) {
        char c = name[i];
        if (!isalnum((unsigned char)c) && c != '_' && c != '-')
            return FASTQ_ERR_INVALID;
    }
    return FASTQ_OK;
}

/* Internal helpers */

static void build_keys(fastq_queue_t *q)
{
    snprintf(q->key_high, KEY_MAX, "fastq:queue:%s:high", q->name);
    snprintf(q->key_normal, KEY_MAX, "fastq:queue:%s:normal", q->name);
    snprintf(q->key_low, KEY_MAX, "fastq:queue:%s:low", q->name);
    snprintf(q->key_delayed, KEY_MAX, "fastq:queue:%s:delayed", q->name);
    snprintf(q->key_dead, KEY_MAX, "fastq:queue:%s:dead", q->name);
    snprintf(q->key_done, KEY_MAX, "fastq:queue:%s:done", q->name);
}

static redisContext *get_ctx(fastq_queue_t *q)
{
    return q->pool ? fastq_pool_acquire(q->pool) : fastq_redis_get_ctx(q->redis);
}

static void put_ctx(fastq_queue_t *q, redisContext *ctx)
{
    if (q->pool) fastq_pool_release(q->pool, ctx);
}

static const char *priority_key(fastq_queue_t *q, fastq_priority_t p)
{
    if (p == FASTQ_PRIORITY_HIGH) return q->key_high;
    if (p == FASTQ_PRIORITY_LOW)  return q->key_low;
    return q->key_normal;
}

fastq_pool_t *fastq_queue_get_pool(fastq_queue_t *q)  { return q ? q->pool  : NULL; }
fastq_redis_t *fastq_queue_get_redis(fastq_queue_t *q) { return q ? q->redis : NULL; }
const char *fastq_queue_get_name(fastq_queue_t *q)  { return q ? q->name  : NULL; }

/* Create / Destroy */

fastq_queue_t *fastq_queue_create(fastq_redis_t *redis, const char *name)
{
    if (!redis || !name) return NULL;
    if (fastq_validate_name(name) != FASTQ_OK) {
        fastq_log(FASTQ_LOG_ERROR, "queue: invalid name '%s'", name);
        return NULL;
    }

    fastq_queue_t *q = calloc(1, sizeof(*q));
    if (!q) return NULL;

    q->redis = redis;
    q->name  = strdup(name);
    if (!q->name) { free(q); return NULL; }

    build_keys(q);
    return q;
}

fastq_queue_t *fastq_queue_create_pooled(const char *host, int port, const char *name, int pool_size)
{
    if (!host || !name || pool_size <= 0) return NULL;
    if (fastq_validate_name(name) != FASTQ_OK) {
        fastq_log(FASTQ_LOG_ERROR, "queue: invalid name '%s'", name);
        return NULL;
    }

    fastq_pool_t *pool = fastq_pool_create(host, port, pool_size);
    if (!pool) return NULL;

    fastq_queue_t *q = calloc(1, sizeof(*q));
    if (!q) { fastq_pool_destroy(pool); return NULL; }

    q->pool = pool;
    q->owns_pool = true;
    q->name = strdup(name);
    if (!q->name) { fastq_pool_destroy(pool); free(q); return NULL; }

    build_keys(q);
    fastq_log(FASTQ_LOG_DEBUG, "queue '%s' created (pool=%d)", name, pool_size);
    return q;
}

void fastq_queue_destroy(fastq_queue_t *q)
{
    if (!q) return;
    if (q->owns_pool) fastq_pool_destroy(q->pool);
    free(q->name);
    free(q);
}

/* Push (MULTI/EXEC pipeline) */

fastq_err_t fastq_push(fastq_queue_t *q, fastq_job_t *job)
{
    if (!q || !job) return FASTQ_ERR;

    free(job->queue_name);
    job->queue_name = strdup(q->name);

    char *json = fastq_job_serialize(job);
    if (!json) return FASTQ_ERR_SERIALIZE;

    const char *qkey = priority_key(q, job->priority);
    char hkey[KEY_MAX];
    JOB_HKEY(hkey, job->id);

    redisContext *ctx = get_ctx(q);
    if (!ctx) { free(json); return FASTQ_ERR_REDIS; }

    redisAppendCommand(ctx, "MULTI");
    redisAppendCommand(ctx, "HSET %s data %s status queued", hkey, json);
    redisAppendCommand(ctx, "RPUSH %s %s", qkey, json);
    redisAppendCommand(ctx, "EXEC");
    free(json);

    redisReply *r;
    fastq_err_t rc = FASTQ_OK;
    for (int i = 0; i < 4; i++) {
        if (redisGetReply(ctx, (void **)&r) != REDIS_OK) { rc = FASTQ_ERR_REDIS; break; }
        freeReplyObject(r);
    }

    put_ctx(q, ctx);
    fastq_log(FASTQ_LOG_DEBUG, "pushed job %s (prio=%d)", job->id, job->priority);
    return rc;
}

/* Schedule (one-shot delayed job) */

fastq_err_t fastq_schedule(fastq_queue_t *q, fastq_job_t *job, time_t run_at)
{
    if (!q || !job || run_at <= 0) return FASTQ_ERR;

    free(job->queue_name);
    job->queue_name = strdup(q->name);

    char *json = fastq_job_serialize(job);
    if (!json) return FASTQ_ERR_SERIALIZE;

    char score[32];
    snprintf(score, sizeof(score), "%ld", (long)run_at);

    redisContext *ctx = get_ctx(q);
    if (!ctx) { free(json); return FASTQ_ERR_REDIS; }

    char hkey[KEY_MAX];
    JOB_HKEY(hkey, job->id);

    redisAppendCommand(ctx, "HSET %s data %s status delayed", hkey, json);
    redisAppendCommand(ctx, "ZADD %s %s %s", q->key_delayed, score, json);
    free(json);

    redisReply *r;
    fastq_err_t rc = FASTQ_OK;
    for (int i = 0; i < 2; i++) {
        if (redisGetReply(ctx, (void **)&r) != REDIS_OK) { rc = FASTQ_ERR_REDIS; break; }
        freeReplyObject(r);
    }

    put_ctx(q, ctx);
    fastq_log(FASTQ_LOG_DEBUG, "scheduled job %s at %ld", job->id, (long)run_at);
    return rc;
}

/* Pop (blocking, priority-aware) */

static void promote_delayed_if_due(fastq_queue_t *q, redisContext *ctx)
{
    time_t now = time(NULL);
    time_t last = atomic_load_explicit(&q->last_delayed_check, memory_order_relaxed);
    if (now - last < DELAYED_CHECK_INTERVAL) return;

    if (!atomic_compare_exchange_strong_explicit(&q->last_delayed_check, &last, now, memory_order_relaxed, memory_order_relaxed)) return;

    char score[32];
    snprintf(score, sizeof(score), "%ld", (long)now);

    redisReply *ready = redisCommand(ctx, "ZRANGEBYSCORE %s -inf %s", q->key_delayed, score);
    if (!ready || ready->type != REDIS_REPLY_ARRAY || ready->elements == 0) {
        if (ready) freeReplyObject(ready);
        return;
    }

    for (size_t i = 0; i < ready->elements; i++) {
        const char *blob = ready->element[i]->str;
        redisReply *r;
        r = redisCommand(ctx, "RPUSH %s %s", q->key_normal, blob);
        if (r) freeReplyObject(r);
        r = redisCommand(ctx, "ZREM %s %s", q->key_delayed, blob);
        if (r) freeReplyObject(r);
    }
    freeReplyObject(ready);
}

static fastq_job_t *pop_with_ctx(fastq_queue_t *q, redisContext *ctx, int timeout_sec)
{
    promote_delayed_if_due(q, ctx);

    redisReply *reply = redisCommand(ctx, "BLPOP %s %s %s %d", q->key_high, q->key_normal, q->key_low, timeout_sec);
    if (!reply) return NULL;
    if (reply->type != REDIS_REPLY_ARRAY || reply->elements < 2) {
        freeReplyObject(reply);
        return NULL;
    }

    fastq_job_t *job = fastq_job_deserialize(reply->element[1]->str);
    freeReplyObject(reply);
    if (!job) return NULL;

    job->processed_at = time(NULL);

    char hkey[KEY_MAX];
    JOB_HKEY(hkey, job->id);
    redisReply *r = redisCommand(ctx, "HSET %s status processing", hkey);
    if (r) freeReplyObject(r);

    fastq_log(FASTQ_LOG_DEBUG, "popped job %s", job->id);
    return job;
}

fastq_job_t *fastq_pop(fastq_queue_t *q, int timeout_sec)
{
    if (!q) return NULL;
    redisContext *ctx = get_ctx(q);
    if (!ctx) return NULL;
    fastq_job_t *job = pop_with_ctx(q, ctx, timeout_sec);
    put_ctx(q, ctx);
    return job;
}

fastq_job_t *fastq_pop_with_ctx(fastq_queue_t *q, redisContext *ctx, int timeout_sec)
{
    return pop_with_ctx(q, ctx, timeout_sec);
}

/*
 * Batch pop: blocking wait for first job, then non-blocking LPOP for
 * up to (max_count - 1) more. Stops early if deadline_us is exceeded.
 */
int fastq_pop_batch_with_ctx(fastq_queue_t *q, redisContext *ctx, fastq_job_t **out, int max_count, int first_timeout_sec, long deadline_us)
{
    if (!q || !ctx || !out || max_count <= 0) return 0;
    if (max_count > BATCH_MAX) max_count = BATCH_MAX;

    int count = 0;

    fastq_job_t *first = pop_with_ctx(q, ctx, first_timeout_sec);
    if (!first) return 0;
    out[count++] = first;

    const char *keys[3] = { q->key_high, q->key_normal, q->key_low };

    while (count < max_count) {
        if (deadline_us > 0 && fastq_now_us() >= deadline_us) break;

        fastq_job_t *job = NULL;
        for (int k = 0; k < 3 && !job; k++) {
            redisReply *r = redisCommand(ctx, "LPOP %s", keys[k]);
            if (r && r->type == REDIS_REPLY_STRING) {
                job = fastq_job_deserialize(r->str);
                if (job) {
                    job->processed_at = time(NULL);
                    char hkey[KEY_MAX];
                    JOB_HKEY(hkey, job->id);
                    redisReply *hr = redisCommand(ctx, "HSET %s status processing", hkey);
                    if (hr) freeReplyObject(hr);
                }
            }
            if (r) freeReplyObject(r);
        }
        if (!job) break;
        out[count++] = job;
    }

    return count;
}

/* Job done (pipelined HSET + LPUSH + chain trigger) */

static fastq_err_t job_done_impl(redisContext *ctx, fastq_queue_t *q, fastq_job_t *job)
{
    job->completed_at = time(NULL);

    char hkey[KEY_MAX];
    JOB_HKEY(hkey, job->id);

    redisAppendCommand(ctx, "HSET %s status done", hkey);
    redisAppendCommand(ctx, "LPUSH %s %s", q->key_done, job->id);

    redisReply *r;
    fastq_err_t rc = FASTQ_OK;
    for (int i = 0; i < 2; i++) {
        if (redisGetReply(ctx, (void **)&r) != REDIS_OK) { rc = FASTQ_ERR_REDIS; break; }
        freeReplyObject(r);
    }

    fastq_chain_trigger(q, ctx, job);

    fastq_log(FASTQ_LOG_DEBUG, "job %s done", job->id);
    return rc;
}

fastq_err_t fastq_job_done(fastq_queue_t *q, fastq_job_t *job)
{
    if (!q || !job) return FASTQ_ERR;
    redisContext *ctx = get_ctx(q);
    if (!ctx) return FASTQ_ERR_REDIS;
    fastq_err_t rc = job_done_impl(ctx, q, job);
    put_ctx(q, ctx);
    return rc;
}

fastq_err_t fastq_job_done_with_ctx(fastq_queue_t *q, redisContext *ctx, fastq_job_t *job)
{
    if (!q || !ctx || !job) return FASTQ_ERR;
    return job_done_impl(ctx, q, job);
}

/* Job fail (pipelined, retry or DLQ) */

static fastq_err_t job_fail_impl(redisContext *ctx, fastq_queue_t *q, fastq_job_t *job, const char *error_msg)
{
    job->retries++;
    free(job->error);
    job->error = error_msg ? strdup(error_msg) : NULL;

    char hkey[KEY_MAX];
    JOB_HKEY(hkey, job->id);

    char *json = fastq_job_serialize(job);
    if (!json) return FASTQ_ERR_SERIALIZE;

    fastq_err_t rc = FASTQ_OK;

    if (job->retries < job->max_retries) {
        long delay  = RETRY_BASE_DELAY * (1L << job->retries);
        char score[32];
        snprintf(score, sizeof(score), "%ld", (long)(time(NULL) + delay));

        redisAppendCommand(ctx, "ZADD %s %s %s", q->key_delayed, score, json);
        redisAppendCommand(ctx, "HSET %s status failed retries %d", hkey, job->retries);

        fastq_log(FASTQ_LOG_WARN, "job %s retry %d/%d in %lds: %s", job->id, job->retries, job->max_retries, delay, error_msg ? error_msg : "");
    } else {
        redisAppendCommand(ctx, "LPUSH %s %s", q->key_dead, json);
        redisAppendCommand(ctx, "HSET %s status dead", hkey);

        fastq_log(FASTQ_LOG_ERROR, "job %s dead after %d retries: %s", job->id, job->retries, error_msg ? error_msg : "");
    }
    free(json);

    redisReply *r;
    for (int i = 0; i < 2; i++) {
        if (redisGetReply(ctx, (void **)&r) != REDIS_OK) { rc = FASTQ_ERR_REDIS; break; }
        freeReplyObject(r);
    }
    return rc;
}

fastq_err_t fastq_job_fail(fastq_queue_t *q, fastq_job_t *job, const char *error_msg)
{
    if (!q || !job) return FASTQ_ERR;
    redisContext *ctx = get_ctx(q);
    if (!ctx) return FASTQ_ERR_REDIS;
    fastq_err_t rc = job_fail_impl(ctx, q, job, error_msg);
    put_ctx(q, ctx);
    return rc;
}

fastq_err_t fastq_job_fail_with_ctx(fastq_queue_t *q, redisContext *ctx, fastq_job_t *job, const char *err)
{
    if (!q || !ctx || !job) return FASTQ_ERR;
    return job_fail_impl(ctx, q, job, err);
}

/* Stats (pipelined, 1 round-trip) */

fastq_err_t fastq_stats(fastq_queue_t *q, fastq_stats_t *out)
{
    if (!q || !out) return FASTQ_ERR;
    memset(out, 0, sizeof(*out));

    redisContext *ctx = get_ctx(q);
    if (!ctx) return FASTQ_ERR_REDIS;

    redisAppendCommand(ctx, "LLEN %s", q->key_high);
    redisAppendCommand(ctx, "LLEN %s", q->key_normal);
    redisAppendCommand(ctx, "LLEN %s", q->key_low);
    redisAppendCommand(ctx, "LLEN %s", q->key_done);
    redisAppendCommand(ctx, "LLEN %s", q->key_dead);
    redisAppendCommand(ctx, "ZCARD %s", q->key_delayed);

    redisReply *r;
    fastq_err_t rc = FASTQ_OK;
    for (int i = 0; i < 6; i++) {
        if (redisGetReply(ctx, (void **)&r) != REDIS_OK) { rc = FASTQ_ERR_REDIS; break; }
        if (r->type == REDIS_REPLY_INTEGER) {
            switch (i) {
            case 0: case 1: case 2: out->pending += (int)r->integer; break;
            case 3: out->done = (int)r->integer; break;
            case 4: out->failed = (int)r->integer; break;
            case 5: out->delayed = (int)r->integer; break;
            }
        }
        freeReplyObject(r);
    }

    put_ctx(q, ctx);
    return rc;
}

/* Dead Letter Queue */

fastq_job_t **fastq_dlq_list(fastq_queue_t *q, int *count)
{
    if (!q || !count) return NULL;
    *count = 0;

    redisContext *ctx = get_ctx(q);
    if (!ctx) return NULL;

    redisReply *r = redisCommand(ctx, "LRANGE %s 0 -1", q->key_dead);
    put_ctx(q, ctx);

    if (!r || r->type != REDIS_REPLY_ARRAY || r->elements == 0) {
        if (r) freeReplyObject(r);
        return NULL;
    }

    int n = (int)r->elements;
    fastq_job_t **jobs = calloc((size_t)n, sizeof(fastq_job_t *));
    if (!jobs) { freeReplyObject(r); return NULL; }

    int valid = 0;
    for (int i = 0; i < n; i++) {
        fastq_job_t *job = fastq_job_deserialize(r->element[i]->str);
        if (job) jobs[valid++] = job;
    }
    freeReplyObject(r);

    *count = valid;
    return jobs;
}

fastq_err_t fastq_dlq_retry(fastq_queue_t *q, const char *job_id)
{
    if (!q || !job_id) return FASTQ_ERR;

    redisContext *ctx = get_ctx(q);
    if (!ctx) return FASTQ_ERR_REDIS;

    redisReply *r = redisCommand(ctx, "LRANGE %s 0 -1", q->key_dead);
    if (!r || r->type != REDIS_REPLY_ARRAY) {
        if (r) freeReplyObject(r);
        put_ctx(q, ctx);
        return FASTQ_ERR_NOT_FOUND;
    }

    fastq_err_t rc = FASTQ_ERR_NOT_FOUND;
    for (size_t i = 0; i < r->elements; i++) {
        fastq_job_t *job = fastq_job_deserialize(r->element[i]->str);
        if (!job) continue;

        if (strcmp(job->id, job_id) == 0) {
            redisReply *rr = redisCommand(ctx, "LREM %s 1 %s", q->key_dead, r->element[i]->str);
            if (rr) freeReplyObject(rr);
            job->retries = 0;
            free(job->error);
            job->error = NULL;
            put_ctx(q, ctx);
            fastq_push(q, job);
            fastq_job_destroy(job);
            rc = FASTQ_OK;
            break;
        }
        fastq_job_destroy(job);
    }

    freeReplyObject(r);
    if (rc != FASTQ_OK) put_ctx(q, ctx);
    fastq_log(FASTQ_LOG_INFO, "dlq: retried job %s", job_id);
    return rc;
}

fastq_err_t fastq_dlq_delete(fastq_queue_t *q, const char *job_id)
{
    if (!q || !job_id) return FASTQ_ERR;

    redisContext *ctx = get_ctx(q);
    if (!ctx) return FASTQ_ERR_REDIS;

    redisReply *r = redisCommand(ctx, "LRANGE %s 0 -1", q->key_dead);
    if (!r || r->type != REDIS_REPLY_ARRAY) {
        if (r) freeReplyObject(r);
        put_ctx(q, ctx);
        return FASTQ_ERR_NOT_FOUND;
    }

    fastq_err_t rc = FASTQ_ERR_NOT_FOUND;
    for (size_t i = 0; i < r->elements; i++) {
        fastq_job_t *job = fastq_job_deserialize(r->element[i]->str);
        if (!job) continue;

        if (strcmp(job->id, job_id) == 0) {
            char hkey[KEY_MAX];
            JOB_HKEY(hkey, job_id);

            redisAppendCommand(ctx, "LREM %s 1 %s", q->key_dead, r->element[i]->str);
            redisAppendCommand(ctx, "DEL %s", hkey);

            redisReply *rr;
            for (int i2 = 0; i2 < 2; i2++) {
                if (redisGetReply(ctx, (void **)&rr) == REDIS_OK)
                    freeReplyObject(rr);
            }

            fastq_job_destroy(job);
            rc = FASTQ_OK;
            break;
        }
        fastq_job_destroy(job);
    }

    freeReplyObject(r);
    put_ctx(q, ctx);
    return rc;
}

/* Crash recovery */

int fastq_recover_orphaned_jobs(fastq_queue_t *q)
{
    if (!q) return 0;

    redisContext *ctx = get_ctx(q);
    if (!ctx) return 0;

    int recovered = 0;
    unsigned long long cursor = 0;

    do {
        redisReply *r = redisCommand(ctx, "SCAN %llu MATCH fastq:job:* COUNT 100", cursor);
        if (!r || r->type != REDIS_REPLY_ARRAY || r->elements != 2) {
            if (r) freeReplyObject(r);
            break;
        }

        cursor = strtoull(r->element[0]->str, NULL, 10);
        redisReply *keys = r->element[1];

        for (size_t i = 0; i < keys->elements; i++) {
            const char *hkey = keys->element[i]->str;
            redisReply *status = redisCommand(ctx, "HGET %s status", hkey);
            if (!status) continue;

            if (status->type == REDIS_REPLY_STRING &&
                strcmp(status->str, "processing") == 0) {

                redisReply *data = redisCommand(ctx, "HGET %s data", hkey);
                if (data && data->type == REDIS_REPLY_STRING) {
                    fastq_job_t *job = fastq_job_deserialize(data->str);
                    if (job) {
                        char *json = fastq_job_serialize(job);
                        if (json) {
                            const char *qkey = priority_key(q, job->priority);
                            redisAppendCommand(ctx, "RPUSH %s %s", qkey, json);
                            redisAppendCommand(ctx, "HSET %s status queued", hkey);
                            redisReply *rr;
                            for (int j = 0; j < 2; j++) {
                                if (redisGetReply(ctx, (void **)&rr) == REDIS_OK)
                                    freeReplyObject(rr);
                            }
                            free(json);
                            recovered++;
                            fastq_log(FASTQ_LOG_INFO, "recovery: re-queued job %s", job->id);
                        }
                        fastq_job_destroy(job);
                    }
                }
                if (data) freeReplyObject(data);
            }
            freeReplyObject(status);
        }
        freeReplyObject(r);
    } while (cursor != 0);

    put_ctx(q, ctx);
    fastq_log(FASTQ_LOG_INFO, "recovery: %d orphaned jobs recovered", recovered);
    return recovered;
}
