/*
 * workflow.c — Job chaining and DAG workflow engine.
 *
 * Chaining (simple):
 *   fastq_chain(q, parent_id, child_job)
 *   → stores child in Redis: "fastq:chain:{parent_id}" = serialized child JSON
 *   → when parent completes, job_done_impl calls fastq_chain_trigger()
 *     which pushes the child automatically.
 *
 * Workflows (DAG):
 *   fastq_workflow_create()
 *   fastq_workflow_add_job(wf, job)        — add a job node
 *   fastq_workflow_add_dep(wf, A_id, B_id) — B runs after A
 *   fastq_workflow_submit(wf, q, wf_id_out, size)
 *     → assigns wf_id, writes state to Redis, pushes root jobs
 *
 *   Redis schema for workflow WF:
 *     fastq:wf:{WF}          — HSET: total N, remaining N
 *     fastq:wf:{WF}:wait:{J} — DECR counter (how many deps still pending)
 *     fastq:wf:{WF}:out:{J}  — SMEMBERS: IDs of jobs that unblock when J completes
 *     fastq:wf:{WF}:job:{J}  — serialized job JSON (pushed when wait reaches 0)
 *
 *   When a workflow job J completes (fastq_chain_trigger sees wf: prefix):
 *     1. SMEMBERS fastq:wf:{WF}:out:{J}  → children C1, C2, ...
 *     2. For each Ci: HINCRBY fastq:wf:{WF}:wait Ci -1
 *        → if result == 0: GET fastq:wf:{WF}:job:{Ci} and push to queue
 *     3. HINCRBY fastq:wf:{WF} remaining -1
 *
 * Security:
 *   - All job IDs and wf_ids are validated via fastq_validate_name().
 *   - Redis keys are bounded-size strings built with snprintf.
 *   - Workflow dep cycles are not enforced at submission (Lua scripting
 *     would be needed for full cycle detection); the caller is responsible.
 *   - Max jobs per workflow: WORKFLOW_MAX_JOBS (256).
 */

#include "fastq.h"
#include "fastq_internal.h"
#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define KEY_MAX 256
#define WORKFLOW_MAX_JOBS 256

/* In-memory workflow representation */

typedef struct {
    fastq_job_t *job; /* owned copy */
    char *deps[WORKFLOW_MAX_JOBS]; /* IDs this job depends on */
    int dep_count;
} wf_node_t;

struct fastq_workflow_t {
    wf_node_t nodes[WORKFLOW_MAX_JOBS];
    int count;
};

/* Workflow creation */

fastq_workflow_t *fastq_workflow_create(void)
{
    return calloc(1, sizeof(fastq_workflow_t));
}

void fastq_workflow_destroy(fastq_workflow_t *wf)
{
    if (!wf) return;
    for (int i = 0; i < wf->count; i++) {
        fastq_job_destroy(wf->nodes[i].job);
        for (int d = 0; d < wf->nodes[i].dep_count; d++)
            free(wf->nodes[i].deps[d]);
    }
    free(wf);
}

fastq_err_t fastq_workflow_add_job(fastq_workflow_t *wf, fastq_job_t *job)
{
    if (!wf || !job) return FASTQ_ERR;
    if (wf->count >= WORKFLOW_MAX_JOBS) return FASTQ_ERR;
    if (!job->id || fastq_validate_name(job->id) != FASTQ_OK)
        return FASTQ_ERR_INVALID;

    /* check for duplicate */
    for (int i = 0; i < wf->count; i++) {
        if (strcmp(wf->nodes[i].job->id, job->id) == 0)
            return FASTQ_ERR_INVALID;
    }

    /* deep-copy the job */
    char *json = fastq_job_serialize(job);
    if (!json) return FASTQ_ERR_SERIALIZE;
    fastq_job_t *copy = fastq_job_deserialize(json);
    free(json);
    if (!copy) return FASTQ_ERR_ALLOC;

    wf_node_t *n = &wf->nodes[wf->count++];
    memset(n, 0, sizeof(*n));
    n->job = copy;
    return FASTQ_OK;
}

fastq_err_t fastq_workflow_add_dep(fastq_workflow_t *wf, const char *before_id, const char *after_id)
{
    if (!wf || !before_id || !after_id) return FASTQ_ERR;
    if (fastq_validate_name(before_id) != FASTQ_OK) return FASTQ_ERR_INVALID;
    if (fastq_validate_name(after_id) != FASTQ_OK) return FASTQ_ERR_INVALID;

    /* find the 'after' node */
    wf_node_t *after_node = NULL;
    for (int i = 0; i < wf->count; i++) {
        if (strcmp(wf->nodes[i].job->id, after_id) == 0) {
            after_node = &wf->nodes[i];
            break;
        }
    }
    if (!after_node) return FASTQ_ERR_NOT_FOUND;
    if (after_node->dep_count >= WORKFLOW_MAX_JOBS) return FASTQ_ERR;

    char *dep = strdup(before_id);
    if (!dep) return FASTQ_ERR_ALLOC;
    after_node->deps[after_node->dep_count++] = dep;
    return FASTQ_OK;
}

fastq_err_t fastq_workflow_submit(fastq_workflow_t *wf, fastq_queue_t *q, char *wf_id_out, size_t wf_id_size)
{
    if (!wf || !q || !wf_id_out || wf_id_size < 32) return FASTQ_ERR;
    if (wf->count == 0) return FASTQ_ERR;

    /* generate workflow ID */
    char *gen_id = fastq_generate_id();
    if (!gen_id) return FASTQ_ERR_ALLOC;
    snprintf(wf_id_out, wf_id_size, "%s", gen_id);
    free(gen_id);

    const char *qname = fastq_queue_get_name(q);
    fastq_pool_t *pool = fastq_queue_get_pool(q);
    redisContext *ctx  = pool ? fastq_pool_acquire(pool) : fastq_redis_get_ctx(fastq_queue_get_redis(q));
    if (!ctx) return FASTQ_ERR_REDIS;

    char wf_key[KEY_MAX];
    snprintf(wf_key, sizeof(wf_key), "fastq:wf:%s", wf_id_out);

    /* persist total count */
    redisReply *r = redisCommand(ctx, "HSET %s total %d remaining %d", wf_key, wf->count, wf->count);
    if (r) freeReplyObject(r);

    /* persist each job and its out-edges */
    for (int i = 0; i < wf->count; i++) {
        wf_node_t *n = &wf->nodes[i];
        fastq_job_t *job = n->job;

        /* tag job with wf_id */
        free(job->wf_id);
        job->wf_id = strdup(wf_id_out);

        char *json = fastq_job_serialize(job);
        if (!json) continue;

        char job_key[KEY_MAX], wait_key[KEY_MAX];
        snprintf(job_key,  sizeof(job_key),  "fastq:wf:%s:job:%s", wf_id_out, job->id);
        snprintf(wait_key, sizeof(wait_key), "fastq:wf:%s:wait", wf_id_out);

        r = redisCommand(ctx, "SET %s %s", job_key, json);
        if (r) freeReplyObject(r);

        /* store the dependency counter */
        r = redisCommand(ctx, "HSET %s %s %d", wait_key, job->id, n->dep_count);
        if (r) freeReplyObject(r);

        free(json);

        /* store out-edges: for each dep D of this job, add job->id to
           fastq:wf:{WF}:out:{D} so we know to unblock this job when D finishes */
        for (int d = 0; d < n->dep_count; d++) {
            char out_key[KEY_MAX];
            snprintf(out_key, sizeof(out_key), "fastq:wf:%s:out:%s", wf_id_out, n->deps[d]);
            r = redisCommand(ctx, "SADD %s %s", out_key, job->id);
            if (r) freeReplyObject(r);
        }
    }

    if (pool) fastq_pool_release(pool, ctx);

    /* push root jobs (no deps) and register chain keys */
    for (int i = 0; i < wf->count; i++) {
        wf_node_t *n = &wf->nodes[i];
        fastq_job_t *job = n->job;

        /* chain key: "wf:{wf_id}" tells job_done to run workflow logic */
        char chain_key[KEY_MAX];
        snprintf(chain_key, sizeof(chain_key), "fastq:chain:%s", job->id);

        char wf_val[KEY_MAX];
        snprintf(wf_val, sizeof(wf_val), "wf:%s", wf_id_out);

        redisContext *ctx2 = pool ? fastq_pool_acquire(pool) : fastq_redis_get_ctx(fastq_queue_get_redis(q));
        if (ctx2) {
            r = redisCommand(ctx2, "SET %s %s", chain_key, wf_val);
            if (r) freeReplyObject(r);
            if (pool) fastq_pool_release(pool, ctx2);
        }

        if (n->dep_count == 0) {
            /* root node: push immediately */
            /* ensure the queue_name is set */
            free(job->queue_name);
            job->queue_name = strdup(qname);
            fastq_push(q, job);
            fastq_log(FASTQ_LOG_DEBUG, "workflow %s: pushed root job %s", wf_id_out, job->id);
        }
    }

    fastq_log(FASTQ_LOG_INFO, "workflow %s: submitted %d jobs", wf_id_out, wf->count);
    return FASTQ_OK;
}

fastq_err_t fastq_workflow_status(fastq_queue_t *q, const char *wf_id, int *total_out, int *remaining_out)
{
    if (!q || !wf_id || !total_out || !remaining_out) return FASTQ_ERR;
    if (fastq_validate_name(wf_id) != FASTQ_OK) return FASTQ_ERR_INVALID;

    fastq_pool_t *pool = fastq_queue_get_pool(q);
    redisContext *ctx  = pool ? fastq_pool_acquire(pool) : fastq_redis_get_ctx(fastq_queue_get_redis(q));
    if (!ctx) return FASTQ_ERR_REDIS;

    char wf_key[KEY_MAX];
    snprintf(wf_key, sizeof(wf_key), "fastq:wf:%s", wf_id);

    redisAppendCommand(ctx, "HGET %s total",     wf_key);
    redisAppendCommand(ctx, "HGET %s remaining", wf_key);

    redisReply *r1, *r2;
    fastq_err_t rc = FASTQ_OK;

    if (redisGetReply(ctx, (void **)&r1) != REDIS_OK) { rc = FASTQ_ERR_REDIS; goto done; }
    if (redisGetReply(ctx, (void **)&r2) != REDIS_OK) {
        freeReplyObject(r1); rc = FASTQ_ERR_REDIS; goto done;
    }

    if (!r1 || r1->type != REDIS_REPLY_STRING ||
        !r2 || r2->type != REDIS_REPLY_STRING) {
        rc = FASTQ_ERR_NOT_FOUND;
    } else {
        *total_out = atoi(r1->str);
        *remaining_out = atoi(r2->str);
    }
    freeReplyObject(r1);
    freeReplyObject(r2);

done:
    if (pool) fastq_pool_release(pool, ctx);
    return rc;
}

/* Chain trigger (called from job_done_impl in queue.c) */

/*
 * Check if job has a chained child or is part of a workflow, and act:
 *   - simple chain: GET fastq:chain:{id} → push serialized child job
 *   - workflow:     GET fastq:chain:{id} → "wf:{wf_id}" → run workflow logic
 */
void fastq_chain_trigger(fastq_queue_t *q, redisContext *ctx, const fastq_job_t *job)
{
    if (!q || !ctx || !job || !job->id) return;

    char chain_key[KEY_MAX];
    snprintf(chain_key, sizeof(chain_key), "fastq:chain:%s", job->id);

    redisReply *chain = redisCommand(ctx, "GET %s", chain_key);
    if (!chain || chain->type != REDIS_REPLY_STRING) {
        if (chain) freeReplyObject(chain);
        return;
    }

    const char *val = chain->str;

    if (strncmp(val, "wf:", 3) == 0) {
        /* workflow continuation */
        const char *wf_id = val + 3;
        if (fastq_validate_name(wf_id) != FASTQ_OK) {
            freeReplyObject(chain);
            return;
        }

        char out_key[KEY_MAX], wait_key[KEY_MAX], wf_key[KEY_MAX];
        snprintf(out_key, sizeof(out_key), "fastq:wf:%s:out:%s", wf_id, job->id);
        snprintf(wait_key, sizeof(wait_key), "fastq:wf:%s:wait", wf_id);
        snprintf(wf_key, sizeof(wf_key), "fastq:wf:%s", wf_id);

        /* get jobs that depend on this one completing */
        redisReply *members = redisCommand(ctx, "SMEMBERS %s", out_key);
        if (members && members->type == REDIS_REPLY_ARRAY) {
            for (size_t i = 0; i < members->elements; i++) {
                const char *child_id = members->element[i]->str;
                if (!child_id || fastq_validate_name(child_id) != FASTQ_OK)
                    continue;

                /* atomic decrement of this child's wait counter */
                redisReply *cnt = redisCommand(ctx, "HINCRBY %s %s -1", wait_key, child_id);
                if (cnt && cnt->type == REDIS_REPLY_INTEGER && cnt->integer == 0) {
                    /* all deps satisfied: fetch and push the child */
                    char job_key[KEY_MAX];
                    snprintf(job_key, sizeof(job_key), "fastq:wf:%s:job:%s", wf_id, child_id);
                    redisReply *job_data = redisCommand(ctx, "GET %s", job_key);
                    if (job_data && job_data->type == REDIS_REPLY_STRING) {
                        fastq_job_t *child = fastq_job_deserialize(job_data->str);
                        if (child) {
                            fastq_push(q, child);
                            fastq_log(FASTQ_LOG_DEBUG, "workflow %s: unlocked job %s", wf_id, child_id);
                            fastq_job_destroy(child);
                        }
                    }
                    if (job_data) freeReplyObject(job_data);
                }
                if (cnt) freeReplyObject(cnt);
            }
        }
        if (members) freeReplyObject(members);

        /* decrement global remaining count */
        redisReply *rem = redisCommand(ctx, "HINCRBY %s remaining -1", wf_key);
        if (rem) freeReplyObject(rem);

    } else {
        /* simple chain */
        fastq_job_t *child = fastq_job_deserialize(val);
        if (child) {
            fastq_push(q, child);
            fastq_log(FASTQ_LOG_DEBUG, "chain: pushed child of job %s", job->id);
            fastq_job_destroy(child);
        }
    }

    /* delete the chain key — it fires only once */
    redisReply *del = redisCommand(ctx, "DEL %s", chain_key);
    if (del) freeReplyObject(del);

    freeReplyObject(chain);
}

/* Public chain API */

fastq_err_t fastq_chain(fastq_queue_t *q, const char *parent_job_id, fastq_job_t *child_job)
{
    if (!q || !parent_job_id || !child_job) return FASTQ_ERR;
    if (fastq_validate_name(parent_job_id) != FASTQ_OK) return FASTQ_ERR_INVALID;

    char *json = fastq_job_serialize(child_job);
    if (!json) return FASTQ_ERR_SERIALIZE;

    fastq_pool_t *pool = fastq_queue_get_pool(q);
    redisContext *ctx  = pool ? fastq_pool_acquire(pool) : fastq_redis_get_ctx(fastq_queue_get_redis(q));
    if (!ctx) { free(json); return FASTQ_ERR_REDIS; }

    char chain_key[KEY_MAX];
    snprintf(chain_key, sizeof(chain_key), "fastq:chain:%s", parent_job_id);

    redisReply *r = redisCommand(ctx, "SET %s %s", chain_key, json);
    if (r) freeReplyObject(r);

    if (pool) fastq_pool_release(pool, ctx);
    free(json);

    fastq_log(FASTQ_LOG_DEBUG, "chain: registered child for parent %s", parent_job_id);
    return FASTQ_OK;
}
