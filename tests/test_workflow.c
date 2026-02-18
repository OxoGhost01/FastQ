#include "fastq.h"
#include "fastq_internal.h"
#include <hiredis/hiredis.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define PASS(name) printf("  %-40s OK\n", name)
#define FAIL(name, msg) do { printf("  %-40s FAIL: %s\n", name, msg); failures++; } while(0)

static int failures = 0;
static fastq_redis_t *g_redis;
static fastq_queue_t *g_queue;

static void test_chain_basic(void)
{
    fastq_job_t *parent = fastq_job_create("{\"step\":1}", FASTQ_PRIORITY_NORMAL);
    fastq_job_t *child  = fastq_job_create("{\"step\":2}", FASTQ_PRIORITY_NORMAL);
    if (!parent || !child) { FAIL("chain_basic", "job_create failed"); goto done; }

    /* push parent and register chain */
    fastq_push(g_queue, parent);
    fastq_err_t rc = fastq_chain(g_queue, parent->id, child);
    if (rc != FASTQ_OK) { FAIL("chain_basic", "fastq_chain failed"); goto done; }

    /* pop parent and mark done — child should be auto-pushed */
    fastq_job_t *popped = fastq_pop(g_queue, 1);
    if (!popped) { FAIL("chain_basic", "pop returned NULL"); goto done; }
    fastq_job_done(g_queue, popped);
    fastq_job_destroy(popped);

    /* child should now be in queue */
    sleep(0); /* yield */
    fastq_job_t *child_popped = fastq_pop(g_queue, 1);
    if (!child_popped) { FAIL("chain_basic", "child job not auto-pushed"); goto done; }

    char *payload = child_popped->payload;
    if (!payload || strstr(payload, "step") == NULL) {
        FAIL("chain_basic", "child payload mismatch");
    } else {
        PASS("chain_basic");
    }
    fastq_job_done(g_queue, child_popped);
    fastq_job_destroy(child_popped);

done:
    fastq_job_destroy(parent);
    fastq_job_destroy(child);
}

static void test_chain_invalid_args(void)
{
    fastq_job_t *job = fastq_job_create("{}", FASTQ_PRIORITY_NORMAL);
    if (!job) { FAIL("chain_invalid", "job_create"); return; }

    if (fastq_chain(NULL, "id", job) == FASTQ_OK)
        FAIL("chain_invalid", "NULL queue accepted");
    if (fastq_chain(g_queue, NULL, job) == FASTQ_OK)
        FAIL("chain_invalid", "NULL parent_id accepted");
    if (fastq_chain(g_queue, "bad id!", job) == FASTQ_OK)
        FAIL("chain_invalid", "invalid parent_id accepted");
    if (fastq_chain(g_queue, "valid", NULL) == FASTQ_OK)
        FAIL("chain_invalid", "NULL child accepted");

    fastq_job_destroy(job);
    PASS("chain_invalid_args");
}

static void test_workflow_linear(void)
{
    /* A → B (B runs after A) */
    fastq_workflow_t *wf = fastq_workflow_create();
    if (!wf) { FAIL("workflow_linear", "create failed"); return; }

    fastq_job_t *a = fastq_job_create("{\"node\":\"A\"}", FASTQ_PRIORITY_NORMAL);
    fastq_job_t *b = fastq_job_create("{\"node\":\"B\"}", FASTQ_PRIORITY_NORMAL);
    if (!a || !b) { FAIL("workflow_linear", "job_create failed"); goto done; }

    fastq_workflow_add_job(wf, a);
    fastq_workflow_add_job(wf, b);
    fastq_workflow_add_dep(wf, a->id, b->id);  /* b depends on a */

    char wf_id[32];
    fastq_err_t rc = fastq_workflow_submit(wf, g_queue, wf_id, sizeof(wf_id));
    if (rc != FASTQ_OK) { FAIL("workflow_linear", "submit failed"); goto done; }

    int total, remaining;
    fastq_workflow_status(g_queue, wf_id, &total, &remaining);
    if (total != 2) { FAIL("workflow_linear", "total != 2"); goto done; }
    if (remaining != 2) { FAIL("workflow_linear", "remaining != 2 before any completion"); goto done; }

    /* Only A should be in queue (root) */
    fastq_job_t *job_a = fastq_pop(g_queue, 1);
    if (!job_a) { FAIL("workflow_linear", "A not in queue"); goto done; }

    /* B should NOT be in queue yet (timeout=1 so BLPOP returns NULL on empty) */
    fastq_job_t *job_b_early = fastq_pop(g_queue, 1);
    if (job_b_early) {
        FAIL("workflow_linear", "B pushed before A completed");
        fastq_job_done(g_queue, job_b_early);
        fastq_job_destroy(job_b_early);
        fastq_job_done(g_queue, job_a);
        fastq_job_destroy(job_a);
        goto done;
    }

    /* complete A → should unlock B */
    fastq_job_done(g_queue, job_a);
    fastq_job_destroy(job_a);

    fastq_job_t *job_b = fastq_pop(g_queue, 2);
    if (!job_b) { FAIL("workflow_linear", "B not pushed after A completed"); goto done; }
    fastq_job_done(g_queue, job_b);
    fastq_job_destroy(job_b);

    PASS("workflow_linear");

done:
    fastq_job_destroy(a);
    fastq_job_destroy(b);
    fastq_workflow_destroy(wf);
}

static void test_workflow_status(void)
{
    int total = -1, remaining = -1;
    fastq_err_t rc = fastq_workflow_status(g_queue, "nonexistent123", &total, &remaining);
    if (rc == FASTQ_OK) FAIL("workflow_status", "nonexistent should return error");
    else PASS("workflow_status (nonexistent)");
}

int main(void)
{
    fastq_set_log_level(FASTQ_LOG_ERROR);
    printf("test_workflow:\n");

    g_redis = fastq_redis_connect("127.0.0.1", 6379);
    if (!g_redis) { fprintf(stderr, "  redis connect failed\n"); return 1; }

    /* flush stale queue keys from previous runs */
    {
        redisContext *ctx = fastq_redis_get_ctx(g_redis);
        if (ctx) {
            const char *suffixes[] = {"high", "normal", "low", "delayed", "dead", "done"};
            for (int i = 0; i < 6; i++) {
                char key[256];
                snprintf(key, sizeof(key), "fastq:queue:test-wf:%s", suffixes[i]);
                redisReply *r = redisCommand(ctx, "DEL %s", key);
                if (r) freeReplyObject(r);
            }
        }
    }

    g_queue = fastq_queue_create(g_redis, "test-wf");
    if (!g_queue) { fprintf(stderr, "  queue create failed\n"); return 1; }

    test_chain_invalid_args();
    test_chain_basic();
    test_workflow_linear();
    test_workflow_status();

    fastq_queue_destroy(g_queue);
    fastq_redis_disconnect(g_redis);

    if (failures > 0) {
        printf("  %d test(s) failed.\n", failures);
        return 1;
    }
    printf("  All workflow tests passed.\n");
    return 0;
}
