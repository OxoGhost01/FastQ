#include "fastq.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define TEST(name) static void name(void)
#define RUN(name) do { printf("  %-40s", #name); name(); printf("OK\n"); } while(0)

static fastq_redis_t *g_redis = NULL;

static void setup(void)
{
    g_redis = fastq_redis_connect("127.0.0.1", 6379);
    assert(g_redis != NULL);
}

static void teardown(void)
{
    fastq_redis_disconnect(g_redis);
    g_redis = NULL;
}

TEST(test_push_pop_single)
{
    setup();
    fastq_queue_t *q = fastq_queue_create(g_redis, "test2_single");
    assert(q != NULL);

    fastq_job_t *job = fastq_job_create("{\"action\":\"send_email\"}", FASTQ_PRIORITY_NORMAL);
    assert(job != NULL);
    char *orig_id = strdup(job->id);

    assert(fastq_push(q, job) == FASTQ_OK);
    fastq_job_destroy(job);

    fastq_job_t *popped = fastq_pop(q, 1);
    assert(popped != NULL);
    assert(strcmp(popped->id, orig_id) == 0);
    assert(strcmp(popped->payload, "{\"action\":\"send_email\"}") == 0);

    fastq_job_done(q, popped);
    fastq_job_destroy(popped);
    free(orig_id);
    fastq_queue_destroy(q);
    teardown();
}

TEST(test_push_fifo_order)
{
    setup();
    fastq_queue_t *q = fastq_queue_create(g_redis, "test2_fifo");

    char *ids[10];
    for (int i = 0; i < 10; i++) {
        char payload[64];
        snprintf(payload, sizeof(payload), "{\"n\":%d}", i);
        fastq_job_t *job = fastq_job_create(payload, FASTQ_PRIORITY_NORMAL);
        ids[i] = strdup(job->id);
        assert(fastq_push(q, job) == FASTQ_OK);
        fastq_job_destroy(job);
    }

    for (int i = 0; i < 10; i++) {
        fastq_job_t *job = fastq_pop(q, 1);
        assert(job != NULL);
        assert(strcmp(job->id, ids[i]) == 0);
        fastq_job_done(q, job);
        fastq_job_destroy(job);
        free(ids[i]);
    }

    fastq_queue_destroy(q);
    teardown();
}

TEST(test_pop_timeout)
{
    setup();
    fastq_queue_t *q = fastq_queue_create(g_redis, "test2_empty_pop");

    fastq_job_t *job = fastq_pop(q, 1);
    assert(job == NULL);

    fastq_queue_destroy(q);
    teardown();
}

TEST(test_priority_ordering)
{
    setup();
    fastq_queue_t *q = fastq_queue_create(g_redis, "test2_prio");

    /* Push low first, then high */
    fastq_job_t *low = fastq_job_create("{\"p\":\"low\"}", FASTQ_PRIORITY_LOW);
    fastq_job_t *norm = fastq_job_create("{\"p\":\"normal\"}", FASTQ_PRIORITY_NORMAL);
    fastq_job_t *high = fastq_job_create("{\"p\":\"high\"}", FASTQ_PRIORITY_HIGH);

    fastq_push(q, low);
    fastq_push(q, norm);
    fastq_push(q, high);

    fastq_job_destroy(low);
    fastq_job_destroy(norm);
    fastq_job_destroy(high);

    /* Pop should return: high, normal, low */
    fastq_job_t *j1 = fastq_pop(q, 1);
    assert(j1 != NULL);
    assert(strcmp(j1->payload, "{\"p\":\"high\"}") == 0);
    fastq_job_done(q, j1);
    fastq_job_destroy(j1);

    fastq_job_t *j2 = fastq_pop(q, 1);
    assert(j2 != NULL);
    assert(strcmp(j2->payload, "{\"p\":\"normal\"}") == 0);
    fastq_job_done(q, j2);
    fastq_job_destroy(j2);

    fastq_job_t *j3 = fastq_pop(q, 1);
    assert(j3 != NULL);
    assert(strcmp(j3->payload, "{\"p\":\"low\"}") == 0);
    fastq_job_done(q, j3);
    fastq_job_destroy(j3);

    fastq_queue_destroy(q);
    teardown();
}

TEST(test_stats)
{
    setup();
    fastq_queue_t *q = fastq_queue_create(g_redis, "test2_stats");

    for (int i = 0; i < 3; i++) {
        fastq_job_t *job = fastq_job_create("{}", FASTQ_PRIORITY_NORMAL);
        fastq_push(q, job);
        fastq_job_destroy(job);
    }

    fastq_stats_t s;
    assert(fastq_stats(q, &s) == FASTQ_OK);
    assert(s.pending >= 3);

    fastq_job_t *job = fastq_pop(q, 1);
    assert(job != NULL);
    fastq_job_done(q, job);
    fastq_job_destroy(job);

    assert(fastq_stats(q, &s) == FASTQ_OK);
    assert(s.done >= 1);

    fastq_queue_destroy(q);
    teardown();
}

TEST(test_job_fail_retry)
{
    setup();
    fastq_queue_t *q = fastq_queue_create(g_redis, "test2_retry");

    fastq_job_t *job = fastq_job_create("{\"retry\":true}", FASTQ_PRIORITY_NORMAL);
    job->max_retries = 3;
    fastq_push(q, job);
    fastq_job_destroy(job);

    fastq_job_t *popped = fastq_pop(q, 1);
    assert(popped != NULL);
    assert(popped->retries == 0);

    fastq_job_fail(q, popped, "temporary error");
    assert(popped->retries == 1);

    fastq_stats_t s;
    fastq_stats(q, &s);
    assert(s.delayed >= 1);

    fastq_job_destroy(popped);
    fastq_queue_destroy(q);
    teardown();
}

TEST(test_job_fail_to_dlq)
{
    setup();
    fastq_queue_t *q = fastq_queue_create(g_redis, "test2_dlq");

    fastq_job_t *job = fastq_job_create("{\"dlq\":true}", FASTQ_PRIORITY_NORMAL);
    job->max_retries = 1;
    fastq_push(q, job);
    char *job_id = strdup(job->id);
    fastq_job_destroy(job);

    fastq_job_t *popped = fastq_pop(q, 1);
    assert(popped != NULL);

    fastq_job_fail(q, popped, "permanent error");

    fastq_stats_t s;
    fastq_stats(q, &s);
    assert(s.failed >= 1);

    int count = 0;
    fastq_job_t **dead = fastq_dlq_list(q, &count);
    assert(count >= 1);
    assert(dead != NULL);

    for (int i = 0; i < count; i++)
        fastq_job_destroy(dead[i]);
    free(dead);

    fastq_job_destroy(popped);
    free(job_id);
    fastq_queue_destroy(q);
    teardown();
}

TEST(test_pooled_queue)
{
    fastq_queue_t *q = fastq_queue_create_pooled("127.0.0.1", 6379,
                                                  "test2_pooled", 4);
    assert(q != NULL);

    fastq_job_t *job = fastq_job_create("{\"pooled\":true}", FASTQ_PRIORITY_HIGH);
    assert(fastq_push(q, job) == FASTQ_OK);
    fastq_job_destroy(job);

    fastq_job_t *popped = fastq_pop(q, 1);
    assert(popped != NULL);
    assert(strcmp(popped->payload, "{\"pooled\":true}") == 0);
    fastq_job_done(q, popped);
    fastq_job_destroy(popped);

    fastq_queue_destroy(q);
}

int main(void)
{
    fastq_set_log_level(FASTQ_LOG_WARN);

    printf("test_queue:\n");
    RUN(test_push_pop_single);
    RUN(test_push_fifo_order);
    RUN(test_pop_timeout);
    RUN(test_priority_ordering);
    RUN(test_stats);
    RUN(test_job_fail_retry);
    RUN(test_job_fail_to_dlq);
    RUN(test_pooled_queue);
    printf("  All queue tests passed.\n");
    return 0;
}
