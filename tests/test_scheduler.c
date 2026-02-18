#include "fastq.h"
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#define PASS(name) printf("  %-40s OK\n", name)
#define FAIL(name, msg) do { printf("  %-40s FAIL: %s\n", name, msg); failures++; } while(0)

static int failures = 0;
static fastq_redis_t  *g_redis;
static fastq_queue_t  *g_queue;

static void test_validate_name(void)
{
    /* valid names */
    if (fastq_validate_name("hello") != FASTQ_OK) FAIL("validate:hello", "expected OK");
    if (fastq_validate_name("q-1_test") != FASTQ_OK) FAIL("validate:q-1_test","expected OK");
    if (fastq_validate_name("A") != FASTQ_OK) FAIL("validate:A", "expected OK");

    /* invalid names */
    if (fastq_validate_name(NULL) == FASTQ_OK) FAIL("validate:null", "expected INVALID");
    if (fastq_validate_name("") == FASTQ_OK) FAIL("validate:empty", "expected INVALID");
    if (fastq_validate_name("bad name") == FASTQ_OK) FAIL("validate:space", "expected INVALID");
    if (fastq_validate_name("x;DROP TABLE;") == FASTQ_OK) FAIL("validate:injection","expected INVALID");
    if (fastq_validate_name("../etc/passwd") == FASTQ_OK) FAIL("validate:path", "expected INVALID");

    PASS("validate_name (valid + invalid)");
}

static void test_invalid_queue_name(void)
{
    fastq_queue_t *q = fastq_queue_create(g_redis, "bad name!");
    if (q != NULL) {
        fastq_queue_destroy(q);
        FAIL("invalid_queue_name", "should have returned NULL");
        return;
    }
    PASS("invalid_queue_name");
}

static void test_schedule_oneshot(void)
{
    /* schedule a job 2 seconds in the future */
    fastq_job_t *job = fastq_job_create("{\"scheduled\":true}", FASTQ_PRIORITY_NORMAL);
    time_t run_at = time(NULL) + 2;
    fastq_err_t rc = fastq_schedule(g_queue, job, run_at);
    fastq_job_destroy(job);

    if (rc != FASTQ_OK) {
        FAIL("schedule_oneshot", "fastq_schedule failed");
        return;
    }

    /* verify it's in delayed set */
    fastq_stats_t s;
    fastq_stats(g_queue, &s);
    if (s.delayed < 1) {
        FAIL("schedule_oneshot", "job not in delayed set");
        return;
    }

    PASS("schedule_oneshot");
}

static void test_cron_add_remove(void)
{
    fastq_scheduler_t *s = fastq_scheduler_create(g_queue);
    if (!s) { FAIL("cron_add_remove", "scheduler_create failed"); return; }

    /* valid cron entry */
    fastq_err_t rc = fastq_scheduler_add_cron(s, "test-job", "* * * * *", "{\"cron\":true}", FASTQ_PRIORITY_NORMAL);
    if (rc != FASTQ_OK) { FAIL("cron_add_remove", "add_cron failed"); goto done; }

    /* duplicate id should fail */
    rc = fastq_scheduler_add_cron(s, "test-job", "*/5 * * * *", "{}", FASTQ_PRIORITY_NORMAL);
    if (rc == FASTQ_OK) { FAIL("cron_add_remove", "duplicate id accepted"); goto done; }

    /* invalid cron expression */
    rc = fastq_scheduler_add_cron(s, "bad-cron", "invalid expr", "{}", FASTQ_PRIORITY_NORMAL);
    if (rc == FASTQ_OK) { FAIL("cron_add_remove", "bad cron accepted"); goto done; }

    /* invalid id */
    rc = fastq_scheduler_add_cron(s, "bad id!", "* * * * *", "{}", FASTQ_PRIORITY_NORMAL);
    if (rc == FASTQ_OK) { FAIL("cron_add_remove", "bad id accepted"); goto done; }

    /* remove */
    rc = fastq_scheduler_remove(s, "test-job");
    if (rc != FASTQ_OK) { FAIL("cron_add_remove", "remove failed"); goto done; }

    PASS("cron_add_remove");
done:
    fastq_scheduler_destroy(s);
}

static void test_cron_fires(void)
{
    /* cron "* * * * *" fires every minute — too slow to test directly.
       Instead, we test that start/stop cycle works without crash. */
    fastq_scheduler_t *s = fastq_scheduler_create(g_queue);
    if (!s) { FAIL("cron_fires", "scheduler_create failed"); return; }

    fastq_scheduler_add_cron(s, "every-min", "* * * * *", "{\"smoke\":true}", FASTQ_PRIORITY_LOW);

    fastq_err_t rc = fastq_scheduler_start(s);
    if (rc != FASTQ_OK) { FAIL("cron_fires", "start failed"); fastq_scheduler_destroy(s); return; }

    /* let it run for 1s (won't fire since cron is per-minute) */
    sleep(1);
    fastq_scheduler_stop(s);
    fastq_scheduler_destroy(s);

    PASS("cron_fires (smoke test)");
}

int main(void)
{
    fastq_set_log_level(FASTQ_LOG_ERROR);
    printf("test_scheduler:\n");

    g_redis = fastq_redis_connect("127.0.0.1", 6379);
    if (!g_redis) { fprintf(stderr, "  redis connect failed\n"); return 1; }
    g_queue = fastq_queue_create(g_redis, "test-sched");
    if (!g_queue) { fprintf(stderr, "  queue create failed\n"); return 1; }

    test_validate_name();
    test_invalid_queue_name();
    test_schedule_oneshot();
    test_cron_add_remove();
    test_cron_fires();

    fastq_queue_destroy(g_queue);
    fastq_redis_disconnect(g_redis);

    if (failures > 0) {
        printf("  %d test(s) failed.\n", failures);
        return 1;
    }
    printf("  All scheduler tests passed.\n");
    return 0;
}
