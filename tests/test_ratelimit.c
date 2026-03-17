#include "fastq.h"
#include <stdio.h>
#include <unistd.h>

#define PASS(name) printf("  %-40s OK\n", name)
#define FAIL(name, msg) do { printf("  %-40s FAIL: %s\n", name, msg); failures++; } while(0)

static int failures = 0;

static void test_basic_acquire(void)
{
    /* capacity=5: first 5 acquires succeed, 6th fails */
    fastq_ratelimit_t *rl = fastq_ratelimit_create(5, 10);
    if (!rl) { FAIL("basic_acquire", "create failed"); return; }

    for (int i = 0; i < 5; i++) {
        if (!fastq_ratelimit_acquire(rl)) {
            FAIL("basic_acquire", "expected success");
            fastq_ratelimit_destroy(rl);
            return;
        }
    }
    if (fastq_ratelimit_acquire(rl)) {
        FAIL("basic_acquire", "expected failure after capacity exhausted");
        fastq_ratelimit_destroy(rl);
        return;
    }

    fastq_ratelimit_destroy(rl);
    PASS("basic_acquire");
}

static void test_refill(void)
{
    /* capacity=2, refill=2/s: drain, wait 1s, should have 2 tokens again */
    fastq_ratelimit_t *rl = fastq_ratelimit_create(2, 2);
    if (!rl) { FAIL("refill", "create failed"); return; }

    fastq_ratelimit_acquire(rl);
    fastq_ratelimit_acquire(rl);

    if (fastq_ratelimit_acquire(rl)) {
        FAIL("refill", "expected fail when empty");
        fastq_ratelimit_destroy(rl);
        return;
    }

    sleep(1);

    if (!fastq_ratelimit_acquire(rl)) {
        FAIL("refill", "expected success after refill");
        fastq_ratelimit_destroy(rl);
        return;
    }

    fastq_ratelimit_destroy(rl);
    PASS("refill");
}

static void test_invalid_args(void)
{
    if (fastq_ratelimit_create(0, 1)  != NULL) FAIL("invalid_args", "capacity=0 accepted");
    if (fastq_ratelimit_create(-1, 1) != NULL) FAIL("invalid_args", "capacity<0 accepted");
    if (fastq_ratelimit_create(1, 0)  != NULL) FAIL("invalid_args", "refill=0 accepted");
    if (fastq_ratelimit_create(1, -1) != NULL) FAIL("invalid_args", "refill<0 accepted");
    if (fastq_ratelimit_acquire(NULL) != true)  FAIL("invalid_args", "acquire(NULL) should return true");
    PASS("invalid_args");
}

static void test_null_limiter(void)
{
    /* NULL ratelimit means "no limit" — acquire should always return true */
    for (int i = 0; i < 100; i++) {
        if (!fastq_ratelimit_acquire(NULL)) {
            FAIL("null_limiter", "NULL should always allow");
            return;
        }
    }
    PASS("null_limiter");
}

int main(void)
{
    fastq_set_log_level(FASTQ_LOG_ERROR);
    printf("test_ratelimit:\n");

    test_invalid_args();
    test_basic_acquire();
    test_refill();
    test_null_limiter();

    if (failures > 0) {
        printf("  %d test(s) failed.\n", failures);
        return 1;
    }
    printf("  All ratelimit tests passed.\n");
    return 0;
}
