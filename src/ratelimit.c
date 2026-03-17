/*
 * ratelimit.c — stub (free / public build).
 * Pro feature. All functions are no-ops that return FASTQ_ERR_LICENSE / NULL / false.
 * Replace with the real implementation for a licensed build.
 */

#include "fastq.h"

fastq_ratelimit_t *fastq_ratelimit_create(int capacity, int refill_per_sec)
{
    (void)capacity; (void)refill_per_sec;
    return NULL;
}

void fastq_ratelimit_destroy(fastq_ratelimit_t *rl)
{
    (void)rl;
}

bool fastq_ratelimit_acquire(fastq_ratelimit_t *rl)
{
    (void)rl;
    return false;
}
