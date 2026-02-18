/*
 * ratelimit.c — Token bucket rate limiter.
 *
 * Each call to fastq_ratelimit_acquire() consumes one token.
 * Tokens are refilled at refill_per_sec per second, capped at capacity.
 *
 * All arithmetic is bounds-checked to prevent overflow:
 *   - capacity and refill_per_sec are validated > 0 and <= INT_MAX/2
 *   - token count is always in [0, capacity]
 *   - elapsed time is capped to avoid multiplication overflow
 */

#include "fastq.h"
#include <stdlib.h>
#include <time.h>
#include <pthread.h>

#define RATELIMIT_MAX_CAPACITY 1000000

struct fastq_ratelimit_t {
    int capacity;
    int tokens;
    int refill_per_sec;
    struct timespec last_refill;
    pthread_mutex_t lock;
};

fastq_ratelimit_t *fastq_ratelimit_create(int capacity, int refill_per_sec)
{
    if (capacity <= 0 || capacity > RATELIMIT_MAX_CAPACITY) return NULL;
    if (refill_per_sec <= 0 || refill_per_sec > RATELIMIT_MAX_CAPACITY) return NULL;

    fastq_ratelimit_t *rl = calloc(1, sizeof(*rl));
    if (!rl) return NULL;

    rl->capacity = capacity;
    rl->tokens = capacity;  /* start full */
    rl->refill_per_sec = refill_per_sec;
    clock_gettime(CLOCK_MONOTONIC, &rl->last_refill);
    pthread_mutex_init(&rl->lock, NULL);
    return rl;
}

void fastq_ratelimit_destroy(fastq_ratelimit_t *rl)
{
    if (!rl) return;
    pthread_mutex_destroy(&rl->lock);
    free(rl);
}

bool fastq_ratelimit_acquire(fastq_ratelimit_t *rl)
{
    if (!rl) return true;  /* no limiter = always allowed */

    pthread_mutex_lock(&rl->lock);

    /* compute elapsed seconds since last refill */
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);

    long sec  = now.tv_sec - rl->last_refill.tv_sec;
    long nsec = now.tv_nsec - rl->last_refill.tv_nsec;
    if (nsec < 0) { sec--; nsec += 1000000000L; }

    /* cap elapsed to prevent overflow: max meaningful elapsed = capacity/rate seconds */
    long max_elapsed = rl->capacity / rl->refill_per_sec + 1;
    if (sec > max_elapsed) sec = max_elapsed;

    /* add tokens (guarded against overflow) */
    if (sec > 0) {
        long new_tokens = (long)rl->tokens + (long)sec * rl->refill_per_sec;
        rl->tokens = (int)(new_tokens > rl->capacity ? rl->capacity : new_tokens);
        rl->last_refill = now;
        rl->last_refill.tv_nsec = 0;  /* align to whole seconds */
    }

    bool allowed = (rl->tokens > 0);
    if (allowed) rl->tokens--;

    pthread_mutex_unlock(&rl->lock);
    return allowed;
}
