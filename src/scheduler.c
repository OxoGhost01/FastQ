/*
 * scheduler.c — stub (free / public build).
 * Pro feature. All functions are no-ops that return FASTQ_ERR_LICENSE / NULL.
 * Replace with the real implementation for a licensed build.
 */

#include "fastq.h"

fastq_scheduler_t *fastq_scheduler_create(fastq_queue_t *q)
{
    (void)q;
    return NULL;
}

fastq_err_t fastq_scheduler_add_cron(fastq_scheduler_t *s, const char *id,
                                      const char *cron_expr, const char *payload,
                                      fastq_priority_t priority)
{
    (void)s; (void)id; (void)cron_expr; (void)payload; (void)priority;
    return FASTQ_ERR_LICENSE;
}

fastq_err_t fastq_scheduler_remove(fastq_scheduler_t *s, const char *id)
{
    (void)s; (void)id;
    return FASTQ_ERR_LICENSE;
}

fastq_err_t fastq_scheduler_load(fastq_scheduler_t *s)
{
    (void)s;
    return FASTQ_ERR_LICENSE;
}

fastq_err_t fastq_scheduler_start(fastq_scheduler_t *s)
{
    (void)s;
    return FASTQ_ERR_LICENSE;
}

void fastq_scheduler_stop(fastq_scheduler_t *s)
{
    (void)s;
}

void fastq_scheduler_destroy(fastq_scheduler_t *s)
{
    (void)s;
}
