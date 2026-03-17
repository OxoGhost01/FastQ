/*
 * metrics.c — stub (free / public build).
 * Pro feature. All functions are no-ops that return FASTQ_ERR_LICENSE / NULL.
 * Replace with the real implementation for a licensed build.
 */

#include "fastq.h"

fastq_metrics_t *fastq_metrics_create(fastq_queue_t *q, int port)
{
    (void)q; (void)port;
    return NULL;
}

fastq_err_t fastq_metrics_start(fastq_metrics_t *m)
{
    (void)m;
    return FASTQ_ERR_LICENSE;
}

void fastq_metrics_stop(fastq_metrics_t *m)
{
    (void)m;
}

void fastq_metrics_destroy(fastq_metrics_t *m)
{
    (void)m;
}
