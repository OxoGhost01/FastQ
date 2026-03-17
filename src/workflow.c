/*
 * workflow.c — stub (free / public build).
 * Pro feature. All functions are no-ops that return FASTQ_ERR_LICENSE / NULL.
 * Replace with the real implementation for a licensed build.
 */

#include "fastq.h"
#include "fastq_internal.h"

/* Internal: called by queue.c when a job completes. No-op in the free build. */
void fastq_chain_trigger(fastq_queue_t *q, redisContext *ctx, const fastq_job_t *job)
{
    (void)q; (void)ctx; (void)job;
}

fastq_err_t fastq_chain(fastq_queue_t *q, const char *parent_job_id, fastq_job_t *child_job)
{
    (void)q; (void)parent_job_id; (void)child_job;
    return FASTQ_ERR_LICENSE;
}

fastq_workflow_t *fastq_workflow_create(void)
{
    return NULL;
}

fastq_err_t fastq_workflow_add_job(fastq_workflow_t *wf, fastq_job_t *job)
{
    (void)wf; (void)job;
    return FASTQ_ERR_LICENSE;
}

fastq_err_t fastq_workflow_add_dep(fastq_workflow_t *wf, const char *before_id,
                                    const char *after_id)
{
    (void)wf; (void)before_id; (void)after_id;
    return FASTQ_ERR_LICENSE;
}

fastq_err_t fastq_workflow_submit(fastq_workflow_t *wf, fastq_queue_t *q,
                                   char *wf_id_out, size_t wf_id_size)
{
    (void)wf; (void)q; (void)wf_id_out; (void)wf_id_size;
    return FASTQ_ERR_LICENSE;
}

fastq_err_t fastq_workflow_status(fastq_queue_t *q, const char *wf_id,
                                   int *total_out, int *remaining_out)
{
    (void)q; (void)wf_id; (void)total_out; (void)remaining_out;
    return FASTQ_ERR_LICENSE;
}

void fastq_workflow_destroy(fastq_workflow_t *wf)
{
    (void)wf;
}
