#ifndef FASTQ_H
#define FASTQ_H

#include <stddef.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    FASTQ_OK             =  0,
    FASTQ_ERR            = -1,
    FASTQ_ERR_REDIS      = -2,
    FASTQ_ERR_ALLOC      = -3,
    FASTQ_ERR_SERIALIZE  = -4,
    FASTQ_ERR_NOT_FOUND  = -5,
    FASTQ_ERR_TIMEOUT    = -6,
    FASTQ_ERR_INVALID    = -7,   /* invalid argument / name / format */
    FASTQ_ERR_LICENSE    = -8    /* feature requires a valid license */
} fastq_err_t;

/* Logging */

typedef enum {
    FASTQ_LOG_DEBUG,
    FASTQ_LOG_INFO,
    FASTQ_LOG_WARN,
    FASTQ_LOG_ERROR
} fastq_log_level_t;

void fastq_set_log_level(fastq_log_level_t level);
void fastq_log(fastq_log_level_t level, const char *fmt, ...);

/* Priority / Status */

typedef enum {
    FASTQ_PRIORITY_HIGH   = 1,
    FASTQ_PRIORITY_NORMAL = 2,
    FASTQ_PRIORITY_LOW    = 3
} fastq_priority_t;

typedef enum {
    FASTQ_STATUS_QUEUED,
    FASTQ_STATUS_PROCESSING,
    FASTQ_STATUS_DONE,
    FASTQ_STATUS_FAILED,
    FASTQ_STATUS_DEAD
} fastq_status_t;

/* Job */

typedef struct {
    char *id;
    char *queue_name;
    char *payload;       /* JSON string */
    fastq_priority_t priority;
    int retries;
    int max_retries;
    time_t created_at;
    time_t processed_at;
    time_t completed_at;
    char *error;
    char *wf_id;         /* workflow ID (NULL if not in a workflow) */
} fastq_job_t;

fastq_job_t *fastq_job_create(const char *payload, fastq_priority_t priority);
void fastq_job_destroy(fastq_job_t *job);
char *fastq_job_serialize(const fastq_job_t *job);
fastq_job_t *fastq_job_deserialize(const char *json);

/* Input validation */

/* Returns FASTQ_OK if name is valid: [a-zA-Z0-9_-], 1-64 chars. */
fastq_err_t fastq_validate_name(const char *name);

/* Redis adapter */

typedef struct fastq_redis_t fastq_redis_t;

fastq_redis_t *fastq_redis_connect(const char *host, int port);
void fastq_redis_disconnect(fastq_redis_t *r);
fastq_err_t fastq_redis_ping(fastq_redis_t *r);

/* Queue */

typedef struct fastq_queue_t fastq_queue_t;

typedef struct {
    int pending;
    int processing;
    int done;
    int failed;
    int delayed;
} fastq_stats_t;

fastq_queue_t *fastq_queue_create(fastq_redis_t *redis, const char *name);
fastq_queue_t *fastq_queue_create_pooled(const char *host, int port, const char *name, int pool_size);
void fastq_queue_destroy(fastq_queue_t *q);

fastq_err_t fastq_push(fastq_queue_t *q, fastq_job_t *job);
fastq_job_t *fastq_pop(fastq_queue_t *q, int timeout_sec);
fastq_err_t fastq_job_done(fastq_queue_t *q, fastq_job_t *job);
fastq_err_t fastq_job_fail(fastq_queue_t *q, fastq_job_t *job, const char *error_msg);
fastq_err_t fastq_stats(fastq_queue_t *q, fastq_stats_t *out);

/* Schedule a one-shot delayed job (pushed to queue at run_at UTC). */
fastq_err_t fastq_schedule(fastq_queue_t *q, fastq_job_t *job, time_t run_at);

/* Dead Letter Queue */

fastq_job_t **fastq_dlq_list(fastq_queue_t *q, int *count);
fastq_err_t fastq_dlq_retry(fastq_queue_t *q, const char *job_id);
fastq_err_t fastq_dlq_delete(fastq_queue_t *q, const char *job_id);

/* Crash recovery */

int fastq_recover_orphaned_jobs(fastq_queue_t *q);

/* Worker */

typedef struct fastq_worker_t fastq_worker_t;
typedef int (*fastq_job_handler_t)(fastq_job_t *job, void *user_data);
typedef int (*fastq_batch_handler_t)(fastq_job_t **jobs, int count, void *user_data);

fastq_worker_t *fastq_worker_create(fastq_queue_t *queue, fastq_job_handler_t handler, void *user_data);
void fastq_worker_set_threads(fastq_worker_t *w, int num_threads);
fastq_err_t fastq_worker_start(fastq_worker_t *w);
void fastq_worker_stop(fastq_worker_t *w);
void fastq_worker_destroy(fastq_worker_t *w);

/* Batch mode: pop up to size jobs at once; use batch_handler instead of handler. */
fastq_err_t fastq_worker_set_batch_size(fastq_worker_t *w, int size);
fastq_err_t fastq_worker_set_batch_timeout_ms(fastq_worker_t *w, int timeout_ms);
fastq_err_t fastq_worker_set_batch_handler(fastq_worker_t *w, fastq_batch_handler_t handler);

/* Rate Limiting */

typedef struct fastq_ratelimit_t fastq_ratelimit_t;

/* capacity: max burst size; refill_per_sec: tokens added per second. */
fastq_ratelimit_t *fastq_ratelimit_create(int capacity, int refill_per_sec);
void fastq_ratelimit_destroy(fastq_ratelimit_t *rl);

/* Returns true and consumes one token if allowed, false if rate limited. */
bool fastq_ratelimit_acquire(fastq_ratelimit_t *rl);

/* Attach a rate limiter to a worker (worker calls acquire() before each pop). */
void fastq_worker_set_ratelimit(fastq_worker_t *w, fastq_ratelimit_t *rl);

/* Scheduler (Cron) */

typedef struct fastq_scheduler_t fastq_scheduler_t;

fastq_scheduler_t *fastq_scheduler_create(fastq_queue_t *q);

/* Add a cron job. id must be unique within the scheduler (1-64 chars, [a-zA-Z0-9_-]).
    cron_expr: standard 5-field cron expression ("min hour dom month dow").
    payload: job payload string (max 4095 bytes).
    The entry is also persisted to Redis so it survives restarts. */
fastq_err_t fastq_scheduler_add_cron(fastq_scheduler_t *s, const char *id, const char *cron_expr, const char *payload, fastq_priority_t priority);

/* Remove a cron job by id (also removes from Redis). */
fastq_err_t fastq_scheduler_remove(fastq_scheduler_t *s, const char *id);

/* Load persisted cron jobs from Redis (call before start after a restart). */
fastq_err_t fastq_scheduler_load(fastq_scheduler_t *s);

/* Start the scheduler background thread (non-blocking). */
fastq_err_t fastq_scheduler_start(fastq_scheduler_t *s);
void fastq_scheduler_stop(fastq_scheduler_t *s);
void fastq_scheduler_destroy(fastq_scheduler_t *s);

/* Job Chaining */

/* Register child_job to be automatically pushed when parent_job_id completes.
   child_job is serialized and stored in Redis; caller may destroy it after. */
fastq_err_t fastq_chain(fastq_queue_t *q, const char *parent_job_id, fastq_job_t *child_job);

/* Workflows (DAG) */

typedef struct fastq_workflow_t fastq_workflow_t;

fastq_workflow_t *fastq_workflow_create(void);

/* Add a job to the workflow. The job's id is assigned if NULL.
   Caller retains ownership; the job is copied internally. */
fastq_err_t fastq_workflow_add_job(fastq_workflow_t *wf, fastq_job_t *job);

/* Declare that before_id must complete before after_id runs. */
fastq_err_t fastq_workflow_add_dep(fastq_workflow_t *wf, const char *before_id, const char *after_id);

/* Submit the workflow to the queue. Root jobs (no deps) are pushed immediately.
   wf_id_out: caller-provided buffer (at least 32 bytes) filled with the workflow ID. */
fastq_err_t fastq_workflow_submit(fastq_workflow_t *wf, fastq_queue_t *q, char *wf_id_out, size_t wf_id_size);

/* Query workflow progress: *total_out = total jobs, *remaining_out = pending. */
fastq_err_t fastq_workflow_status(fastq_queue_t *q, const char *wf_id, int *total_out, int *remaining_out);

void fastq_workflow_destroy(fastq_workflow_t *wf);

/* Metrics */

typedef struct fastq_metrics_t fastq_metrics_t;

/* Create a metrics server. port: TCP port to listen on (e.g. 9090).
   Exposes GET /metrics (Prometheus) and GET /health (JSON). */
fastq_metrics_t *fastq_metrics_create(fastq_queue_t *q, int port);
fastq_err_t fastq_metrics_start(fastq_metrics_t *m);
void fastq_metrics_stop(fastq_metrics_t *m);
void fastq_metrics_destroy(fastq_metrics_t *m);

/* License */

/* Set the license key. Returns FASTQ_OK if the key is valid, FASTQ_ERR_INVALID
    if the format is wrong, or FASTQ_ERR_LICENSE if the HMAC doesn't verify.
    Key format: "{email}:{timestamp_hex}:{hmac_sha256_hex}" */
fastq_err_t fastq_license_set(const char *license_key);

/* Returns true if a valid license has been set in this process. */
bool fastq_license_valid(void);

/* Returns the email embedded in the license, or NULL if no valid license. */
const char *fastq_license_owner(void);

/* Daemon */

fastq_err_t fastq_daemonize(const char *pid_file, const char *log_file);

#ifdef __cplusplus
}
#endif

#endif /* FASTQ_H */
