#ifndef FASTQ_H
#define FASTQ_H

#include <stddef.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Error codes ────────────────────────────────────────────────── */

typedef enum {
    FASTQ_OK             =  0,
    FASTQ_ERR            = -1,
    FASTQ_ERR_REDIS      = -2,
    FASTQ_ERR_ALLOC      = -3,
    FASTQ_ERR_SERIALIZE  = -4,
    FASTQ_ERR_NOT_FOUND  = -5,
    FASTQ_ERR_TIMEOUT    = -6
} fastq_err_t;

/* ── Logging ────────────────────────────────────────────────────── */

typedef enum {
    FASTQ_LOG_DEBUG,
    FASTQ_LOG_INFO,
    FASTQ_LOG_WARN,
    FASTQ_LOG_ERROR
} fastq_log_level_t;

void fastq_set_log_level(fastq_log_level_t level);
void fastq_log(fastq_log_level_t level, const char *fmt, ...);

/* ── Priority / Status ──────────────────────────────────────────── */

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

/* ── Job ────────────────────────────────────────────────────────── */

typedef struct {
    char            *id;
    char            *queue_name;
    char            *payload;       /* JSON string */
    fastq_priority_t priority;
    int              retries;
    int              max_retries;
    time_t           created_at;
    time_t           processed_at;
    time_t           completed_at;
    char            *error;
} fastq_job_t;

fastq_job_t *fastq_job_create(const char *payload, fastq_priority_t priority);
void         fastq_job_destroy(fastq_job_t *job);
char        *fastq_job_serialize(const fastq_job_t *job);
fastq_job_t *fastq_job_deserialize(const char *json);

/* ── Redis adapter ──────────────────────────────────────────────── */

typedef struct fastq_redis_t fastq_redis_t;

fastq_redis_t *fastq_redis_connect(const char *host, int port);
void           fastq_redis_disconnect(fastq_redis_t *r);
fastq_err_t    fastq_redis_ping(fastq_redis_t *r);

/* ── Queue ──────────────────────────────────────────────────────── */

typedef struct fastq_queue_t fastq_queue_t;

typedef struct {
    int pending;
    int processing;
    int done;
    int failed;
    int delayed;
} fastq_stats_t;

/* pool_size: number of Redis connections for the worker pool.
   Pass 0 to disable pooling (single-connection mode). */
fastq_queue_t *fastq_queue_create(fastq_redis_t *redis, const char *name);
fastq_queue_t *fastq_queue_create_pooled(const char *host, int port,
                                          const char *name, int pool_size);
void           fastq_queue_destroy(fastq_queue_t *q);

fastq_err_t    fastq_push(fastq_queue_t *q, fastq_job_t *job);
fastq_job_t   *fastq_pop(fastq_queue_t *q, int timeout_sec);
fastq_err_t    fastq_job_done(fastq_queue_t *q, fastq_job_t *job);
fastq_err_t    fastq_job_fail(fastq_queue_t *q, fastq_job_t *job,
                              const char *error_msg);
fastq_err_t    fastq_stats(fastq_queue_t *q, fastq_stats_t *out);

/* ── Dead Letter Queue ──────────────────────────────────────────── */

fastq_job_t  **fastq_dlq_list(fastq_queue_t *q, int *count);
fastq_err_t    fastq_dlq_retry(fastq_queue_t *q, const char *job_id);
fastq_err_t    fastq_dlq_delete(fastq_queue_t *q, const char *job_id);

/* ── Crash recovery ─────────────────────────────────────────────── */

/* Re-queue orphaned jobs stuck in "processing" state. */
int fastq_recover_orphaned_jobs(fastq_queue_t *q);

/* ── Worker ─────────────────────────────────────────────────────── */

typedef struct fastq_worker_t fastq_worker_t;
typedef int (*fastq_job_handler_t)(fastq_job_t *job, void *user_data);

/* num_threads: number of worker threads. Pass 1 for single-threaded. */
fastq_worker_t *fastq_worker_create(fastq_queue_t *queue,
                                     fastq_job_handler_t handler,
                                     void *user_data);
void            fastq_worker_set_threads(fastq_worker_t *w, int num_threads);
fastq_err_t     fastq_worker_start(fastq_worker_t *w);
void            fastq_worker_stop(fastq_worker_t *w);
void            fastq_worker_destroy(fastq_worker_t *w);

/* ── Daemon ─────────────────────────────────────────────────────── */

/* Daemonize the current process. Returns FASTQ_OK in child, FASTQ_ERR on failure.
   pid_file: path to write PID (can be NULL to skip).
   log_file: path to redirect stdout/stderr (can be NULL for /dev/null). */
fastq_err_t fastq_daemonize(const char *pid_file, const char *log_file);

#ifdef __cplusplus
}
#endif

#endif /* FASTQ_H */
