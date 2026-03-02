# FastQ - API Design

## Public Header: `include/fastq.h`

This document defines the planned public C API for FastQ.

---

## Types

```c
/* Opaque types */
typedef struct fastq_redis_t   fastq_redis_t;
typedef struct fastq_queue_t   fastq_queue_t;
typedef struct fastq_worker_t  fastq_worker_t;

/* Job */
typedef struct {
    char*   id;
    char*   queue_name;
    char*   payload;        /* JSON string */
    int     priority;       /* 1=HIGH, 2=NORMAL, 3=LOW */
    int     retries;
    int     max_retries;
    time_t  created_at;
    time_t  processed_at;
    time_t  completed_at;
    char*   error;
} fastq_job_t;

/* Job priority levels */
typedef enum {
    FASTQ_PRIORITY_HIGH   = 1,
    FASTQ_PRIORITY_NORMAL = 2,
    FASTQ_PRIORITY_LOW    = 3
} fastq_priority_t;

/* Job status */
typedef enum {
    FASTQ_STATUS_QUEUED,
    FASTQ_STATUS_PROCESSING,
    FASTQ_STATUS_DONE,
    FASTQ_STATUS_FAILED,
    FASTQ_STATUS_DEAD
} fastq_status_t;

/* Queue statistics */
typedef struct {
    int pending;
    int processing;
    int done;
    int failed;
    int delayed;
} fastq_stats_t;

/* Worker callback */
typedef int (*fastq_job_handler_t)(fastq_job_t *job, void *user_data);

/* Return codes */
typedef enum {
    FASTQ_OK             =  0,
    FASTQ_ERR            = -1,
    FASTQ_ERR_REDIS      = -2,
    FASTQ_ERR_ALLOC      = -3,
    FASTQ_ERR_SERIALIZE  = -4,
    FASTQ_ERR_NOT_FOUND  = -5
} fastq_err_t;
```

---

## Redis Adapter

```c
/* Connect to Redis. Returns NULL on failure. */
fastq_redis_t *fastq_redis_connect(const char *url);

/* Disconnect and free resources. */
void fastq_redis_disconnect(fastq_redis_t *redis);

/* Ping Redis. Returns FASTQ_OK or FASTQ_ERR_REDIS. */
fastq_err_t fastq_redis_ping(fastq_redis_t *redis);
```

---

## Job

```c
/* Create a new job. Caller owns the returned pointer. */
fastq_job_t *fastq_job_create(const char *payload, fastq_priority_t priority);

/* Free a job and all its fields. */
void fastq_job_destroy(fastq_job_t *job);

/* Serialize job to JSON string. Caller must free the result. */
char *fastq_job_serialize(const fastq_job_t *job);

/* Deserialize JSON string to job. Caller owns the returned pointer. */
fastq_job_t *fastq_job_deserialize(const char *json);
```

---

## Queue

```c
/* Create a queue handle. */
fastq_queue_t *fastq_queue_create(fastq_redis_t *redis, const char *name);

/* Destroy a queue handle (does NOT delete data from Redis). */
void fastq_queue_destroy(fastq_queue_t *queue);

/* Push a job onto the queue. Writes job.id on success. */
fastq_err_t fastq_push(fastq_queue_t *queue, fastq_job_t *job);

/* Blocking pop. Blocks up to timeout_sec. Returns NULL on timeout. */
fastq_job_t *fastq_pop(fastq_queue_t *queue, int timeout_sec);

/* Mark job as completed. */
fastq_err_t fastq_job_done(fastq_queue_t *queue, fastq_job_t *job);

/* Mark job as failed. Handles retry logic internally. */
fastq_err_t fastq_job_fail(fastq_queue_t *queue, fastq_job_t *job, const char *error_msg);

/* Get queue statistics. */
fastq_err_t fastq_stats(fastq_queue_t *queue, fastq_stats_t *stats);
```

---

## Worker

```c
/* Create a worker bound to a queue. */
fastq_worker_t *fastq_worker_create(fastq_queue_t *queue,
                                     fastq_job_handler_t handler,
                                     void *user_data);

/* Start the worker loop (blocking). */
fastq_err_t fastq_worker_start(fastq_worker_t *worker);

/* Signal the worker to stop gracefully. */
void fastq_worker_stop(fastq_worker_t *worker);

/* Destroy the worker and free resources. */
void fastq_worker_destroy(fastq_worker_t *worker);
```

---

## Dead Letter Queue

```c
/* List dead jobs. Caller must free the returned array. */
fastq_job_t **fastq_dlq_list(fastq_queue_t *queue, int *count);

/* Retry a dead job (re-push to queue). */
fastq_err_t fastq_dlq_retry(fastq_queue_t *queue, const char *job_id);

/* Permanently delete a dead job. */
fastq_err_t fastq_dlq_delete(fastq_queue_t *queue, const char *job_id);
```

---

## Logging

```c
typedef enum {
    FASTQ_LOG_DEBUG,
    FASTQ_LOG_INFO,
    FASTQ_LOG_WARN,
    FASTQ_LOG_ERROR
} fastq_log_level_t;

/* Set global log level. Default: FASTQ_LOG_INFO */
void fastq_set_log_level(fastq_log_level_t level);
```
