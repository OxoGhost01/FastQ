#include "fastq.h"
#include "fastq_internal.h"
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <pthread.h>

#define WORKER_POP_TIMEOUT 2

struct fastq_worker_t {
    fastq_queue_t           *queue;
    fastq_job_handler_t      handler;
    void                    *user_data;
    volatile sig_atomic_t    running;
    int                      num_threads;
    pthread_t               *threads;
};

typedef struct {
    fastq_worker_t *worker;
    int             thread_id;
} thread_arg_t;

/* Uniform function-pointer types so the hot loop has no per-job branches. */
typedef fastq_job_t *(*pop_fn_t) (fastq_queue_t *, redisContext *, int);
typedef fastq_err_t  (*done_fn_t)(fastq_queue_t *, redisContext *, fastq_job_t *);
typedef fastq_err_t  (*fail_fn_t)(fastq_queue_t *, redisContext *, fastq_job_t *, const char *);

static fastq_job_t *pop_noctx(fastq_queue_t *q, redisContext *ctx, int t)
{
    (void)ctx;
    return fastq_pop(q, t);
}

static fastq_err_t done_noctx(fastq_queue_t *q, redisContext *ctx, fastq_job_t *job)
{
    (void)ctx;
    return fastq_job_done(q, job);
}

static fastq_err_t fail_noctx(fastq_queue_t *q, redisContext *ctx,
                               fastq_job_t *job, const char *err)
{
    (void)ctx;
    return fastq_job_fail(q, job, err);
}

static void *worker_thread(void *arg)
{
    thread_arg_t *ta = arg;
    fastq_worker_t *w = ta->worker;
    int tid = ta->thread_id;
    free(ta);

    fastq_pool_t *pool = fastq_queue_get_pool(w->queue);
    redisContext *ctx = NULL;

    pop_fn_t  pop_fn;
    done_fn_t done_fn;
    fail_fn_t fail_fn;

    if (pool) {
        ctx = fastq_pool_acquire(pool);
        if (!ctx) {
            fastq_log(FASTQ_LOG_ERROR, "worker[%d]: failed to acquire conn", tid);
            return NULL;
        }
        pop_fn  = fastq_pop_with_ctx;
        done_fn = fastq_job_done_with_ctx;
        fail_fn = fastq_job_fail_with_ctx;
    } else {
        pop_fn  = pop_noctx;
        done_fn = done_noctx;
        fail_fn = fail_noctx;
    }

    fastq_log(FASTQ_LOG_DEBUG, "worker[%d]: started", tid);

    while (w->running) {
        fastq_job_t *job = pop_fn(w->queue, ctx, WORKER_POP_TIMEOUT);
        if (!job) continue;

        fastq_log(FASTQ_LOG_DEBUG, "worker[%d]: processing job %s", tid, job->id);

        int rc = w->handler(job, w->user_data);
        if (rc == 0) {
            done_fn(w->queue, ctx, job);
        } else {
            char err[64];
            snprintf(err, sizeof(err), "handler returned %d", rc);
            fail_fn(w->queue, ctx, job, err);
        }

        fastq_job_destroy(job);
    }

    if (pool && ctx)
        fastq_pool_release(pool, ctx);

    fastq_log(FASTQ_LOG_DEBUG, "worker[%d]: stopped", tid);
    return NULL;
}

fastq_worker_t *fastq_worker_create(fastq_queue_t *queue,
                                     fastq_job_handler_t handler,
                                     void *user_data)
{
    if (!queue || !handler) return NULL;

    fastq_worker_t *w = calloc(1, sizeof(*w));
    if (!w) return NULL;

    w->queue       = queue;
    w->handler     = handler;
    w->user_data   = user_data;
    w->num_threads = 1;

    fastq_log(FASTQ_LOG_DEBUG, "worker: created");
    return w;
}

void fastq_worker_set_threads(fastq_worker_t *w, int num_threads)
{
    if (!w || num_threads < 1) return;
    w->num_threads = num_threads;
}

fastq_err_t fastq_worker_start(fastq_worker_t *w)
{
    if (!w) return FASTQ_ERR;
    w->running = 1;

    int n = w->num_threads;
    w->threads = calloc((size_t)n, sizeof(pthread_t));
    if (!w->threads) return FASTQ_ERR_ALLOC;

    fastq_log(FASTQ_LOG_INFO, "worker: starting %d thread(s)", n);

    for (int i = 0; i < n; i++) {
        thread_arg_t *ta = malloc(sizeof(*ta));
        if (!ta) { w->running = 0; return FASTQ_ERR_ALLOC; }
        ta->worker    = w;
        ta->thread_id = i;

        if (pthread_create(&w->threads[i], NULL, worker_thread, ta) != 0) {
            free(ta);
            fastq_log(FASTQ_LOG_ERROR, "worker: failed to create thread %d", i);
            w->running = 0;
            for (int j = 0; j < i; j++)
                pthread_join(w->threads[j], NULL);
            free(w->threads);
            w->threads = NULL;
            return FASTQ_ERR;
        }
    }

    for (int i = 0; i < n; i++)
        pthread_join(w->threads[i], NULL);

    free(w->threads);
    w->threads = NULL;

    fastq_log(FASTQ_LOG_INFO, "worker: all %d thread(s) stopped", n);
    return FASTQ_OK;
}

void fastq_worker_stop(fastq_worker_t *w)
{
    if (!w) return;
    w->running = 0;
    fastq_log(FASTQ_LOG_INFO, "worker: stop requested");
}

void fastq_worker_destroy(fastq_worker_t *w)
{
    free(w);
}
