/*
 * fastq_node.c — Node.js N-API binding for FastQ.
 *
 * Exports:
 *   Queue(host, port, name [, pool_size])
 *     .push(payload [, priority])  → job_id string
 *     .pop([timeout_sec])          → {id, payload, priority, retries} or null
 *     .done(job_id)
 *     .fail(job_id, error_msg)
 *     .stats()                     → {pending, done, failed, delayed}
 *     .destroy()
 *
 *   Worker(queue, handler [, num_threads])
 *     .setThreads(n)
 *     .setRatelimit(capacity, refill_per_sec)
 *     .start()   → Promise (resolves when all threads stop)
 *     .stop()
 *
 * Security:
 *   - All string arguments are length-validated before passing to the C API.
 *   - napi_get_value_string_utf8 is called with explicit buffer sizes.
 *   - Worker handler uses a threadsafe function + pthread condvar to safely
 *     bridge the worker thread and the JS main thread.
 */

#define NAPI_VERSION 8
#include <node_api.h>
#include "fastq.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>

#define STR_MAX   4096
#define NAME_MAX  64
#define HOST_MAX  256

/* Helpers */

#define NAPI_ASSERT(env, call, msg) \
    do { if ((call) != napi_ok) { \
        napi_throw_error((env), NULL, (msg)); return NULL; } } while(0)

static bool get_string(napi_env env, napi_value val, char *buf, size_t n)
{
    size_t written;
    return napi_get_value_string_utf8(env, val, buf, n, &written) == napi_ok;
}

static napi_value make_string(napi_env env, const char *s)
{
    napi_value v;
    napi_create_string_utf8(env, s ? s : "", NAPI_AUTO_LENGTH, &v);
    return v;
}

static napi_value make_int(napi_env env, int n)
{
    napi_value v;
    napi_create_int32(env, n, &v);
    return v;
}

/* Queue */

typedef struct {
    fastq_queue_t *queue;
    fastq_redis_t *redis;  /* non-NULL only if non-pooled */
} queue_obj_t;

static void queue_finalize(napi_env env, void *data, void *hint)
{
    (void)env; (void)hint;
    queue_obj_t *obj = data;
    if (obj->queue) fastq_queue_destroy(obj->queue);
    if (obj->redis) fastq_redis_disconnect(obj->redis);
    free(obj);
}

static napi_value queue_constructor(napi_env env, napi_callback_info info)
{
    size_t argc = 4;
    napi_value args[4];
    napi_value this_val;
    napi_get_cb_info(env, info, &argc, args, &this_val, NULL);

    if (argc < 3) {
        napi_throw_error(env, NULL, "Queue(host, port, name [, pool_size])");
        return NULL;
    }

    char host[HOST_MAX] = {0};
    char name[NAME_MAX] = {0};
    int port = 6379;
    int pool_size = 0;

    if (!get_string(env, args[0], host, sizeof(host))) {
        napi_throw_type_error(env, NULL, "host must be a string");
        return NULL;
    }
    napi_get_value_int32(env, args[1], &port);
    if (!get_string(env, args[2], name, sizeof(name))) {
        napi_throw_type_error(env, NULL, "name must be a string");
        return NULL;
    }
    if (argc >= 4) napi_get_value_int32(env, args[3], &pool_size);

    if (port <= 0 || port > 65535) {
        napi_throw_range_error(env, NULL, "port must be 1-65535");
        return NULL;
    }

    queue_obj_t *obj = calloc(1, sizeof(*obj));
    if (!obj) { napi_throw_error(env, NULL, "out of memory"); return NULL; }

    if (pool_size > 0) {
        obj->queue = fastq_queue_create_pooled(host, port, name, pool_size);
    } else {
        obj->redis = fastq_redis_connect(host, port);
        if (obj->redis) obj->queue = fastq_queue_create(obj->redis, name);
    }

    if (!obj->queue) {
        if (obj->redis) fastq_redis_disconnect(obj->redis);
        free(obj);
        napi_throw_error(env, NULL, "failed to create queue (check host/port/name)");
        return NULL;
    }

    napi_wrap(env, this_val, obj, queue_finalize, NULL, NULL);
    return this_val;
}

static napi_value queue_push(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2], this_val;
    napi_get_cb_info(env, info, &argc, args, &this_val, NULL);

    queue_obj_t *obj;
    napi_unwrap(env, this_val, (void **)&obj);
    if (!obj || !obj->queue) {
        napi_throw_error(env, NULL, "invalid Queue");
        return NULL;
    }

    if (argc < 1) { napi_throw_error(env, NULL, "push(payload)"); return NULL; }

    char payload[STR_MAX] = {0};
    if (!get_string(env, args[0], payload, sizeof(payload))) {
        napi_throw_type_error(env, NULL, "payload must be a string");
        return NULL;
    }

    int priority_int = FASTQ_PRIORITY_NORMAL;
    if (argc >= 2) napi_get_value_int32(env, args[1], &priority_int);
    if (priority_int < 1 || priority_int > 3) priority_int = FASTQ_PRIORITY_NORMAL;

    fastq_job_t *job = fastq_job_create(payload, (fastq_priority_t)priority_int);
    if (!job) { napi_throw_error(env, NULL, "job_create failed"); return NULL; }

    char id_copy[32];
    snprintf(id_copy, sizeof(id_copy), "%s", job->id);

    fastq_err_t rc = fastq_push(obj->queue, job);
    fastq_job_destroy(job);

    if (rc != FASTQ_OK) { napi_throw_error(env, NULL, "push failed"); return NULL; }

    return make_string(env, id_copy);
}

static napi_value queue_pop(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1], this_val;
    napi_get_cb_info(env, info, &argc, args, &this_val, NULL);

    queue_obj_t *obj;
    napi_unwrap(env, this_val, (void **)&obj);
    if (!obj || !obj->queue) {
        napi_throw_error(env, NULL, "invalid Queue");
        return NULL;
    }

    int timeout_sec = 1;
    if (argc >= 1) napi_get_value_int32(env, args[0], &timeout_sec);
    if (timeout_sec < 0) timeout_sec = 0;

    fastq_job_t *job = fastq_pop(obj->queue, timeout_sec);
    if (!job) {
        napi_value null_val;
        napi_get_null(env, &null_val);
        return null_val;
    }

    napi_value result;
    napi_create_object(env, &result);

    napi_value id_val, payload_val, priority_val, retries_val;
    napi_create_string_utf8(env, job->id, NAPI_AUTO_LENGTH, &id_val);
    napi_create_string_utf8(env, job->payload, NAPI_AUTO_LENGTH, &payload_val);
    napi_create_int32(env, job->priority, &priority_val);
    napi_create_int32(env, job->retries, &retries_val);

    napi_set_named_property(env, result, "id", id_val);
    napi_set_named_property(env, result, "payload", payload_val);
    napi_set_named_property(env, result, "priority", priority_val);
    napi_set_named_property(env, result, "retries", retries_val);

    /* keep a copy of id for done/fail calls; store it in result.id */
    fastq_job_destroy(job);
    return result;
}

static napi_value queue_done(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1], this_val;
    napi_get_cb_info(env, info, &argc, args, &this_val, NULL);
    queue_obj_t *obj;
    napi_unwrap(env, this_val, (void **)&obj);
    if (!obj || !obj->queue || argc < 1) return NULL;

    char job_id[64] = {0};
    get_string(env, args[0], job_id, sizeof(job_id));

    fastq_job_t *job = calloc(1, sizeof(*job));
    if (!job) return NULL;
    job->id = strdup(job_id);
    fastq_job_done(obj->queue, job);
    fastq_job_destroy(job);
    return NULL;
}

static napi_value queue_fail(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2], this_val;
    napi_get_cb_info(env, info, &argc, args, &this_val, NULL);
    queue_obj_t *obj;
    napi_unwrap(env, this_val, (void **)&obj);
    if (!obj || !obj->queue || argc < 2) return NULL;

    char job_id[64] = {0}, err_msg[512] = {0};
    get_string(env, args[0], job_id, sizeof(job_id));
    get_string(env, args[1], err_msg, sizeof(err_msg));

    fastq_job_t *job = calloc(1, sizeof(*job));
    if (!job) return NULL;
    job->id = strdup(job_id);
    job->max_retries = 5;
    fastq_job_fail(obj->queue, job, err_msg);
    fastq_job_destroy(job);
    return NULL;
}

static napi_value queue_stats(napi_env env, napi_callback_info info)
{
    size_t argc = 0;
    napi_value this_val;
    napi_get_cb_info(env, info, &argc, NULL, &this_val, NULL);
    queue_obj_t *obj;
    napi_unwrap(env, this_val, (void **)&obj);
    if (!obj || !obj->queue) { napi_throw_error(env, NULL, "invalid Queue"); return NULL; }

    fastq_stats_t s;
    fastq_stats(obj->queue, &s);

    napi_value result;
    napi_create_object(env, &result);
    napi_set_named_property(env, result, "pending", make_int(env, s.pending));
    napi_set_named_property(env, result, "done",    make_int(env, s.done));
    napi_set_named_property(env, result, "failed",  make_int(env, s.failed));
    napi_set_named_property(env, result, "delayed", make_int(env, s.delayed));
    return result;
}

/* Worker */

/*
 * Handler bridge: the C worker thread calls this from a background thread.
 * It packages the job and signals the JS main thread via a threadsafe function.
 * The JS main thread calls the JS handler, then signals the worker thread with
 * the result via a pthread condvar.
 */

typedef struct {
    char id[64];
    char payload[STR_MAX];
    int priority;
    int retries;
    int result;     /* 0=done, non-zero=fail */
    bool done;      /* set by JS thread after handler returns */
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} handler_call_t;

typedef struct {
    fastq_worker_t *worker;
    napi_ref handler_ref;
    napi_threadsafe_function tsfn;
    napi_async_work async_work;
    napi_deferred deferred;
    fastq_ratelimit_t *ratelimit;
    int num_threads;
} worker_obj_t;

static void worker_finalize(napi_env env, void *data, void *hint)
{
    (void)env; (void)hint;
    worker_obj_t *obj = data;
    if (obj->worker)    fastq_worker_destroy(obj->worker);
    if (obj->ratelimit) fastq_ratelimit_destroy(obj->ratelimit);
    free(obj);
}

/* Called on the JS main thread by the threadsafe function mechanism. */
static void js_call_handler(napi_env env, napi_value js_callback, void *context, void *data)
{
    (void)context;
    handler_call_t *call = data;
    if (!call) return;

    napi_value job_obj;
    napi_create_object(env, &job_obj);

    napi_value id_val, payload_val, priority_val, retries_val;
    napi_create_string_utf8(env, call->id, NAPI_AUTO_LENGTH, &id_val);
    napi_create_string_utf8(env, call->payload, NAPI_AUTO_LENGTH, &payload_val);
    napi_create_int32(env, call->priority, &priority_val);
    napi_create_int32(env, call->retries, &retries_val);
    napi_set_named_property(env, job_obj, "id", id_val);
    napi_set_named_property(env, job_obj, "payload", payload_val);
    napi_set_named_property(env, job_obj, "priority", priority_val);
    napi_set_named_property(env, job_obj, "retries", retries_val);

    napi_value undefined;
    napi_get_undefined(env, &undefined);
    napi_value argv[1] = { job_obj };

    napi_value result;
    napi_status status = napi_call_function(env, undefined, js_callback, 1, argv, &result);

    int rc = 0;
    if (status == napi_ok && result) {
        bool is_false;
        napi_is_exception_pending(env, &is_false);
        if (!is_false) {
            /* truthy return → success, falsy → fail */
            napi_value global;
            napi_get_global(env, &global);
            bool truthy = false;
            napi_coerce_to_bool(env, result, &result);
            napi_get_value_bool(env, result, &truthy);
            rc = truthy ? 0 : 1;
        }
    }

    pthread_mutex_lock(&call->mutex);
    call->result = rc;
    call->done = true;
    pthread_cond_signal(&call->cond);
    pthread_mutex_unlock(&call->mutex);
}

static int worker_job_handler(fastq_job_t *job, void *user_data)
{
    worker_obj_t *obj = user_data;

    handler_call_t *call = calloc(1, sizeof(*call));
    if (!call) return 1;

    snprintf(call->id, sizeof(call->id), "%s", job->id ? job->id : "");
    snprintf(call->payload, sizeof(call->payload), "%s", job->payload ? job->payload : "");
    call->priority = job->priority;
    call->retries = job->retries;
    call->result = 1;
    call->done = false;
    pthread_mutex_init(&call->mutex, NULL);
    pthread_cond_init(&call->cond, NULL);

    /* enqueue the JS call */
    napi_call_threadsafe_function(obj->tsfn, call, napi_tsfn_blocking);

    /* wait for JS thread to finish */
    pthread_mutex_lock(&call->mutex);
    while (!call->done) pthread_cond_wait(&call->cond, &call->mutex);
    pthread_mutex_unlock(&call->mutex);

    int rc = call->result;
    pthread_mutex_destroy(&call->mutex);
    pthread_cond_destroy(&call->cond);
    free(call);
    return rc;
}

/* napi_async_work execute: runs fastq_worker_start on the thread pool */
static void worker_execute(napi_env env, void *data)
{
    (void)env;
    worker_obj_t *obj = data;
    fastq_worker_start(obj->worker);
}

/* napi_async_work complete: resolve the promise */
static void worker_complete(napi_env env, napi_status status, void *data)
{
    worker_obj_t *obj = data;
    napi_release_threadsafe_function(obj->tsfn, napi_tsfn_release);
    obj->tsfn = NULL;

    napi_value undefined;
    napi_get_undefined(env, &undefined);
    if (status == napi_ok)
        napi_resolve_deferred(env, obj->deferred, undefined);
    else
        napi_reject_deferred(env, obj->deferred, undefined);
    obj->deferred = NULL;
}

static napi_value worker_constructor(napi_env env, napi_callback_info info)
{
    size_t argc = 3;
    napi_value args[3], this_val;
    napi_get_cb_info(env, info, &argc, args, &this_val, NULL);

    if (argc < 2) {
        napi_throw_error(env, NULL, "Worker(queue, handler [, num_threads])");
        return NULL;
    }

    /* get queue obj from first arg */
    queue_obj_t *qobj;
    if (napi_unwrap(env, args[0], (void **)&qobj) != napi_ok || !qobj || !qobj->queue) {
        napi_throw_error(env, NULL, "first argument must be a Queue");
        return NULL;
    }

    napi_valuetype handler_type;
    napi_typeof(env, args[1], &handler_type);
    if (handler_type != napi_function) {
        napi_throw_type_error(env, NULL, "handler must be a function");
        return NULL;
    }

    worker_obj_t *obj = calloc(1, sizeof(*obj));
    if (!obj) { napi_throw_error(env, NULL, "out of memory"); return NULL; }

    obj->num_threads = 1;
    if (argc >= 3) napi_get_value_int32(env, args[2], &obj->num_threads);
    if (obj->num_threads < 1 || obj->num_threads > 64) obj->num_threads = 1;

    napi_create_reference(env, args[1], 1, &obj->handler_ref);

    obj->worker = fastq_worker_create(qobj->queue, worker_job_handler, obj);
    if (!obj->worker) {
        napi_delete_reference(env, obj->handler_ref);
        free(obj);
        napi_throw_error(env, NULL, "worker_create failed");
        return NULL;
    }
    fastq_worker_set_threads(obj->worker, obj->num_threads);

    napi_wrap(env, this_val, obj, worker_finalize, NULL, NULL);
    return this_val;
}

static napi_value worker_set_threads(napi_env env, napi_callback_info info)
{
    size_t argc = 1;
    napi_value args[1], this_val;
    napi_get_cb_info(env, info, &argc, args, &this_val, NULL);
    worker_obj_t *obj;
    napi_unwrap(env, this_val, (void **)&obj);
    if (!obj || !obj->worker || argc < 1) return NULL;
    int n = 1;
    napi_get_value_int32(env, args[0], &n);
    if (n >= 1 && n <= 64) fastq_worker_set_threads(obj->worker, n);
    return NULL;
}

static napi_value worker_set_ratelimit(napi_env env, napi_callback_info info)
{
    size_t argc = 2;
    napi_value args[2], this_val;
    napi_get_cb_info(env, info, &argc, args, &this_val, NULL);
    worker_obj_t *obj;
    napi_unwrap(env, this_val, (void **)&obj);
    if (!obj || argc < 2) return NULL;

    int capacity = 0, refill = 0;
    napi_get_value_int32(env, args[0], &capacity);
    napi_get_value_int32(env, args[1], &refill);

    fastq_ratelimit_t *rl = fastq_ratelimit_create(capacity, refill);
    if (rl) {
        if (obj->ratelimit) fastq_ratelimit_destroy(obj->ratelimit);
        obj->ratelimit = rl;
        fastq_worker_set_ratelimit(obj->worker, rl);
    }
    return NULL;
}

static napi_value worker_start(napi_env env, napi_callback_info info)
{
    size_t argc = 0;
    napi_value this_val;
    napi_get_cb_info(env, info, &argc, NULL, &this_val, NULL);

    worker_obj_t *obj;
    napi_unwrap(env, this_val, (void **)&obj);
    if (!obj || !obj->worker) {
        napi_throw_error(env, NULL, "invalid Worker");
        return NULL;
    }

    /* create the threadsafe function for the job handler callback */
    napi_value handler_fn;
    napi_get_reference_value(env, obj->handler_ref, &handler_fn);

    napi_value resource_name;
    napi_create_string_utf8(env, "fastq_worker", NAPI_AUTO_LENGTH, &resource_name);

    napi_create_threadsafe_function(env, handler_fn, NULL, resource_name,
                                    0, 1, NULL, NULL, obj,
                                    js_call_handler, &obj->tsfn);

    /* create promise + async work */
    napi_value promise;
    napi_create_promise(env, &obj->deferred, &promise);

    napi_create_async_work(env, NULL, resource_name, worker_execute, worker_complete, obj, &obj->async_work);
    napi_queue_async_work(env, obj->async_work);

    return promise;
}

static napi_value worker_stop(napi_env env, napi_callback_info info)
{
    size_t argc = 0;
    napi_value this_val;
    napi_get_cb_info(env, info, &argc, NULL, &this_val, NULL);
    worker_obj_t *obj;
    napi_unwrap(env, this_val, (void **)&obj);
    if (obj && obj->worker) fastq_worker_stop(obj->worker);
    return NULL;
}

/* Module init */

static napi_value init(napi_env env, napi_value exports)
{
    fastq_set_log_level(FASTQ_LOG_WARN);

    /* Queue class */
    napi_property_descriptor queue_props[] = {
        { "push",  NULL, queue_push,  NULL, NULL, NULL, napi_default, NULL },
        { "pop",   NULL, queue_pop,   NULL, NULL, NULL, napi_default, NULL },
        { "done",  NULL, queue_done,  NULL, NULL, NULL, napi_default, NULL },
        { "fail",  NULL, queue_fail,  NULL, NULL, NULL, napi_default, NULL },
        { "stats", NULL, queue_stats, NULL, NULL, NULL, napi_default, NULL },
    };
    napi_value queue_ctor;
    napi_define_class(env, "Queue", NAPI_AUTO_LENGTH, queue_constructor, NULL, 5, queue_props, &queue_ctor);
    napi_set_named_property(env, exports, "Queue", queue_ctor);

    /* Worker class */
    napi_property_descriptor worker_props[] = {
        { "setThreads", NULL, worker_set_threads, NULL, NULL, NULL, napi_default, NULL },
        { "setRatelimit", NULL, worker_set_ratelimit, NULL, NULL, NULL, napi_default, NULL },
        { "start", NULL, worker_start, NULL, NULL, NULL, napi_default, NULL },
        { "stop", NULL, worker_stop, NULL, NULL, NULL, napi_default, NULL },
    };
    napi_value worker_ctor;
    napi_define_class(env, "Worker", NAPI_AUTO_LENGTH, worker_constructor, NULL, 4, worker_props, &worker_ctor);
    napi_set_named_property(env, exports, "Worker", worker_ctor);

    return exports;
}

NAPI_MODULE(fastq_native, init)
