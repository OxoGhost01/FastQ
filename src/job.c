#include "fastq.h"
#include "fastq_internal.h"
#include <json-c/json.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

static _Atomic unsigned long g_id_seq = 0;

char *fastq_generate_id(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    unsigned long ms  = (unsigned long)ts.tv_sec * 1000 +
                        (unsigned long)ts.tv_nsec / 1000000;
    unsigned long seq = atomic_fetch_add_explicit(&g_id_seq, 1,
                                                  memory_order_relaxed);
    char *id = malloc(32);
    if (id) snprintf(id, 32, "%lx-%lx", ms, seq);
    return id;
}

fastq_job_t *fastq_job_create(const char *payload, fastq_priority_t priority)
{
    if (!payload) return NULL;

    fastq_job_t *job = calloc(1, sizeof(*job));
    if (!job) return NULL;

    job->id          = fastq_generate_id();
    job->payload     = strdup(payload);
    job->priority    = priority;
    job->max_retries = 5;
    job->created_at  = time(NULL);

    if (!job->id || !job->payload) {
        fastq_job_destroy(job);
        return NULL;
    }
    return job;
}

void fastq_job_destroy(fastq_job_t *job)
{
    if (!job) return;
    free(job->id);
    free(job->queue_name);
    free(job->payload);
    free(job->error);
    free(job);
}

char *fastq_job_serialize(const fastq_job_t *job)
{
    if (!job) return NULL;

    json_object *obj = json_object_new_object();
    if (!obj) return NULL;

    json_object_object_add(obj, "id",          json_object_new_string(job->id));
    json_object_object_add(obj, "payload",     json_object_new_string(job->payload));
    json_object_object_add(obj, "priority",    json_object_new_int(job->priority));
    json_object_object_add(obj, "retries",     json_object_new_int(job->retries));
    json_object_object_add(obj, "max_retries", json_object_new_int(job->max_retries));
    json_object_object_add(obj, "created_at",  json_object_new_int64(job->created_at));

    if (job->queue_name)
        json_object_object_add(obj, "queue",
            json_object_new_string(job->queue_name));
    if (job->error)
        json_object_object_add(obj, "error",
            json_object_new_string(job->error));
    if (job->processed_at)
        json_object_object_add(obj, "processed_at",
            json_object_new_int64(job->processed_at));
    if (job->completed_at)
        json_object_object_add(obj, "completed_at",
            json_object_new_int64(job->completed_at));

    const char *s = json_object_to_json_string(obj);
    char *result = strdup(s);
    json_object_put(obj);
    return result;
}

static char *jstr(json_object *obj, const char *key)
{
    json_object *v;
    if (!json_object_object_get_ex(obj, key, &v)) return NULL;
    const char *s = json_object_get_string(v);
    return s ? strdup(s) : NULL;
}

static int jint(json_object *obj, const char *key, int def)
{
    json_object *v;
    return json_object_object_get_ex(obj, key, &v) ?
           json_object_get_int(v) : def;
}

static time_t jtime(json_object *obj, const char *key)
{
    json_object *v;
    return json_object_object_get_ex(obj, key, &v) ?
           (time_t)json_object_get_int64(v) : 0;
}

fastq_job_t *fastq_job_deserialize(const char *json_str)
{
    if (!json_str) return NULL;

    json_object *obj = json_tokener_parse(json_str);
    if (!obj) {
        fastq_log(FASTQ_LOG_ERROR, "job: failed to parse JSON");
        return NULL;
    }

    fastq_job_t *job = calloc(1, sizeof(*job));
    if (!job) { json_object_put(obj); return NULL; }

    job->id           = jstr(obj, "id");
    job->payload      = jstr(obj, "payload");
    job->queue_name   = jstr(obj, "queue");
    job->error        = jstr(obj, "error");
    job->priority     = jint(obj, "priority",    FASTQ_PRIORITY_NORMAL);
    job->retries      = jint(obj, "retries",     0);
    job->max_retries  = jint(obj, "max_retries", 5);
    job->created_at   = jtime(obj, "created_at");
    job->processed_at = jtime(obj, "processed_at");
    job->completed_at = jtime(obj, "completed_at");

    json_object_put(obj);

    if (!job->id || !job->payload) {
        fastq_log(FASTQ_LOG_ERROR, "job: missing required fields");
        fastq_job_destroy(job);
        return NULL;
    }
    return job;
}
