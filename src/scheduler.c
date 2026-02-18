/*
 * scheduler.c — Cron-based job scheduler with Redis persistence.
 *
 * Cron expression format: "minute hour dom month dow"
 *   minute  : 0-59
 *   hour    : 0-23
 *   dom     : 1-31  (day of month)
 *   month   : 1-12
 *   dow     : 0-6   (0=Sun)
 *
 * Each field supports: *, N, N-M, N/step, N,M,...
 *
 * Security: all input strings are bounds-checked; bitmask arithmetic uses
 * only pre-validated values; no dynamic code execution on user input.
 */

#include "fastq.h"
#include "fastq_internal.h"
#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <signal.h>
#include <time.h>
#include <ctype.h>
#include <errno.h>

#define SCHED_MAX_ENTRIES  256
#define CRON_EXPR_MAX      128
#define PAYLOAD_MAX        4096
#define ID_MAX             64
#define KEY_MAX            256

/* Cron expression */

typedef struct {
    uint64_t minutes;   /* bits 0-59  */
    uint32_t hours;     /* bits 0-23  */
    uint32_t dom;       /* bits 1-31  */
    uint16_t months;    /* bits 1-12  */
    uint8_t dow;        /* bits 0-6   */
    bool dom_star;
    bool dow_star;
} cron_expr_t;

/*
 * Parse a single cron field into a bitmask.
 * Accepts: *, N, N-M, N/step, N-M/step, comma-separated combinations.
 * Returns 0 on success, -1 on invalid input.
 *
 * Security: field string is validated character-by-character before any
 * atoi call; range [lo,hi] enforced on every parsed value.
 */
static int parse_field(const char *field, uint64_t *bits, int lo, int hi)
{
    if (!field || !bits) return -1;

    size_t flen = strlen(field);
    if (flen == 0 || flen >= 64) return -1;

    /* quick character validation: only digits, *, -, /, , */
    for (size_t i = 0; i < flen; i++) {
        char c = field[i];
        if (!isdigit((unsigned char)c) && c != '*' && c != '-' &&
            c != '/' && c != ',')
            return -1;
    }

    *bits = 0;

    char buf[64];
    memcpy(buf, field, flen + 1);

    /* iterate comma-separated tokens */
    char *save = NULL;
    char *tok  = strtok_r(buf, ",", &save);
    while (tok) {
        int  start, end, step = 1;
        bool star = false;

        /* extract step */
        char *slash = strchr(tok, '/');
        if (slash) {
            *slash = '\0';
            char *end_ptr;
            errno = 0;
            long sv = strtol(slash + 1, &end_ptr, 10);
            if (errno || *end_ptr || sv < 1 || sv > hi) return -1;
            step = (int)sv;
        }

        /* extract range or star */
        if (strcmp(tok, "*") == 0) {
            start = lo;
            end = hi;
            star = true;
        } else {
            char *dash = strchr(tok, '-');
            if (dash) {
                *dash = '\0';
                char *end_ptr;
                errno = 0;
                long sv = strtol(tok, &end_ptr, 10);
                if (errno || *end_ptr) return -1;
                start = (int)sv;
                errno = 0;
                sv = strtol(dash + 1, &end_ptr, 10);
                if (errno || *end_ptr) return -1;
                end = (int)sv;
            } else {
                char *end_ptr;
                errno = 0;
                long sv = strtol(tok, &end_ptr, 10);
                if (errno || *end_ptr) return -1;
                start = end = (int)sv;
            }
        }

        if (start < lo || end > hi || start > end) return -1;
        (void)star;

        for (int i = start; i <= end; i += step)
            *bits |= (1ULL << i);

        tok = strtok_r(NULL, ",", &save);
    }
    return 0;
}

/*
 * Parse a 5-field cron expression string into a cron_expr_t.
 * Returns 0 on success, -1 on parse error.
 */
static int cron_parse(const char *expr, cron_expr_t *out)
{
    if (!expr || !out) return -1;

    size_t elen = strlen(expr);
    if (elen == 0 || elen >= CRON_EXPR_MAX) return -1;

    char buf[CRON_EXPR_MAX];
    memcpy(buf, expr, elen + 1);

    char *save = NULL;
    char *fields[5];
    int nf = 0;

    char *tok = strtok_r(buf, " \t", &save);
    while (tok && nf < 5) {
        fields[nf++] = tok;
        tok = strtok_r(NULL, " \t", &save);
    }
    if (nf != 5) return -1;

    memset(out, 0, sizeof(*out));

    out->dom_star = (strcmp(fields[2], "*") == 0);
    out->dow_star = (strcmp(fields[4], "*") == 0);

    uint64_t tmp;
    if (parse_field(fields[0], &tmp, 0, 59) < 0) return -1;
    out->minutes = tmp;

    if (parse_field(fields[1], &tmp, 0, 23) < 0) return -1;
    out->hours = (uint32_t)tmp;

    if (parse_field(fields[2], &tmp, 1, 31) < 0) return -1;
    out->dom = (uint32_t)tmp;

    if (parse_field(fields[3], &tmp, 1, 12) < 0) return -1;
    out->months = (uint16_t)tmp;

    if (parse_field(fields[4], &tmp, 0, 6) < 0) return -1;
    out->dow = (uint8_t)tmp;

    return 0;
}

/*
 * Compute the next execution time after `from`.
 * Iterates minute-by-minute (max 1 year) with standard OR semantics for
 * dom/dow when both fields are restricted.
 */
static time_t cron_next(const cron_expr_t *expr, time_t from)
{
    /* align to next full minute */
    time_t t = from - (from % 60) + 60;

    for (int i = 0; i < 366 * 24 * 60; i++, t += 60) {
        struct tm tm;
        localtime_r(&t, &tm);

        /* month: tm_mon is 0-based */
        if (!(expr->months & (1U << (tm.tm_mon + 1)))) continue;

        /* day matching: OR semantics when both dom and dow are restricted */
        bool dom_ok = (expr->dom & (1U << tm.tm_mday)) != 0;
        bool dow_ok = (expr->dow & (1U << tm.tm_wday)) != 0;
        bool day_ok;
        if (!expr->dom_star && !expr->dow_star)
            day_ok = dom_ok || dow_ok;
        else
            day_ok = dom_ok && dow_ok;
        if (!day_ok) continue;

        if (!(expr->hours   & (1U << tm.tm_hour))) continue;
        if (!(expr->minutes & (1ULL << tm.tm_min))) continue;

        return t;
    }
    return (time_t)-1;
}

/* Entry */

typedef struct {
    char id[ID_MAX];
    char payload[PAYLOAD_MAX];
    char expr_str[CRON_EXPR_MAX];
    fastq_priority_t priority;
    cron_expr_t expr;
    time_t next_run;
} cron_entry_t;

/* Scheduler */

struct fastq_scheduler_t {
    fastq_queue_t *queue;
    cron_entry_t *entries;
    int count;
    int cap;
    pthread_mutex_t lock;
    pthread_t thread;
    volatile sig_atomic_t running;
};

/* Redis keys */
static void sched_key(const fastq_scheduler_t *s, char *buf, size_t n)
{
    snprintf(buf, n, "fastq:scheduler:%s:crons", fastq_queue_get_name(s->queue));
}

static void entry_key(const fastq_scheduler_t *s, const char *id, char *buf, size_t n)
{
    snprintf(buf, n, "fastq:scheduler:%s:cron:%s", fastq_queue_get_name(s->queue), id);
}

/* Persist one entry to Redis. */
static void persist_entry(fastq_scheduler_t *s, const cron_entry_t *e)
{
    fastq_pool_t *pool = fastq_queue_get_pool(s->queue);
    redisContext *ctx  = pool ? fastq_pool_acquire(pool) : fastq_redis_get_ctx(NULL);
    if (!ctx) return;

    char ekey[KEY_MAX], skey[KEY_MAX];
    entry_key(s, e->id, ekey, sizeof(ekey));
    sched_key(s, skey, sizeof(skey));

    redisReply *r;
    r = redisCommand(ctx, "HSET %s expr %s payload %s priority %d", ekey, e->expr_str, e->payload, (int)e->priority);
    if (r) freeReplyObject(r);
    r = redisCommand(ctx, "SADD %s %s", skey, e->id);
    if (r) freeReplyObject(r);

    if (pool) fastq_pool_release(pool, ctx);
}

/* Remove one entry from Redis. */
static void unpersist_entry(fastq_scheduler_t *s, const char *id)
{
    fastq_pool_t *pool = fastq_queue_get_pool(s->queue);
    redisContext *ctx  = pool ? fastq_pool_acquire(pool) : fastq_redis_get_ctx(NULL);
    if (!ctx) return;

    char ekey[KEY_MAX], skey[KEY_MAX];
    entry_key(s, id, ekey, sizeof(ekey));
    sched_key(s, skey, sizeof(skey));

    redisReply *r;
    r = redisCommand(ctx, "DEL %s", ekey);
    if (r) freeReplyObject(r);
    r = redisCommand(ctx, "SREM %s %s", skey, id);
    if (r) freeReplyObject(r);

    if (pool) fastq_pool_release(pool, ctx);
}

/* Public API */

fastq_scheduler_t *fastq_scheduler_create(fastq_queue_t *q)
{
    if (!q) return NULL;

    fastq_scheduler_t *s = calloc(1, sizeof(*s));
    if (!s) return NULL;

    s->entries = calloc(8, sizeof(cron_entry_t));
    if (!s->entries) { free(s); return NULL; }

    s->queue = q;
    s->cap = 8;
    pthread_mutex_init(&s->lock, NULL);
    return s;
}

fastq_err_t fastq_scheduler_add_cron(fastq_scheduler_t *s, const char *id, const char *cron_expr, const char *payload, fastq_priority_t priority)
{
    if (!s || !id || !cron_expr || !payload) return FASTQ_ERR;

    if (fastq_validate_name(id) != FASTQ_OK) return FASTQ_ERR_INVALID;

    size_t plen = strlen(payload);
    if (plen == 0 || plen >= PAYLOAD_MAX) return FASTQ_ERR_INVALID;

    cron_expr_t expr;
    if (cron_parse(cron_expr, &expr) < 0) {
        fastq_log(FASTQ_LOG_ERROR, "scheduler: invalid cron expr '%s'", cron_expr);
        return FASTQ_ERR_INVALID;
    }

    pthread_mutex_lock(&s->lock);

    /* reject duplicate id */
    for (int i = 0; i < s->count; i++) {
        if (strcmp(s->entries[i].id, id) == 0) {
            pthread_mutex_unlock(&s->lock);
            return FASTQ_ERR_INVALID;
        }
    }

    if (s->count >= SCHED_MAX_ENTRIES) {
        pthread_mutex_unlock(&s->lock);
        return FASTQ_ERR;
    }

    /* grow if needed */
    if (s->count == s->cap) {
        int newcap = s->cap * 2;
        cron_entry_t *tmp = realloc(s->entries, (size_t)newcap * sizeof(cron_entry_t));
        if (!tmp) { pthread_mutex_unlock(&s->lock); return FASTQ_ERR_ALLOC; }
        s->entries = tmp;
        s->cap     = newcap;
    }

    cron_entry_t *e = &s->entries[s->count++];
    memset(e, 0, sizeof(*e));
    /* safe copies — dest sizes enforced by the checks above */
    snprintf(e->id, sizeof(e->id), "%s", id);
    snprintf(e->payload, sizeof(e->payload), "%s", payload);
    snprintf(e->expr_str, sizeof(e->expr_str), "%s", cron_expr);
    e->expr = expr;
    e->priority = priority;
    e->next_run = cron_next(&expr, time(NULL));

    pthread_mutex_unlock(&s->lock);

    persist_entry(s, e);
    fastq_log(FASTQ_LOG_INFO, "scheduler: added cron '%s' expr='%s'", id, cron_expr);
    return FASTQ_OK;
}

fastq_err_t fastq_scheduler_remove(fastq_scheduler_t *s, const char *id)
{
    if (!s || !id) return FASTQ_ERR;
    if (fastq_validate_name(id) != FASTQ_OK) return FASTQ_ERR_INVALID;

    pthread_mutex_lock(&s->lock);
    fastq_err_t rc = FASTQ_ERR_NOT_FOUND;
    for (int i = 0; i < s->count; i++) {
        if (strcmp(s->entries[i].id, id) == 0) {
            /* swap with last */
            s->entries[i] = s->entries[--s->count];
            rc = FASTQ_OK;
            break;
        }
    }
    pthread_mutex_unlock(&s->lock);

    if (rc == FASTQ_OK) {
        unpersist_entry(s, id);
        fastq_log(FASTQ_LOG_INFO, "scheduler: removed cron '%s'", id);
    }
    return rc;
}

fastq_err_t fastq_scheduler_load(fastq_scheduler_t *s)
{
    if (!s) return FASTQ_ERR;

    fastq_pool_t *pool = fastq_queue_get_pool(s->queue);
    redisContext *ctx  = pool ? fastq_pool_acquire(pool) : fastq_redis_get_ctx(NULL);
    if (!ctx) return FASTQ_ERR_REDIS;

    char skey[KEY_MAX];
    sched_key(s, skey, sizeof(skey));

    redisReply *ids = redisCommand(ctx, "SMEMBERS %s", skey);
    if (!ids || ids->type != REDIS_REPLY_ARRAY) {
        if (ids) freeReplyObject(ids);
        if (pool) fastq_pool_release(pool, ctx);
        return FASTQ_OK;  /* empty, not an error */
    }

    int loaded = 0;
    for (size_t i = 0; i < ids->elements; i++) {
        const char *eid = ids->element[i]->str;
        if (!eid || fastq_validate_name(eid) != FASTQ_OK) continue;

        char ekey[KEY_MAX];
        entry_key(s, eid, ekey, sizeof(ekey));

        redisReply *h = redisCommand(ctx, "HGETALL %s", ekey);
        if (!h || h->type != REDIS_REPLY_ARRAY || h->elements < 6) {
            if (h) freeReplyObject(h);
            continue;
        }

        const char *expr_str = NULL, *payload = NULL;
        int priority = FASTQ_PRIORITY_NORMAL;
        for (size_t j = 0; j + 1 < h->elements; j += 2) {
            const char *k = h->element[j]->str;
            const char *v = h->element[j+1]->str;
            if (!k || !v) continue;
            if (strcmp(k, "expr") == 0) expr_str = v;
            if (strcmp(k, "payload") == 0) payload  = v;
            if (strcmp(k, "priority") == 0) priority = atoi(v);
        }

        if (expr_str && payload) {
            /* silently skip invalid stored entries */
            fastq_scheduler_add_cron(s, eid, expr_str, payload, (fastq_priority_t)priority);
            loaded++;
        }
        freeReplyObject(h);
    }
    freeReplyObject(ids);
    if (pool) fastq_pool_release(pool, ctx);

    fastq_log(FASTQ_LOG_INFO, "scheduler: loaded %d cron job(s) from Redis", loaded);
    return FASTQ_OK;
}

static void *scheduler_thread(void *arg)
{
    fastq_scheduler_t *s = arg;

    while (s->running) {
        time_t now = time(NULL);

        pthread_mutex_lock(&s->lock);
        for (int i = 0; i < s->count; i++) {
            cron_entry_t *e = &s->entries[i];
            if (e->next_run == (time_t)-1 || now < e->next_run) continue;

            /* push job */
            fastq_job_t *job = fastq_job_create(e->payload, e->priority);
            if (job) {
                fastq_push(s->queue, job);
                fastq_job_destroy(job);
                fastq_log(FASTQ_LOG_DEBUG, "scheduler: fired cron '%s'", e->id);
            }
            e->next_run = cron_next(&e->expr, now);
        }
        pthread_mutex_unlock(&s->lock);

        struct timespec ts = { .tv_sec = 1, .tv_nsec = 0 };
        nanosleep(&ts, NULL);
    }
    return NULL;
}

fastq_err_t fastq_scheduler_start(fastq_scheduler_t *s)
{
    if (!s) return FASTQ_ERR;
    s->running = 1;
    if (pthread_create(&s->thread, NULL, scheduler_thread, s) != 0) {
        s->running = 0;
        return FASTQ_ERR;
    }
    fastq_log(FASTQ_LOG_INFO, "scheduler: started (%d entry/entries)", s->count);
    return FASTQ_OK;
}

void fastq_scheduler_stop(fastq_scheduler_t *s)
{
    if (!s || !s->running) return;
    s->running = 0;
    pthread_join(s->thread, NULL);
    fastq_log(FASTQ_LOG_INFO, "scheduler: stopped");
}

void fastq_scheduler_destroy(fastq_scheduler_t *s)
{
    if (!s) return;
    fastq_scheduler_stop(s);
    pthread_mutex_destroy(&s->lock);
    free(s->entries);
    free(s);
}
