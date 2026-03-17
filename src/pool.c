#include "fastq.h"
#include "fastq_internal.h"
#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <string.h>

struct fastq_pool_t {
    redisContext **conns;
    bool *in_use;
    int size;
    int hint;    /* index to start scanning on next acquire */
    char *host;
    int port;
    pthread_mutex_t lock;
    pthread_cond_t avail;
};

fastq_pool_t *fastq_pool_create(const char *host, int port, int size)
{
    if (!host || size <= 0) return NULL;

    fastq_pool_t *p = calloc(1, sizeof(*p));
    if (!p) return NULL;

    p->conns = calloc((size_t)size, sizeof(redisContext *));
    p->in_use = calloc((size_t)size, sizeof(bool));
    p->host = strdup(host);
    p->size = size;
    p->port = port;

    if (!p->conns || !p->in_use || !p->host) {
        free(p->conns); free(p->in_use); free(p->host); free(p);
        return NULL;
    }

    pthread_mutex_init(&p->lock, NULL);
    pthread_cond_init(&p->avail, NULL);

    struct timeval tv = { .tv_sec = 5, .tv_usec = 0 };
    for (int i = 0; i < size; i++) {
        p->conns[i] = redisConnectWithTimeout(host, port, tv);
        if (!p->conns[i] || p->conns[i]->err) {
            fastq_log(FASTQ_LOG_ERROR, "pool: failed to create conn %d: %s", i, p->conns[i] ? p->conns[i]->errstr : "alloc error");
            fastq_pool_destroy(p);
            return NULL;
        }
    }

    fastq_log(FASTQ_LOG_INFO, "pool: created %d connections to %s:%d", size, host, port);
    return p;
}

void fastq_pool_destroy(fastq_pool_t *p)
{
    if (!p) return;
    for (int i = 0; i < p->size; i++) {
        if (p->conns[i]) redisFree(p->conns[i]);
    }
    pthread_mutex_destroy(&p->lock);
    pthread_cond_destroy(&p->avail);
    free(p->conns);
    free(p->in_use);
    free(p->host);
    free(p);
}

redisContext *fastq_pool_acquire(fastq_pool_t *p)
{
    if (!p) return NULL;

    pthread_mutex_lock(&p->lock);
    for (;;) {
        for (int n = 0; n < p->size; n++) {
            int i = (p->hint + n) % p->size;
            if (!p->in_use[i]) {
                p->in_use[i] = true;
                pthread_mutex_unlock(&p->lock);
                return p->conns[i];
            }
        }
        pthread_cond_wait(&p->avail, &p->lock);
    }
}

void fastq_pool_release(fastq_pool_t *p, redisContext *ctx)
{
    if (!p || !ctx) return;

    pthread_mutex_lock(&p->lock);
    for (int i = 0; i < p->size; i++) {
        if (p->conns[i] == ctx) {
            p->in_use[i] = false;
            p->hint = i;
            pthread_cond_signal(&p->avail);
            break;
        }
    }
    pthread_mutex_unlock(&p->lock);
}
