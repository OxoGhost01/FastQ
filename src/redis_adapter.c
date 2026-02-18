#include "fastq.h"
#include "fastq_internal.h"
#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <string.h>

struct fastq_redis_t {
    redisContext *ctx;
    char *host;
    int port;
};

fastq_redis_t *fastq_redis_connect(const char *host, int port)
{
    fastq_redis_t *r = calloc(1, sizeof(*r));
    if (!r) {
        fastq_log(FASTQ_LOG_ERROR, "redis: allocation failed");
        return NULL;
    }

    struct timeval tv = { .tv_sec = 5, .tv_usec = 0 };
    r->ctx = redisConnectWithTimeout(host, port, tv);
    if (!r->ctx || r->ctx->err) {
        fastq_log(FASTQ_LOG_ERROR, "redis: connect failed: %s", r->ctx ? r->ctx->errstr : "allocation error");
        if (r->ctx) redisFree(r->ctx);
        free(r);
        return NULL;
    }

    r->host = strdup(host);
    r->port = port;

    fastq_log(FASTQ_LOG_INFO, "redis: connected to %s:%d", host, port);
    return r;
}

void fastq_redis_disconnect(fastq_redis_t *r)
{
    if (!r) return;
    if (r->ctx) redisFree(r->ctx);
    free(r->host);
    free(r);
    fastq_log(FASTQ_LOG_INFO, "redis: disconnected");
}

fastq_err_t fastq_redis_ping(fastq_redis_t *r)
{
    if (!r || !r->ctx) return FASTQ_ERR_REDIS;

    redisReply *reply = redisCommand(r->ctx, "PING");
    if (!reply) {
        fastq_log(FASTQ_LOG_ERROR, "redis: ping failed: %s", r->ctx->errstr);
        return FASTQ_ERR_REDIS;
    }

    int ok = (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "PONG") == 0);
    freeReplyObject(reply);
    return ok ? FASTQ_OK : FASTQ_ERR_REDIS;
}

redisContext *fastq_redis_get_ctx(fastq_redis_t *r)
{
    return r ? r->ctx : NULL;
}
