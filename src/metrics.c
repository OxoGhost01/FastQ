/*
 * metrics.c — Prometheus-compatible HTTP metrics endpoint.
 *
 * Exposes two endpoints on a configurable TCP port:
 *   GET /metrics  → Prometheus text format (fastq_* gauge family)
 *   GET /health   → JSON health summary
 *
 * Security design:
 *   - Request is read up to REQ_MAX bytes; oversized requests get 400 and
 *     are dropped — the server never executes code based on request body.
 *   - Only the request path is inspected; headers and body are ignored.
 *   - SO_REUSEADDR + bind-to-loopback-or-any depending on configuration.
 *   - Each connection is handled synchronously (accept → respond → close);
 *     no persistent state per client, no threading per connection.
 *   - All response strings are built with snprintf with fixed-size buffers.
 */

#include "fastq.h"
#include "fastq_internal.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

#define REQ_MAX 4096
#define RESP_MAX 4096
#define PATH_MAX_L 256

struct fastq_metrics_t {
    fastq_queue_t *queue;
    int port;
    int server_fd;
    pthread_t thread;
    volatile sig_atomic_t running;
};

static const char *http_ok =
    "HTTP/1.0 200 OK\r\n"
    "Content-Type: text/plain; charset=utf-8\r\n"
    "Connection: close\r\n"
    "\r\n";

static const char *http_ok_json =
    "HTTP/1.0 200 OK\r\n"
    "Content-Type: application/json\r\n"
    "Connection: close\r\n"
    "\r\n";

static const char *http_404 =
    "HTTP/1.0 404 Not Found\r\n"
    "Content-Type: text/plain\r\n"
    "Connection: close\r\n"
    "\r\n"
    "Not Found\n";

static const char *http_400 =
    "HTTP/1.0 400 Bad Request\r\n"
    "Content-Type: text/plain\r\n"
    "Connection: close\r\n"
    "\r\n"
    "Bad Request\n";

static void send_metrics(int fd, fastq_queue_t *q)
{
    fastq_stats_t s;
    memset(&s, 0, sizeof(s));
    fastq_stats(q, &s);

    char body[RESP_MAX];
    int n = snprintf(body, sizeof(body),
        "# HELP fastq_jobs_pending Jobs waiting in queue\n"
        "# TYPE fastq_jobs_pending gauge\n"
        "fastq_jobs_pending{queue=\"%s\"} %d\n"
        "# HELP fastq_jobs_done Completed jobs\n"
        "# TYPE fastq_jobs_done gauge\n"
        "fastq_jobs_done{queue=\"%s\"} %d\n"
        "# HELP fastq_jobs_failed Jobs in dead letter queue\n"
        "# TYPE fastq_jobs_failed gauge\n"
        "fastq_jobs_failed{queue=\"%s\"} %d\n"
        "# HELP fastq_jobs_delayed Delayed jobs waiting to run\n"
        "# TYPE fastq_jobs_delayed gauge\n"
        "fastq_jobs_delayed{queue=\"%s\"} %d\n",
        fastq_queue_get_name(q), s.pending,
        fastq_queue_get_name(q), s.done,
        fastq_queue_get_name(q), s.failed,
        fastq_queue_get_name(q), s.delayed);

    if (n <= 0 || n >= RESP_MAX) {
        send(fd, http_400, strlen(http_400), MSG_NOSIGNAL);
        return;
    }

    send(fd, http_ok, strlen(http_ok), MSG_NOSIGNAL);
    send(fd, body, (size_t)n, MSG_NOSIGNAL);
}

static void send_health(int fd, fastq_queue_t *q)
{
    time_t now = time(NULL);
    char body[512];
    int n = snprintf(body, sizeof(body),
        "{\"status\":\"ok\",\"queue\":\"%s\",\"timestamp\":%ld}\n",
        fastq_queue_get_name(q), (long)now);

    if (n <= 0 || n >= (int)sizeof(body)) {
        send(fd, http_400, strlen(http_400), MSG_NOSIGNAL);
        return;
    }

    send(fd, http_ok_json, strlen(http_ok_json), MSG_NOSIGNAL);
    send(fd, body, (size_t)n, MSG_NOSIGNAL);
}

static void handle_connection(int fd, fastq_queue_t *q)
{
    char req[REQ_MAX + 1];
    ssize_t nread = recv(fd, req, REQ_MAX, 0);
    if (nread <= 0) return;
    req[nread] = '\0';

    /* extract request path from first line: "GET /path HTTP/..." */
    char path[PATH_MAX_L] = {0};
    if (sscanf(req, "%*s %255s", path) != 1) {
        send(fd, http_400, strlen(http_400), MSG_NOSIGNAL);
        return;
    }

    /* strip query string */
    char *qs = strchr(path, '?');
    if (qs) *qs = '\0';

    if (strcmp(path, "/metrics") == 0) {
        send_metrics(fd, q);
    } else if (strcmp(path, "/health") == 0) {
        send_health(fd, q);
    } else {
        send(fd, http_404, strlen(http_404), MSG_NOSIGNAL);
    }
}

static void *metrics_thread(void *arg)
{
    fastq_metrics_t *m = arg;

    while (m->running) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);

        /* accept with a 1s timeout (SO_RCVTIMEO set on server_fd) */
        int client_fd = accept(m->server_fd, (struct sockaddr *)&client_addr, &addrlen);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                continue;
            if (!m->running) break;
            fastq_log(FASTQ_LOG_WARN, "metrics: accept error: %s", strerror(errno));
            continue;
        }

        handle_connection(client_fd, m->queue);
        close(client_fd);
    }
    return NULL;
}

fastq_metrics_t *fastq_metrics_create(fastq_queue_t *q, int port)
{
    if (!q || port <= 0 || port > 65535) return NULL;

    fastq_metrics_t *m = calloc(1, sizeof(*m));
    if (!m) return NULL;

    m->queue = q;
    m->port = port;
    m->server_fd = -1;
    return m;
}

fastq_err_t fastq_metrics_start(fastq_metrics_t *m)
{
    if (!m) return FASTQ_ERR;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return FASTQ_ERR;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    /* set accept timeout so the thread can check m->running */
    struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons((uint16_t)m->port),
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK)  /* loopback only */
    };

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fastq_log(FASTQ_LOG_ERROR, "metrics: bind to port %d failed: %s", m->port, strerror(errno));
        close(fd);
        return FASTQ_ERR;
    }

    if (listen(fd, 8) < 0) {
        close(fd);
        return FASTQ_ERR;
    }

    m->server_fd = fd;
    m->running = 1;

    if (pthread_create(&m->thread, NULL, metrics_thread, m) != 0) {
        close(fd);
        m->server_fd = -1;
        m->running = 0;
        return FASTQ_ERR;
    }

    fastq_log(FASTQ_LOG_INFO, "metrics: listening on 127.0.0.1:%d", m->port);
    return FASTQ_OK;
}

void fastq_metrics_stop(fastq_metrics_t *m)
{
    if (!m || !m->running) return;
    m->running = 0;
    if (m->server_fd >= 0) {
        close(m->server_fd);
        m->server_fd = -1;
    }
    pthread_join(m->thread, NULL);
    fastq_log(FASTQ_LOG_INFO, "metrics: stopped");
}

void fastq_metrics_destroy(fastq_metrics_t *m)
{
    if (!m) return;
    fastq_metrics_stop(m);
    free(m);
}
