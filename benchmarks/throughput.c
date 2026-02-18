#include "fastq.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#define DEFAULT_NUM_JOBS 10000
#define WARMUP_JOBS      200
#define PAYLOAD          "{\"bench\":true,\"data\":\"aaaaaaaaaaaaaaaaaaaaaaaaa\"}"

static double now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e6 + (double)ts.tv_nsec / 1e3;
}

static int cmp_double(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static double percentile(double *sorted, int n, double p)
{
    int idx = (int)(p * (n - 1) / 100.0 + 0.5);
    if (idx < 0) idx = 0;
    if (idx >= n) idx = n - 1;
    return sorted[idx];
}

static void print_latency(double *lat, int n, double elapsed_us)
{
    qsort(lat, (size_t)n, sizeof(double), cmp_double);
    double sum = 0;
    for (int i = 0; i < n; i++) sum += lat[i];
    printf("  throughput : %.0f jobs/s\n", n / (elapsed_us / 1e6));
    printf("  latency    : p50=%.0fus  p95=%.0fus  p99=%.0fus  "
           "min=%.0fus  max=%.0fus  avg=%.0fus\n",
           percentile(lat, n, 50), percentile(lat, n, 95),
           percentile(lat, n, 99), lat[0], lat[n - 1], sum / n);
}

/* ── Push benchmark ────────────────────────────────────────────────── */

static void bench_push(const char *host, int port, int num_jobs)
{
    printf("[push: %d jobs]\n", num_jobs);

    fastq_redis_t *r = fastq_redis_connect(host, port);
    if (!r) { fprintf(stderr, "  connect failed\n"); return; }
    fastq_queue_t *q = fastq_queue_create(r, "bench_push");

    /* warmup */
    for (int i = 0; i < WARMUP_JOBS; i++) {
        fastq_job_t *job = fastq_job_create(PAYLOAD, FASTQ_PRIORITY_NORMAL);
        fastq_push(q, job);
        fastq_job_destroy(job);
    }

    double *lat = malloc((size_t)num_jobs * sizeof(double));
    double t0 = now_us();

    for (int i = 0; i < num_jobs; i++) {
        fastq_job_t *job = fastq_job_create(PAYLOAD, FASTQ_PRIORITY_NORMAL);
        double s = now_us();
        fastq_push(q, job);
        lat[i] = now_us() - s;
        fastq_job_destroy(job);
    }

    print_latency(lat, num_jobs, now_us() - t0);
    free(lat);

    fastq_queue_destroy(q);
    fastq_redis_disconnect(r);
    printf("\n");
}

/* ── Pop benchmark (single thread) ─────────────────────────────────── */

static void bench_pop_single(const char *host, int port, int num_jobs)
{
    printf("[pop (1 thread): %d jobs]\n", num_jobs);

    fastq_redis_t *r = fastq_redis_connect(host, port);
    fastq_queue_t *q = fastq_queue_create(r, "bench_pop");

    /* pre-fill warmup + measured jobs */
    for (int i = 0; i < WARMUP_JOBS + num_jobs; i++) {
        fastq_job_t *job = fastq_job_create(PAYLOAD, FASTQ_PRIORITY_NORMAL);
        fastq_push(q, job);
        fastq_job_destroy(job);
    }

    /* drain warmup silently */
    for (int i = 0; i < WARMUP_JOBS; i++) {
        fastq_job_t *job = fastq_pop(q, 1);
        if (job) { fastq_job_done(q, job); fastq_job_destroy(job); }
    }

    double *lat = malloc((size_t)num_jobs * sizeof(double));
    int popped = 0;
    double t0 = now_us();

    while (popped < num_jobs) {
        double s = now_us();
        fastq_job_t *job = fastq_pop(q, 1);
        if (!job) break;
        lat[popped++] = now_us() - s;
        fastq_job_done(q, job);
        fastq_job_destroy(job);
    }

    print_latency(lat, popped, now_us() - t0);
    free(lat);

    fastq_queue_destroy(q);
    fastq_redis_disconnect(r);
    printf("\n");
}

/* ── Multi-threaded worker benchmark ────────────────────────────────── */

static int bench_handler(fastq_job_t *job, void *user_data)
{
    (void)job;
    (void)user_data;
    return 0;
}

typedef struct {
    fastq_queue_t  *q;
    fastq_worker_t *w;
} drain_arg_t;

static void *drain_thread(void *arg)
{
    drain_arg_t *d = arg;
    for (;;) {
        fastq_stats_t s;
        fastq_stats(d->q, &s);
        if (s.pending == 0 && s.delayed == 0) {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 500000000 };
            nanosleep(&ts, NULL);
            fastq_worker_stop(d->w);
            break;
        }
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 50000000 };
        nanosleep(&ts, NULL);
    }
    return NULL;
}

static void bench_worker_mt(const char *host, int port, int num_jobs,
                            int threads)
{
    printf("[worker: %d thread%s, %d jobs]\n",
           threads, threads > 1 ? "s" : "", num_jobs);

    fastq_queue_t *q = fastq_queue_create_pooled(host, port,
                                                  "bench_mt", threads + 1);
    if (!q) { fprintf(stderr, "  pooled queue failed\n"); return; }

    /* pre-fill */
    for (int i = 0; i < num_jobs; i++) {
        fastq_job_t *job = fastq_job_create(PAYLOAD, FASTQ_PRIORITY_NORMAL);
        fastq_push(q, job);
        fastq_job_destroy(job);
    }

    fastq_worker_t *w = fastq_worker_create(q, bench_handler, NULL);
    fastq_worker_set_threads(w, threads);

    drain_arg_t darg = { .q = q, .w = w };
    pthread_t drain;
    pthread_create(&drain, NULL, drain_thread, &darg);

    double t0 = now_us();
    fastq_worker_start(w);
    double elapsed = now_us() - t0;

    printf("  throughput : %.0f jobs/s\n", num_jobs / (elapsed / 1e6));

    pthread_join(drain, NULL);
    fastq_worker_destroy(w);
    fastq_queue_destroy(q);
    printf("\n");
}

/* ── Main ───────────────────────────────────────────────────────────── */

int main(int argc, char **argv)
{
    const char *host = "127.0.0.1";
    int port = 6379;
    int jobs = DEFAULT_NUM_JOBS;

    if (argc > 1) jobs = atoi(argv[1]);

    fastq_set_log_level(FASTQ_LOG_WARN);

    printf("=== FastQ Benchmark (warmup: %d jobs, measured: %d jobs) ===\n\n",
           WARMUP_JOBS, jobs);

    bench_push(host, port, jobs);
    bench_pop_single(host, port, jobs);

    int thread_counts[] = {1, 2, 4, 8};
    for (int i = 0; i < 4; i++)
        bench_worker_mt(host, port, jobs, thread_counts[i]);

    printf("=== Benchmark limits and methodology ===\n\n"
           "What is measured:\n"
           "  push       MULTI/EXEC round-trip: HSET (job hash) + RPUSH (queue list)\n"
           "  pop        BLPOP (immediate when queue non-empty) + pipelined HSET+LPUSH\n"
           "  worker     End-to-end: pop + handler (no-op) + done, N parallel threads\n\n"
           "What distorts the numbers:\n"
           "  * Network RTT — even on loopback, each round-trip adds ~30-100 us of\n"
           "    kernel overhead. Throughput is bounded by 1/RTT per connection.\n"
           "  * Redis is single-threaded — all commands serialize through one event\n"
           "    loop. Adding worker threads past Redis's saturation point (~200-500k\n"
           "    simple ops/s on localhost) will not increase throughput and may\n"
           "    decrease it due to connection overhead.\n"
           "  * JSON serialization — json-c allocates and formats on every push/pop.\n"
           "    Payload size and field count directly affect this cost.\n"
           "  * Clock granularity — CLOCK_MONOTONIC resolution is typically 1-10 ns\n"
           "    on Linux, but the syscall itself costs ~20-50 ns. Per-job latency\n"
           "    measurements include this overhead twice (start + end).\n"
           "  * Warmup — the first %d pushes prime Redis's memory allocator,\n"
           "    TCP socket buffers, and our own heap. Without warmup, the first\n"
           "    batch would show inflated latency.\n"
           "  * Drain heuristic — the worker benchmark polls stats every 50 ms to\n"
           "    detect queue empty. This adds up to 50 ms + 500 ms of slack to the\n"
           "    measured wall time, slightly underestimating throughput.\n\n"
           "What this benchmark does NOT measure:\n"
           "  * Latency under concurrent load (multiple producers + multiple consumers)\n"
           "  * Behavior with large payloads or many queues\n"
           "  * Priority contention between high/normal/low lanes\n"
           "  * Retry and DLQ paths\n"
           "  * Memory usage per job (estimate: ~300-500 bytes incl. Redis overhead)\n",
           WARMUP_JOBS);

    return 0;
}
