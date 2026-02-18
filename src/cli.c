#include "fastq.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <getopt.h>

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 6379

static fastq_worker_t *g_worker = NULL;

static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s [options] <command> [args]\n"
        "\n"
        "Commands:\n"
        "  push   <queue> <payload> [priority]  Push a job (priority: 1=high 2=normal 3=low)\n"
        "  pop    <queue>                       Pop and print one job\n"
        "  stats  <queue>                       Show queue statistics\n"
        "  worker <queue>                       Start a worker\n"
        "  recover <queue>                      Re-queue orphaned jobs\n"
        "\n"
        "Options:\n"
        "  -h, --host <host>       Redis host (default: 127.0.0.1)\n"
        "  -p, --port <port>       Redis port (default: 6379)\n"
        "  -t, --threads <n>       Worker threads (default: 1)\n"
        "  -d, --daemon            Daemonize worker\n"
        "  --pid-file <path>       PID file for daemon mode\n"
        "  --log-file <path>       Log file for daemon mode\n"
        "  -v, --verbose           Enable debug logging\n"
        "  --help                  Show this help\n",
        prog);
}

static void sighandler(int sig)
{
    (void)sig;
    if (g_worker) fastq_worker_stop(g_worker);
}

static int cmd_push(fastq_queue_t *q, const char *payload, int priority)
{
    fastq_job_t *job = fastq_job_create(payload, priority);
    if (!job) {
        fprintf(stderr, "error: failed to create job\n");
        return 1;
    }

    fastq_err_t err = fastq_push(q, job);
    if (err != FASTQ_OK) {
        fprintf(stderr, "error: push failed (%d)\n", err);
        fastq_job_destroy(job);
        return 1;
    }

    printf("%s\n", job->id);
    fastq_job_destroy(job);
    return 0;
}

static int cmd_pop(fastq_queue_t *q)
{
    fastq_job_t *job = fastq_pop(q, 5);
    if (!job) {
        fprintf(stderr, "no job available (timeout)\n");
        return 1;
    }

    printf("id:       %s\n", job->id);
    printf("payload:  %s\n", job->payload);
    printf("priority: %d\n", job->priority);

    fastq_job_done(q, job);
    fastq_job_destroy(job);
    return 0;
}

static int cmd_stats(fastq_queue_t *q)
{
    fastq_stats_t s;
    if (fastq_stats(q, &s) != FASTQ_OK) {
        fprintf(stderr, "error: failed to get stats\n");
        return 1;
    }

    printf("pending:    %d\n", s.pending);
    printf("processing: %d\n", s.processing);
    printf("done:       %d\n", s.done);
    printf("failed:     %d\n", s.failed);
    printf("delayed:    %d\n", s.delayed);
    return 0;
}

static int dev_handler(fastq_job_t *job, void *user_data)
{
    (void)user_data;
    printf("[worker] job %s : %s\n", job->id, job->payload);
    return 0;
}

static int cmd_worker(fastq_queue_t *q, int threads, int daemon_mode,
                      const char *pid_file, const char *log_file)
{
    if (daemon_mode) {
        if (fastq_daemonize(pid_file, log_file) != FASTQ_OK) {
            fprintf(stderr, "error: failed to daemonize\n");
            return 1;
        }
    }

    g_worker = fastq_worker_create(q, dev_handler, NULL);
    if (!g_worker) {
        fprintf(stderr, "error: failed to create worker\n");
        return 1;
    }

    fastq_worker_set_threads(g_worker, threads);

    signal(SIGINT,  sighandler);
    signal(SIGTERM, sighandler);

    if (!daemon_mode)
        printf("Worker started (%d thread%s). Press Ctrl+C to stop.\n",
               threads, threads > 1 ? "s" : "");

    fastq_worker_start(g_worker);
    fastq_worker_destroy(g_worker);
    g_worker = NULL;
    return 0;
}

static int cmd_recover(fastq_queue_t *q)
{
    int n = fastq_recover_orphaned_jobs(q);
    printf("recovered %d orphaned job(s)\n", n);
    return 0;
}

int main(int argc, char **argv)
{
    const char *host = DEFAULT_HOST;
    int port = DEFAULT_PORT;
    int threads = 1;
    int daemon_mode = 0;
    const char *pid_file = NULL;
    const char *log_file = NULL;

    enum { OPT_HELP = 256, OPT_PID, OPT_LOG };

    static struct option long_opts[] = {
        {"host",     required_argument, NULL, 'h'},
        {"port",     required_argument, NULL, 'p'},
        {"threads",  required_argument, NULL, 't'},
        {"daemon",   no_argument,       NULL, 'd'},
        {"pid-file", required_argument, NULL, OPT_PID},
        {"log-file", required_argument, NULL, OPT_LOG},
        {"verbose",  no_argument,       NULL, 'v'},
        {"help",     no_argument,       NULL, OPT_HELP},
        {NULL, 0, NULL, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "h:p:t:dv", long_opts, NULL)) != -1) {
        switch (opt) {
        case 'h': host = optarg; break;
        case 'p': port = atoi(optarg); break;
        case 't': threads = atoi(optarg); break;
        case 'd': daemon_mode = 1; break;
        case 'v': fastq_set_log_level(FASTQ_LOG_DEBUG); break;
        case OPT_PID: pid_file = optarg; break;
        case OPT_LOG: log_file = optarg; break;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    if (optind >= argc) {
        usage(argv[0]);
        return 1;
    }

    const char *command = argv[optind];
    const char *queue_name = (optind + 1 < argc) ? argv[optind + 1] : NULL;

    if (!queue_name) {
        fprintf(stderr, "error: missing queue name\n");
        usage(argv[0]);
        return 1;
    }

    /* Connect */
    fastq_redis_t *redis = fastq_redis_connect(host, port);
    if (!redis) {
        fprintf(stderr, "error: cannot connect to Redis at %s:%d\n",
                host, port);
        return 1;
    }

    fastq_queue_t *q = fastq_queue_create(redis, queue_name);
    int rc = 1;

    if (strcmp(command, "push") == 0) {
        const char *payload = (optind + 2 < argc) ? argv[optind + 2] : NULL;
        int prio = FASTQ_PRIORITY_NORMAL;
        if (optind + 3 < argc) prio = atoi(argv[optind + 3]);
        if (!payload) {
            fprintf(stderr, "error: missing payload\n");
            usage(argv[0]);
        } else {
            rc = cmd_push(q, payload, prio);
        }
    } else if (strcmp(command, "pop") == 0) {
        rc = cmd_pop(q);
    } else if (strcmp(command, "stats") == 0) {
        rc = cmd_stats(q);
    } else if (strcmp(command, "worker") == 0) {
        rc = cmd_worker(q, threads, daemon_mode, pid_file, log_file);
    } else if (strcmp(command, "recover") == 0) {
        rc = cmd_recover(q);
    } else {
        fprintf(stderr, "error: unknown command '%s'\n", command);
        usage(argv[0]);
    }

    fastq_queue_destroy(q);
    fastq_redis_disconnect(redis);
    return rc;
}
