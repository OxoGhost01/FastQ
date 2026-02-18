#include "fastq.h"
#include <stdio.h>
#include <stdarg.h>
#include <time.h>

static fastq_log_level_t g_log_level = FASTQ_LOG_INFO;

static const char *level_str[] = {
    [FASTQ_LOG_DEBUG] = "DEBUG",
    [FASTQ_LOG_INFO]  = "INFO",
    [FASTQ_LOG_WARN]  = "WARN",
    [FASTQ_LOG_ERROR] = "ERROR"
};

static const char *level_color[] = {
    [FASTQ_LOG_DEBUG] = "\033[36m",   /* cyan */
    [FASTQ_LOG_INFO]  = "\033[32m",   /* green */
    [FASTQ_LOG_WARN]  = "\033[33m",   /* yellow */
    [FASTQ_LOG_ERROR] = "\033[31m"    /* red */
};

void fastq_set_log_level(fastq_log_level_t level)
{
    g_log_level = level;
}

void fastq_log(fastq_log_level_t level, const char *fmt, ...)
{
    if (level < g_log_level)
        return;

    time_t now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);

    char timebuf[20];
    strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", &tm);

    fprintf(stderr, "%s%s [%s]\033[0m ", level_color[level], timebuf,
            level_str[level]);

    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);

    fputc('\n', stderr);
}
