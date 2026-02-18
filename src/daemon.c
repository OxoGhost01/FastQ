#include "fastq.h"
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <signal.h>

fastq_err_t fastq_daemonize(const char *pid_file, const char *log_file)
{
    pid_t pid = fork();
    if (pid < 0) {
        fastq_log(FASTQ_LOG_ERROR, "daemon: fork failed");
        return FASTQ_ERR;
    }
    if (pid > 0) _exit(0);

    if (setsid() < 0) {
        fastq_log(FASTQ_LOG_ERROR, "daemon: setsid failed");
        return FASTQ_ERR;
    }

    signal(SIGHUP, SIG_IGN);

    pid = fork();
    if (pid < 0) return FASTQ_ERR;
    if (pid > 0) _exit(0);

    umask(0);
    if (chdir("/") != 0) { /* best-effort */ }

    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);

    open("/dev/null", O_RDONLY);

    const char *out = log_file ? log_file : "/dev/null";
    int fd = open(out, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) fd = open("/dev/null", O_WRONLY);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    if (fd > STDERR_FILENO) close(fd);

    if (pid_file) {
        FILE *f = fopen(pid_file, "w");
        if (f) {
            fprintf(f, "%d\n", getpid());
            fclose(f);
        }
    }

    fastq_log(FASTQ_LOG_INFO, "daemon: started (pid=%d)", getpid());
    return FASTQ_OK;
}
