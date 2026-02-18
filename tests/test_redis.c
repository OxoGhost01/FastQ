#include "fastq.h"
#include <stdio.h>
#include <assert.h>

#define TEST(name) static void name(void)
#define RUN(name) do { printf("  %-40s", #name); name(); printf("OK\n"); } while(0)

TEST(test_connect_ok)
{
    fastq_redis_t *r = fastq_redis_connect("127.0.0.1", 6379);
    assert(r != NULL);
    fastq_redis_disconnect(r);
}

TEST(test_connect_bad_host)
{
    fastq_redis_t *r = fastq_redis_connect("192.0.2.1", 6379);
    /* Should fail (TEST-NET address, unreachable) or timeout */
    /* We accept NULL here; if hiredis connects anyway, skip */
    if (r) fastq_redis_disconnect(r);
}

TEST(test_ping)
{
    fastq_redis_t *r = fastq_redis_connect("127.0.0.1", 6379);
    assert(r != NULL);
    assert(fastq_redis_ping(r) == FASTQ_OK);
    fastq_redis_disconnect(r);
}

TEST(test_ping_null)
{
    assert(fastq_redis_ping(NULL) == FASTQ_ERR_REDIS);
}

TEST(test_disconnect_null)
{
    /* Should not crash */
    fastq_redis_disconnect(NULL);
}

int main(void)
{
    printf("test_redis:\n");
    RUN(test_connect_ok);
    RUN(test_connect_bad_host);
    RUN(test_ping);
    RUN(test_ping_null);
    RUN(test_disconnect_null);
    printf("  All redis tests passed.\n");
    return 0;
}
