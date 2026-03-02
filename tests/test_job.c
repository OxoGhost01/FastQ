#include "fastq.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define TEST(name) static void name(void)
#define RUN(name) do { printf("  %-40s", #name); name(); printf("OK\n"); } while(0)

TEST(test_job_create_destroy)
{
    fastq_job_t *job = fastq_job_create("{\"key\":\"value\"}", FASTQ_PRIORITY_NORMAL);
    assert(job != NULL);
    assert(job->id != NULL);
    assert(strcmp(job->payload, "{\"key\":\"value\"}") == 0);
    assert(job->priority == FASTQ_PRIORITY_NORMAL);
    assert(job->retries == 0);
    assert(job->max_retries == 5);
    assert(job->created_at > 0);
    fastq_job_destroy(job);
}

TEST(test_job_create_null_payload)
{
    fastq_job_t *job = fastq_job_create(NULL, FASTQ_PRIORITY_HIGH);
    assert(job == NULL);
}

TEST(test_job_priorities)
{
    fastq_job_t *h = fastq_job_create("{}", FASTQ_PRIORITY_HIGH);
    fastq_job_t *n = fastq_job_create("{}", FASTQ_PRIORITY_NORMAL);
    fastq_job_t *l = fastq_job_create("{}", FASTQ_PRIORITY_LOW);
    assert(h->priority == 1);
    assert(n->priority == 2);
    assert(l->priority == 3);
    fastq_job_destroy(h);
    fastq_job_destroy(n);
    fastq_job_destroy(l);
}

TEST(test_job_serialize_deserialize)
{
    fastq_job_t *job = fastq_job_create("{\"user\":123}", FASTQ_PRIORITY_HIGH);
    assert(job != NULL);

    char *json = fastq_job_serialize(job);
    assert(json != NULL);

    fastq_job_t *restored = fastq_job_deserialize(json);
    assert(restored != NULL);
    assert(strcmp(restored->id, job->id) == 0);
    assert(strcmp(restored->payload, job->payload) == 0);
    assert(restored->priority == job->priority);
    assert(restored->retries == job->retries);
    assert(restored->max_retries == job->max_retries);
    assert(restored->created_at == job->created_at);

    free(json);
    fastq_job_destroy(job);
    fastq_job_destroy(restored);
}

TEST(test_job_deserialize_invalid)
{
    fastq_job_t *job = fastq_job_deserialize("not json");
    assert(job == NULL);

    job = fastq_job_deserialize(NULL);
    assert(job == NULL);

    /* Missing required fields */
    job = fastq_job_deserialize("{\"foo\":\"bar\"}");
    assert(job == NULL);
}

TEST(test_job_serialize_with_error)
{
    fastq_job_t *job = fastq_job_create("{}", FASTQ_PRIORITY_NORMAL);
    job->error = strdup("something went wrong");
    job->retries = 3;

    char *json = fastq_job_serialize(job);
    assert(json != NULL);

    fastq_job_t *restored = fastq_job_deserialize(json);
    assert(restored != NULL);
    assert(strcmp(restored->error, "something went wrong") == 0);
    assert(restored->retries == 3);

    free(json);
    fastq_job_destroy(job);
    fastq_job_destroy(restored);
}

TEST(test_job_unique_ids)
{
    fastq_job_t *a = fastq_job_create("{}", FASTQ_PRIORITY_NORMAL);
    fastq_job_t *b = fastq_job_create("{}", FASTQ_PRIORITY_NORMAL);
    assert(a != NULL && b != NULL);
    assert(strcmp(a->id, b->id) != 0);
    fastq_job_destroy(a);
    fastq_job_destroy(b);
}

int main(void)
{
    printf("test_job:\n");
    RUN(test_job_create_destroy);
    RUN(test_job_create_null_payload);
    RUN(test_job_priorities);
    RUN(test_job_serialize_deserialize);
    RUN(test_job_deserialize_invalid);
    RUN(test_job_serialize_with_error);
    RUN(test_job_unique_ids);
    printf("  All job tests passed.\n");
    return 0;
}
