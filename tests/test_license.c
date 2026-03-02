#include "fastq.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PASS(name) printf("  %-40s OK\n", name)
#define FAIL(name, msg) do { printf("  %-40s FAIL: %s\n", name, msg); failures++; } while(0)

static int failures = 0;

static void test_no_license(void)
{
    if (fastq_license_valid()) FAIL("no_license", "should be invalid by default");
    if (fastq_license_owner() != NULL) FAIL("no_license", "owner should be NULL");
    PASS("no_license (default invalid)");
}

static void test_null_key(void)
{
    fastq_err_t rc = fastq_license_set(NULL);
    if (rc == FASTQ_OK) FAIL("null_key", "NULL key accepted");
    else PASS("null_key");
}

static void test_bad_format(void)
{
    /* missing colons */
    if (fastq_license_set("justonepart") == FASTQ_OK) FAIL("bad_format", "onepart accepted");
    if (fastq_license_set("a:b") == FASTQ_OK) FAIL("bad_format", "twopart accepted");
    if (fastq_license_set("a:b:TOOLONGHEXXX") == FASTQ_OK) FAIL("bad_format", "bad hmac len accepted");
    if (fastq_license_set("") == FASTQ_OK) FAIL("bad_format", "empty accepted");
    PASS("bad_format");
}

static void test_bad_hmac_chars(void)
{
    /* hmac field must be lowercase hex only */
    char key[256];
    snprintf(key, sizeof(key), "user@example.com:deadbeef:%s", "GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG");
    if (fastq_license_set(key) == FASTQ_OK)
        FAIL("bad_hmac_chars", "uppercase/invalid hex chars accepted");
    else PASS("bad_hmac_chars");
}

static void test_wrong_secret(void)
{
    /* Even with correct format and valid hex, wrong secret → FASTQ_ERR_LICENSE.
       This only runs if FASTQ_LICENSE_SECRET is set in environment. */
    const char *secret = getenv("FASTQ_LICENSE_SECRET");
    if (!secret) {
        PASS("wrong_secret (skipped — FASTQ_LICENSE_SECRET not set)");
        return;
    }

    /* craft a key with garbage hmac */
    char key[256];
    snprintf(key, sizeof(key), "test@example.com:deadbeef:%s", "0000000000000000000000000000000000000000000000000000000000000000");
    fastq_err_t rc = fastq_license_set(key);
    if (rc != FASTQ_ERR_LICENSE && rc != FASTQ_ERR_INVALID)
        FAIL("wrong_secret", "bad hmac should return ERR_LICENSE or ERR_INVALID");
    else PASS("wrong_secret");
}

int main(void)
{
    fastq_set_log_level(FASTQ_LOG_ERROR);
    printf("test_license:\n");

    test_no_license();
    test_null_key();
    test_bad_format();
    test_bad_hmac_chars();
    test_wrong_secret();

    if (failures > 0) {
        printf(" %d test(s) failed.\n", failures);
        return 1;
    }
    printf(" All license tests passed.\n");
    return 0;
}
