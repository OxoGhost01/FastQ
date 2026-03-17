/*
 * license.c — stub (free / public build).
 * Pro feature. All functions are no-ops that return FASTQ_ERR_LICENSE.
 * Replace with the real implementation for a licensed build.
 */

#include "fastq.h"

fastq_err_t fastq_license_set(const char *license_key)
{
    (void)license_key;
    return FASTQ_ERR_LICENSE;
}

bool fastq_license_valid(void)
{
    return false;
}

const char *fastq_license_owner(void)
{
    return NULL;
}
