/*
 * license.c — HMAC-SHA256 license key validation.
 *
 * Key format: "{email}:{timestamp_hex}:{hmac_sha256_hex}"
 *   - email        : the licensee's email address
 *   - timestamp_hex: Unix timestamp (hex) of key issuance
 *   - hmac_sha256_hex: lowercase hex of HMAC-SHA256(email:timestamp_hex, secret)
 *
 * The signing secret is read from the FASTQ_LICENSE_SECRET environment variable
 * at validation time. If the variable is not set, all keys are rejected.
 *
 * Security:
 *   - HMAC comparison uses CRYPTO_memcmp (constant-time) to prevent timing attacks.
 *   - All input lengths are validated before any cryptographic operation.
 *   - The email and timestamp are re-serialized from parsed values to prevent
 *     canonicalization attacks.
 */

#include "fastq.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <openssl/hmac.h>
#include <openssl/crypto.h>

#define LICENSE_KEY_MAX 512
#define LICENSE_EMAIL_MAX 254
#define LICENSE_HMAC_HEX 64   /* SHA-256 = 32 bytes = 64 hex chars */

static char g_owner[LICENSE_EMAIL_MAX + 1];
static bool g_valid = false;

/* Convert binary to lowercase hex. out must be at least len*2+1 bytes. */
static void to_hex(const unsigned char *in, size_t len, char *out)
{
    static const char hex[] = "0123456789abcdef";
    for (size_t i = 0; i < len; i++) {
        out[i * 2] = hex[(in[i] >> 4) & 0xF];
        out[i * 2 + 1] = hex[ in[i] & 0xF];
    }
    out[len * 2] = '\0';
}

/* Compute HMAC-SHA256 of message with key, write hex to out (65 bytes). */
static bool compute_hmac(const char *secret, size_t secret_len, const char *message, size_t msg_len, char out[LICENSE_HMAC_HEX + 1])
{
    unsigned char digest[32];
    unsigned int  dlen = sizeof(digest);

    if (!HMAC(EVP_sha256(), secret,  secret_len, (const unsigned char *)message, msg_len, digest, &dlen))
        return false;

    to_hex(digest, dlen, out);
    return true;
}

fastq_err_t fastq_license_set(const char *license_key)
{
    g_valid = false;
    memset(g_owner, 0, sizeof(g_owner));

    if (!license_key) return FASTQ_ERR_INVALID;

    size_t klen = strlen(license_key);
    if (klen == 0 || klen >= LICENSE_KEY_MAX) return FASTQ_ERR_INVALID;

    /* parse: email:timestamp_hex:hmac_hex */
    char buf[LICENSE_KEY_MAX];
    memcpy(buf, license_key, klen + 1);

    char *p1 = strchr(buf, ':');
    if (!p1) return FASTQ_ERR_INVALID;
    *p1 = '\0';
    const char *email = buf;
    char *p2 = strchr(p1 + 1, ':');
    if (!p2) return FASTQ_ERR_INVALID;
    *p2 = '\0';
    const char *ts_hex = p1 + 1;
    const char *hmac_hex = p2 + 1;

    /* validate lengths */
    size_t email_len = strlen(email);
    size_t ts_len = strlen(ts_hex);
    size_t hmac_len = strlen(hmac_hex);

    if (email_len == 0 || email_len > LICENSE_EMAIL_MAX) return FASTQ_ERR_INVALID;
    if (ts_len == 0 || ts_len > 16) return FASTQ_ERR_INVALID;
    if (hmac_len != LICENSE_HMAC_HEX) return FASTQ_ERR_INVALID;

    /* validate hmac_hex chars */
    for (size_t i = 0; i < hmac_len; i++) {
        char c = hmac_hex[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            return FASTQ_ERR_INVALID;
    }

    /* validate ts_hex chars */
    for (size_t i = 0; i < ts_len; i++) {
        char c = ts_hex[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            return FASTQ_ERR_INVALID;
    }

    /* get signing secret */
    const char *secret = getenv("FASTQ_LICENSE_SECRET");
    if (!secret || strlen(secret) == 0) {
        fastq_log(FASTQ_LOG_WARN, "license: FASTQ_LICENSE_SECRET not set, license rejected");
        return FASTQ_ERR_LICENSE;
    }
    size_t secret_len = strlen(secret);

    /* build the signed message: email:timestamp_hex */
    char message[LICENSE_EMAIL_MAX + 1 + 16 + 1];
    int mlen = snprintf(message, sizeof(message), "%s:%s", email, ts_hex);
    if (mlen <= 0 || (size_t)mlen >= sizeof(message)) return FASTQ_ERR_INVALID;

    /* compute expected HMAC */
    char expected[LICENSE_HMAC_HEX + 1];
    if (!compute_hmac(secret, secret_len, message, (size_t)mlen, expected))
        return FASTQ_ERR;

    /* constant-time comparison */
    if (CRYPTO_memcmp(expected, hmac_hex, LICENSE_HMAC_HEX) != 0) {
        fastq_log(FASTQ_LOG_WARN, "license: HMAC verification failed");
        return FASTQ_ERR_LICENSE;
    }

    memcpy(g_owner, email, email_len);
    g_owner[email_len] = '\0';
    g_valid = true;

    fastq_log(FASTQ_LOG_INFO, "license: valid key accepted for '%s'", g_owner);
    return FASTQ_OK;
}

bool fastq_license_valid(void)
{
    return g_valid;
}

const char *fastq_license_owner(void)
{
    return g_valid ? g_owner : NULL;
}
