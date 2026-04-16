#define _POSIX_C_SOURCE 200809L
#define BT_FILL_NO_MAIN
#include "fill.c" /* for sha256_bytes */
#include "core256.h"
#include "core256_mv.h"

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct
{
    size_t samples;
    uint64_t seed;
    char workload[32];
} core_opts_t;

typedef struct
{
    size_t events;
    size_t delay;
    size_t drift_interval;
    uint64_t seed;
    size_t hamming_sample_rate;
    size_t buffer_cap;
    double recovery_threshold;
    size_t recovery_window;
} online_opts_t;

typedef struct
{
    uint8_t key[32];
    uint8_t label;
    size_t release_step;
} pending_event_t;

typedef struct
{
    uint8_t key[32];
    uint8_t label;
} key_item_t;

static void print_usage(const char *argv0)
{
    fprintf(stderr,
            "usage:\n"
            "  %s core [--samples N] [--seed N] [--workload random|clustered|adversarial|replay]\n"
            "  %s online [--events N] [--delay N] [--drift-interval N] [--seed N]\n"
            "             [--hamming-sample-rate N] [--buffer-cap N]\n",
            argv0,
            argv0);
}

static uint64_t bench_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec * UINT64_C(1000000000)) + (uint64_t)ts.tv_nsec;
}

static uint64_t bench_splitmix64_next(uint64_t *state)
{
    uint64_t z;
    *state += UINT64_C(0x9E3779B97F4A7C15);
    z = *state;
    z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
    return z ^ (z >> 31);
}

static uint64_t bench_load_u64_be(const uint8_t *p)
{
    return ((uint64_t)p[0] << 56) |
           ((uint64_t)p[1] << 48) |
           ((uint64_t)p[2] << 40) |
           ((uint64_t)p[3] << 32) |
           ((uint64_t)p[4] << 24) |
           ((uint64_t)p[5] << 16) |
           ((uint64_t)p[6] << 8) |
           ((uint64_t)p[7]);
}

static int bench_cmp_double(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

static void bench_percentiles(const double *values,
                              size_t n,
                              double *out_p50,
                              double *out_p95,
                              double *out_p99,
                              double *out_mean)
{
    double *tmp;
    size_t i;
    size_t i50;
    size_t i95;
    size_t i99;
    double sum = 0.0;

    if (out_p50) *out_p50 = 0.0;
    if (out_p95) *out_p95 = 0.0;
    if (out_p99) *out_p99 = 0.0;
    if (out_mean) *out_mean = 0.0;
    if (!values || n == 0u) return;

    tmp = (double *)malloc(n * sizeof(*tmp));
    if (!tmp) return;
    memcpy(tmp, values, n * sizeof(*tmp));
    qsort(tmp, n, sizeof(*tmp), bench_cmp_double);

    for (i = 0u; i < n; ++i) sum += values[i];
    i50 = (size_t)floor((double)(n - 1u) * 0.50);
    i95 = (size_t)floor((double)(n - 1u) * 0.95);
    i99 = (size_t)floor((double)(n - 1u) * 0.99);

    if (out_p50) *out_p50 = tmp[i50];
    if (out_p95) *out_p95 = tmp[i95];
    if (out_p99) *out_p99 = tmp[i99];
    if (out_mean) *out_mean = sum / (double)n;
    free(tmp);
}

static void bench_fill_random_keys(uint8_t *keys, size_t count, uint64_t seed)
{
    size_t i;
    for (i = 0u; i < count; ++i)
    {
        size_t j;
        uint8_t *dst = keys + i * 32u;
        for (j = 0u; j < 32u; j += 8u)
        {
            uint64_t v = bench_splitmix64_next(&seed);
            dst[j + 0u] = (uint8_t)(v >> 56);
            dst[j + 1u] = (uint8_t)(v >> 48);
            dst[j + 2u] = (uint8_t)(v >> 40);
            dst[j + 3u] = (uint8_t)(v >> 32);
            dst[j + 4u] = (uint8_t)(v >> 24);
            dst[j + 5u] = (uint8_t)(v >> 16);
            dst[j + 6u] = (uint8_t)(v >> 8);
            dst[j + 7u] = (uint8_t)(v >> 0);
        }
    }
}

static void bench_fill_clustered_keys(uint8_t *keys, size_t count, uint64_t seed)
{
    uint8_t centers[8][32];
    size_t i;
    size_t c;

    for (c = 0u; c < 8u; ++c)
    {
        size_t j;
        for (j = 0u; j < 32u; j += 8u)
        {
            uint64_t v = bench_splitmix64_next(&seed);
            centers[c][j + 0u] = (uint8_t)(v >> 56);
            centers[c][j + 1u] = (uint8_t)(v >> 48);
            centers[c][j + 2u] = (uint8_t)(v >> 40);
            centers[c][j + 3u] = (uint8_t)(v >> 32);
            centers[c][j + 4u] = (uint8_t)(v >> 24);
            centers[c][j + 5u] = (uint8_t)(v >> 16);
            centers[c][j + 6u] = (uint8_t)(v >> 8);
            centers[c][j + 7u] = (uint8_t)(v >> 0);
        }
    }

    for (i = 0u; i < count; ++i)
    {
        uint8_t *dst = keys + i * 32u;
        unsigned flips;
        unsigned f;
        size_t center_id = (size_t)(bench_splitmix64_next(&seed) & 7u);
        memcpy(dst, centers[center_id], 32u);

        flips = (unsigned)(1u + (bench_splitmix64_next(&seed) % 4u));
        for (f = 0u; f < flips; ++f)
        {
            size_t bit_pos = (size_t)(bench_splitmix64_next(&seed) % 256u);
            size_t byte_idx = bit_pos >> 3;
            uint8_t bit_mask = (uint8_t)(1u << (bit_pos & 7u));
            dst[byte_idx] ^= bit_mask;
        }
    }
}

static void bench_fill_adversarial_keys(uint8_t *keys, size_t count, uint64_t seed)
{
    uint8_t prefix[32];
    size_t i;
    size_t j;

    for (j = 0u; j < 32u; j += 8u)
    {
        uint64_t v = bench_splitmix64_next(&seed);
        prefix[j + 0u] = (uint8_t)(v >> 56);
        prefix[j + 1u] = (uint8_t)(v >> 48);
        prefix[j + 2u] = (uint8_t)(v >> 40);
        prefix[j + 3u] = (uint8_t)(v >> 32);
        prefix[j + 4u] = (uint8_t)(v >> 24);
        prefix[j + 5u] = (uint8_t)(v >> 16);
        prefix[j + 6u] = (uint8_t)(v >> 8);
        prefix[j + 7u] = (uint8_t)(v >> 0);
    }

    /* Keep first 248 bits stable; vary the tail bits. */
    prefix[31] = 0u;

    for (i = 0u; i < count; ++i)
    {
        uint8_t *dst = keys + i * 32u;
        uint8_t tail = (uint8_t)(i & 0xFFu);
        memcpy(dst, prefix, 32u);
        dst[31] = tail;
    }
}

static void bench_fill_replay_keys(uint8_t *keys, size_t count)
{
    uint8_t current[32];
    uint8_t next[32];
    size_t i;

    memset(current, 0, sizeof(current));
    current[0] = 0x12;
    current[1] = 0x34;
    current[2] = 0x56;
    current[3] = 0x78;

    for (i = 0u; i < count; ++i)
    {
        memcpy(keys + i * 32u, current, 32u);
        sha256_bytes(current, 32u, next);
        memcpy(current, next, 32u);
    }
}

static int bench_generate_keys(uint8_t *keys,
                               size_t count,
                               const char *workload,
                               uint64_t seed)
{
    if (!keys || !workload) return -1;
    if (strcmp(workload, "random") == 0)
    {
        bench_fill_random_keys(keys, count, seed);
        return 0;
    }
    if (strcmp(workload, "clustered") == 0)
    {
        bench_fill_clustered_keys(keys, count, seed);
        return 0;
    }
    if (strcmp(workload, "adversarial") == 0)
    {
        bench_fill_adversarial_keys(keys, count, seed);
        return 0;
    }
    if (strcmp(workload, "replay") == 0)
    {
        bench_fill_replay_keys(keys, count);
        return 0;
    }
    return -1;
}

static int bench_parse_u64(const char *s, uint64_t *out)
{
    char *end = NULL;
    unsigned long long v;
    if (!s || !*s || !out) return -1;
    errno = 0;
    v = strtoull(s, &end, 10);
    if (errno != 0 || !end || *end != '\0') return -1;
    *out = (uint64_t)v;
    return 0;
}

static int bench_parse_size(const char *s, size_t *out)
{
    uint64_t v = 0u;
    if (!out) return -1;
    if (bench_parse_u64(s, &v) != 0) return -1;
    if (v > (uint64_t)SIZE_MAX) return -1;
    *out = (size_t)v;
    return 0;
}

static int bench_parse_double(const char *s, double *out)
{
    char *end = NULL;
    double v;
    if (!s || !*s || !out) return -1;
    errno = 0;
    v = strtod(s, &end);
    if (errno != 0 || !end || *end != '\0') return -1;
    *out = v;
    return 0;
}

static int parse_core_opts(int argc, char **argv, core_opts_t *out)
{
    int i;
    if (!out) return -1;
    out->samples = 10000u;
    out->seed = UINT64_C(0x1234567890ABCDEF);
    snprintf(out->workload, sizeof(out->workload), "random");

    for (i = 0; i < argc; ++i)
    {
        if (strcmp(argv[i], "--samples") == 0)
        {
            if (i + 1 >= argc || bench_parse_size(argv[++i], &out->samples) != 0) return -1;
        }
        else if (strcmp(argv[i], "--seed") == 0)
        {
            if (i + 1 >= argc || bench_parse_u64(argv[++i], &out->seed) != 0) return -1;
        }
        else if (strcmp(argv[i], "--workload") == 0)
        {
            if (i + 1 >= argc) return -1;
            snprintf(out->workload, sizeof(out->workload), "%s", argv[++i]);
        }
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0)
        {
            return 1;
        }
        else
        {
            return -1;
        }
    }

    if (out->samples == 0u) out->samples = 1u;
    if (!(strcmp(out->workload, "random") == 0 ||
          strcmp(out->workload, "clustered") == 0 ||
          strcmp(out->workload, "adversarial") == 0 ||
          strcmp(out->workload, "replay") == 0))
    {
        return -1;
    }
    return 0;
}

static int parse_online_opts(int argc, char **argv, online_opts_t *out)
{
    int i;
    if (!out) return -1;
    out->events = 20000u;
    out->delay = 32u;
    out->drift_interval = 5000u;
    out->seed = UINT64_C(0xCAFEBABE12345678);
    out->hamming_sample_rate = 10u;
    out->buffer_cap = 65536u;
    out->recovery_threshold = 0.65;
    out->recovery_window = 256u;

    for (i = 0; i < argc; ++i)
    {
        if (strcmp(argv[i], "--events") == 0)
        {
            if (i + 1 >= argc || bench_parse_size(argv[++i], &out->events) != 0) return -1;
        }
        else if (strcmp(argv[i], "--delay") == 0)
        {
            if (i + 1 >= argc || bench_parse_size(argv[++i], &out->delay) != 0) return -1;
        }
        else if (strcmp(argv[i], "--drift-interval") == 0)
        {
            if (i + 1 >= argc || bench_parse_size(argv[++i], &out->drift_interval) != 0) return -1;
        }
        else if (strcmp(argv[i], "--seed") == 0)
        {
            if (i + 1 >= argc || bench_parse_u64(argv[++i], &out->seed) != 0) return -1;
        }
        else if (strcmp(argv[i], "--hamming-sample-rate") == 0)
        {
            if (i + 1 >= argc || bench_parse_size(argv[++i], &out->hamming_sample_rate) != 0) return -1;
        }
        else if (strcmp(argv[i], "--buffer-cap") == 0)
        {
            if (i + 1 >= argc || bench_parse_size(argv[++i], &out->buffer_cap) != 0) return -1;
        }
        else if (strcmp(argv[i], "--recovery-threshold") == 0)
        {
            if (i + 1 >= argc || bench_parse_double(argv[++i], &out->recovery_threshold) != 0) return -1;
        }
        else if (strcmp(argv[i], "--recovery-window") == 0)
        {
            if (i + 1 >= argc || bench_parse_size(argv[++i], &out->recovery_window) != 0) return -1;
        }
        else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0)
        {
            return 1;
        }
        else
        {
            return -1;
        }
    }

    if (out->events == 0u) out->events = 1u;
    if (out->hamming_sample_rate == 0u) out->hamming_sample_rate = 1u;
    if (out->buffer_cap == 0u) out->buffer_cap = 1u;
    if (out->drift_interval == 0u) out->drift_interval = out->events + 1u;
    if (out->recovery_window == 0u) out->recovery_window = 1u;
    if (out->recovery_threshold < 0.0) out->recovery_threshold = 0.0;
    if (out->recovery_threshold > 1.0) out->recovery_threshold = 1.0;

    return 0;
}

static void print_core_json_line(const char *operation,
                                 const core_opts_t *opt,
                                 double total_time_s,
                                 double qps,
                                 double p50_ms,
                                 double p95_ms,
                                 double p99_ms,
                                 size_t state_bits,
                                 size_t index_bytes,
                                 double bytes_per_insert,
                                 double state_bits_per_insert,
                                 double mean_matched_bits,
                                 double mean_prefix_gap,
                                 double extra_value,
                                 const char *extra_name)
{
    printf("{\"mode\":\"core\","
           "\"operation\":\"%s\","
           "\"workload\":\"%s\","
           "\"samples\":%zu,"
           "\"seed\":%" PRIu64 ","
           "\"total_time_s\":%.9f,"
           "\"qps\":%.6f,"
           "\"lat_p50_ms\":%.9f,"
           "\"lat_p95_ms\":%.9f,"
           "\"lat_p99_ms\":%.9f,"
           "\"state_bits\":%zu,"
           "\"index_bytes\":%zu,"
           "\"bytes_per_insert\":%.9f,"
           "\"state_bits_per_insert\":%.9f,"
           "\"mean_matched_bits\":%.9f,"
           "\"mean_prefix_gap\":%.9f,",
           operation,
           opt->workload,
           opt->samples,
           opt->seed,
           total_time_s,
           qps,
           p50_ms,
           p95_ms,
           p99_ms,
           state_bits,
           index_bytes,
           bytes_per_insert,
           state_bits_per_insert,
           mean_matched_bits,
           mean_prefix_gap);
    if (extra_name && *extra_name)
    {
        printf("\"%s\":%.9f", extra_name, extra_value);
    }
    else
    {
        printf("\"extra\":%.9f", extra_value);
    }
    printf("}\n");
}

static int run_core_mode(const core_opts_t *opt)
{
    uint8_t *keys = NULL;
    uint8_t *queries = NULL;
    double *lat_insert_ms = NULL;
    double *lat_predict_ms = NULL;
    double *lat_probe_ms = NULL;
    uint32_t *matched = NULL;
    bt256_core_t *tree = NULL;
    size_t i;
    double p50;
    double p95;
    double p99;
    double mean_lat;
    double insert_total_s;
    double predict_total_s;
    double probe_total_s;
    double batch_total_s;
    double qps_insert;
    double qps_predict;
    double qps_probe;
    double qps_batch;
    size_t state_bits;
    size_t index_bytes;
    double bytes_per_insert;
    double state_bits_per_insert;
    double mean_probe_matched = 0.0;
    double mean_probe_gap = 0.0;
    int used_opencl = 0;

    keys = (uint8_t *)malloc(opt->samples * 32u);
    queries = (uint8_t *)malloc(opt->samples * 32u);
    lat_insert_ms = (double *)malloc(opt->samples * sizeof(*lat_insert_ms));
    lat_predict_ms = (double *)malloc(opt->samples * sizeof(*lat_predict_ms));
    lat_probe_ms = (double *)malloc(opt->samples * sizeof(*lat_probe_ms));
    matched = (uint32_t *)malloc(opt->samples * sizeof(*matched));
    if (!keys || !queries || !lat_insert_ms || !lat_predict_ms || !lat_probe_ms || !matched)
    {
        fprintf(stderr, "bench_core256_tool core: allocation failed\n");
        goto fail;
    }

    if (bench_generate_keys(keys, opt->samples, opt->workload, opt->seed) != 0)
    {
        fprintf(stderr, "bench_core256_tool core: unknown workload '%s'\n", opt->workload);
        goto fail;
    }
    if (bench_generate_keys(queries, opt->samples, opt->workload, opt->seed ^ UINT64_C(0xA5A5A5A55A5A5A5A)) != 0)
    {
        goto fail;
    }

    tree = bt256_create(256u, 0);
    if (!tree)
    {
        fprintf(stderr, "bench_core256_tool core: bt256_create failed\n");
        goto fail;
    }

    {
        uint64_t t0_ns = bench_now_ns();
        for (i = 0u; i < opt->samples; ++i)
        {
            uint64_t s_ns = bench_now_ns();
            bt256_status_t rc = bt256_insert(tree, keys + i * 32u, 1);
            uint64_t e_ns = bench_now_ns();
            if (rc != BT256_OK)
            {
                fprintf(stderr, "bench_core256_tool core: insert failed at i=%zu rc=%d\n", i, (int)rc);
                goto fail;
            }
            lat_insert_ms[i] = (double)(e_ns - s_ns) / 1000000.0;
        }
        insert_total_s = (double)(bench_now_ns() - t0_ns) / 1000000000.0;
    }

    qps_insert = insert_total_s > 0.0 ? (double)opt->samples / insert_total_s : 0.0;
    state_bits = bt256_nodes(tree);
    index_bytes = bt256_used_bytes(tree);
    bytes_per_insert = opt->samples ? (double)index_bytes / (double)opt->samples : 0.0;
    state_bits_per_insert = opt->samples ? (double)state_bits / (double)opt->samples : 0.0;

    bench_percentiles(lat_insert_ms, opt->samples, &p50, &p95, &p99, &mean_lat);
    print_core_json_line("insert",
                         opt,
                         insert_total_s,
                         qps_insert,
                         p50,
                         p95,
                         p99,
                         state_bits,
                         index_bytes,
                         bytes_per_insert,
                         state_bits_per_insert,
                         -1.0,
                         -1.0,
                         mean_lat,
                         "lat_mean_ms");

    {
        uint64_t t0_ns = bench_now_ns();
        uint64_t cls_sum = 0u;
        for (i = 0u; i < opt->samples; ++i)
        {
            int cls = 0;
            uint64_t s_ns = bench_now_ns();
            bt256_status_t rc = bt256_predict(tree, queries + i * 32u, &cls);
            uint64_t e_ns = bench_now_ns();
            if (rc != BT256_OK)
            {
                fprintf(stderr, "bench_core256_tool core: predict failed at i=%zu rc=%d\n", i, (int)rc);
                goto fail;
            }
            lat_predict_ms[i] = (double)(e_ns - s_ns) / 1000000.0;
            cls_sum += (uint64_t)(cls != 0);
        }
        predict_total_s = (double)(bench_now_ns() - t0_ns) / 1000000000.0;
        (void)cls_sum;
    }

    qps_predict = predict_total_s > 0.0 ? (double)opt->samples / predict_total_s : 0.0;
    bench_percentiles(lat_predict_ms, opt->samples, &p50, &p95, &p99, &mean_lat);
    print_core_json_line("predict",
                         opt,
                         predict_total_s,
                         qps_predict,
                         p50,
                         p95,
                         p99,
                         state_bits,
                         index_bytes,
                         bytes_per_insert,
                         state_bits_per_insert,
                         -1.0,
                         -1.0,
                         mean_lat,
                         "lat_mean_ms");

    {
        uint64_t t0_ns = bench_now_ns();
        for (i = 0u; i < opt->samples; ++i)
        {
            bt256_probe_t probe;
            uint64_t s_ns = bench_now_ns();
            bt256_status_t rc = bt256_probe(tree, queries + i * 32u, &probe);
            uint64_t e_ns = bench_now_ns();
            if (rc != BT256_OK)
            {
                fprintf(stderr, "bench_core256_tool core: probe failed at i=%zu rc=%d\n", i, (int)rc);
                goto fail;
            }
            lat_probe_ms[i] = (double)(e_ns - s_ns) / 1000000.0;
            mean_probe_matched += (double)probe.matched_bits;
            mean_probe_gap += (double)(256u - probe.matched_bits);
        }
        probe_total_s = (double)(bench_now_ns() - t0_ns) / 1000000000.0;
    }

    mean_probe_matched /= (double)opt->samples;
    mean_probe_gap /= (double)opt->samples;
    qps_probe = probe_total_s > 0.0 ? (double)opt->samples / probe_total_s : 0.0;
    bench_percentiles(lat_probe_ms, opt->samples, &p50, &p95, &p99, &mean_lat);
    print_core_json_line("probe",
                         opt,
                         probe_total_s,
                         qps_probe,
                         p50,
                         p95,
                         p99,
                         state_bits,
                         index_bytes,
                         bytes_per_insert,
                         state_bits_per_insert,
                         mean_probe_matched,
                         mean_probe_gap,
                         mean_lat,
                         "lat_mean_ms");

    {
        uint64_t t0_ns = bench_now_ns();
        bt256_status_t rc = bt256_mv_match_batch(tree, queries, opt->samples, matched, &used_opencl);
        uint64_t dt_ns = bench_now_ns() - t0_ns;
        double per_query_ms;
        double mean_matched = 0.0;

        if (rc != BT256_OK)
        {
            fprintf(stderr, "bench_core256_tool core: match_batch failed rc=%d\n", (int)rc);
            goto fail;
        }

        for (i = 0u; i < opt->samples; ++i) mean_matched += (double)matched[i];
        mean_matched /= (double)opt->samples;
        batch_total_s = (double)dt_ns / 1000000000.0;
        qps_batch = batch_total_s > 0.0 ? (double)opt->samples / batch_total_s : 0.0;
        per_query_ms = (double)dt_ns / 1000000.0 / (double)opt->samples;
        print_core_json_line("match_batch",
                             opt,
                             batch_total_s,
                             qps_batch,
                             per_query_ms,
                             per_query_ms,
                             per_query_ms,
                             state_bits,
                             index_bytes,
                             bytes_per_insert,
                             state_bits_per_insert,
                             mean_matched,
                             256.0 - mean_matched,
                             (double)used_opencl,
                             "used_opencl");
    }

    bt256_destroy(tree);
    free(keys);
    free(queries);
    free(lat_insert_ms);
    free(lat_predict_ms);
    free(lat_probe_ms);
    free(matched);
    return 0;

fail:
    bt256_destroy(tree);
    free(keys);
    free(queries);
    free(lat_insert_ms);
    free(lat_predict_ms);
    free(lat_probe_ms);
    free(matched);
    return 1;
}

static uint8_t bench_key_bit(const uint8_t key[32], unsigned bit)
{
    size_t byte_idx = (size_t)(bit >> 3);
    unsigned bit_in_byte = 7u - (bit & 7u);
    return (uint8_t)((key[byte_idx] >> bit_in_byte) & 1u);
}

static void bench_generate_online_key(uint8_t key[32], uint64_t *state, size_t step, size_t phase)
{
    size_t j;
    for (j = 0u; j < 32u; j += 8u)
    {
        uint64_t v = bench_splitmix64_next(state) ^ ((uint64_t)step * UINT64_C(0x9E3779B97F4A7C15));
        key[j + 0u] = (uint8_t)(v >> 56);
        key[j + 1u] = (uint8_t)(v >> 48);
        key[j + 2u] = (uint8_t)(v >> 40);
        key[j + 3u] = (uint8_t)(v >> 32);
        key[j + 4u] = (uint8_t)(v >> 24);
        key[j + 5u] = (uint8_t)(v >> 16);
        key[j + 6u] = (uint8_t)(v >> 8);
        key[j + 7u] = (uint8_t)(v >> 0);
    }

    /* Phase-dependent drift perturbations. */
    if (phase == 1u)
    {
        key[0] ^= 0xA5u;
        key[9] ^= 0x5Au;
        key[17] ^= 0x33u;
    }
    else if (phase == 2u)
    {
        key[4] ^= 0xCCu;
        key[12] ^= 0x3Cu;
        key[28] ^= 0x0Fu;
    }
}

static uint8_t bench_online_label(const uint8_t key[32], size_t phase)
{
    unsigned score = 0u;
    unsigned i;
    if (phase == 0u)
    {
        for (i = 0u; i < 32u; ++i) score += bench_key_bit(key, i);
    }
    else if (phase == 1u)
    {
        for (i = 64u; i < 128u; ++i) score += bench_key_bit(key, i);
    }
    else
    {
        for (i = 160u; i < 224u; ++i) score += bench_key_bit(key, i);
    }
    return (uint8_t)(score & 1u);
}

static unsigned bench_popcount_xor_256(const uint8_t a[32], const uint8_t b[32])
{
    unsigned total = 0u;
    size_t i;
    for (i = 0u; i < 32u; i += 8u)
    {
        uint64_t xa = bench_load_u64_be(a + i);
        uint64_t xb = bench_load_u64_be(b + i);
        uint64_t xv = xa ^ xb;
#if defined(__GNUC__) || defined(__clang__)
        total += (unsigned)__builtin_popcountll((unsigned long long)xv);
#else
        while (xv)
        {
            xv &= xv - 1u;
            total++;
        }
#endif
    }
    return total;
}

static int bench_predict_by_prefix(const bt256_probe_t *p0,
                                   const bt256_probe_t *p1,
                                   const size_t class_support[2])
{
    if (!p0 || !p1) return 0;
    if (p1->matched_bits > p0->matched_bits) return 1;
    if (p1->matched_bits < p0->matched_bits) return 0;

    /* Tie fallback: choose class with larger observed support. */
    if (class_support[1] > class_support[0]) return 1;
    if (class_support[1] < class_support[0]) return 0;
    return 0;
}

static int run_online_mode(const online_opts_t *opt)
{
    bt256_core_t *trees[2] = {NULL, NULL};
    pending_event_t *pending = NULL;
    key_item_t *buffer = NULL;
    double *update_lat_ms = NULL;
    int *recovery_window_buf = NULL;
    size_t pending_cap;
    size_t pending_head = 0u;
    size_t pending_tail = 0u;
    size_t buffer_start = 0u;
    size_t buffer_count = 0u;
    size_t update_count = 0u;
    uint64_t gen_state = opt->seed;
    size_t step;
    size_t predicted = 0u;
    size_t correct = 0u;
    size_t tp = 0u;
    size_t fp = 0u;
    size_t fn = 0u;
    double prefix_gap_sum = 0.0;
    double hamming_sum = 0.0;
    double hamming_gap_sum = 0.0;
    size_t hamming_samples = 0u;
    size_t drift_count = 0u;
    size_t recovery_count = 0u;
    size_t class_support[2] = {0u, 0u};
    double adaptation_lag_sum = 0.0;
    int recovering = 0;
    size_t drift_start = 0u;
    size_t recovery_buf_pos = 0u;
    size_t recovery_buf_count = 0u;
    size_t recovery_correct_sum = 0u;
    size_t prev_phase = 0u;
    uint64_t t_start_ns;
    uint64_t t_end_ns;
    double total_time_s;
    double qps;
    size_t total_state_bits = 0u;
    size_t total_index_bytes = 0u;
    double update_p50 = 0.0;
    double update_p95 = 0.0;
    double update_p99 = 0.0;
    double update_mean = 0.0;
    double mean_prefix_gap;
    double mean_hamming;
    double mean_hamming_gap;
    double accuracy;
    double precision;
    double recall;
    double f1;
    double bytes_per_insert;
    double state_bits_per_insert;

    trees[0] = bt256_create(256u, 0);
    trees[1] = bt256_create(256u, 0);
    if (!trees[0] || !trees[1])
    {
        fprintf(stderr, "bench_core256_tool online: tree create failed\n");
        goto fail;
    }

    pending_cap = opt->events + opt->delay + 8u;
    pending = (pending_event_t *)calloc(pending_cap, sizeof(*pending));
    buffer = (key_item_t *)calloc(opt->buffer_cap, sizeof(*buffer));
    update_lat_ms = (double *)calloc(opt->events + opt->delay + 8u, sizeof(*update_lat_ms));
    recovery_window_buf = (int *)calloc(opt->recovery_window, sizeof(*recovery_window_buf));
    if (!pending || !buffer || !update_lat_ms || !recovery_window_buf)
    {
        fprintf(stderr, "bench_core256_tool online: allocation failed\n");
        goto fail;
    }

    t_start_ns = bench_now_ns();
    for (step = 0u; step < opt->events; ++step)
    {
        uint8_t key[32];
        uint8_t label;
        size_t phase = (step / opt->drift_interval) % 3u;
        int pred = 0;
        bt256_probe_t p0;
        bt256_probe_t p1;
        size_t matched_max;
        int is_correct;

        while (pending_head < pending_tail && pending[pending_head].release_step <= step)
        {
            pending_event_t *ev = &pending[pending_head];
            uint64_t s_ns;
            uint64_t e_ns;
            bt256_status_t rc;

            s_ns = bench_now_ns();
            rc = bt256_insert(trees[ev->label], ev->key, 1);
            e_ns = bench_now_ns();
            if (rc != BT256_OK)
            {
                fprintf(stderr, "bench_core256_tool online: insert failed rc=%d\n", (int)rc);
                goto fail;
            }

            update_lat_ms[update_count++] = (double)(e_ns - s_ns) / 1000000.0;
            class_support[ev->label]++;

            if (buffer_count < opt->buffer_cap)
            {
                size_t idx = (buffer_start + buffer_count) % opt->buffer_cap;
                buffer[idx].label = ev->label;
                memcpy(buffer[idx].key, ev->key, 32u);
                buffer_count++;
            }
            else
            {
                buffer[buffer_start].label = ev->label;
                memcpy(buffer[buffer_start].key, ev->key, 32u);
                buffer_start = (buffer_start + 1u) % opt->buffer_cap;
            }

            pending_head++;
        }

        if (step > 0u && phase != prev_phase)
        {
            drift_count++;
            recovering = 1;
            drift_start = step;
            recovery_buf_count = 0u;
            recovery_buf_pos = 0u;
            recovery_correct_sum = 0u;
        }
        prev_phase = phase;

        bench_generate_online_key(key, &gen_state, step, phase);
        label = bench_online_label(key, phase);

        if (bt256_probe(trees[0], key, &p0) != BT256_OK || bt256_probe(trees[1], key, &p1) != BT256_OK)
        {
            fprintf(stderr, "bench_core256_tool online: probe failed\n");
            goto fail;
        }

        pred = bench_predict_by_prefix(&p0, &p1, class_support);

        matched_max = (p0.matched_bits > p1.matched_bits) ? p0.matched_bits : p1.matched_bits;
        prefix_gap_sum += (double)(256u - matched_max);

        predicted++;
        is_correct = (pred == (int)label) ? 1 : 0;
        correct += (size_t)is_correct;
        if (pred == 1 && label == 1) tp++;
        else if (pred == 1 && label == 0) fp++;
        else if (pred == 0 && label == 1) fn++;

        if (recovering)
        {
            if (recovery_buf_count < opt->recovery_window)
            {
                recovery_window_buf[recovery_buf_pos] = is_correct;
                recovery_correct_sum += (size_t)is_correct;
                recovery_buf_count++;
                recovery_buf_pos = (recovery_buf_pos + 1u) % opt->recovery_window;
            }
            else
            {
                recovery_correct_sum -= (size_t)recovery_window_buf[recovery_buf_pos];
                recovery_window_buf[recovery_buf_pos] = is_correct;
                recovery_correct_sum += (size_t)is_correct;
                recovery_buf_pos = (recovery_buf_pos + 1u) % opt->recovery_window;
            }

            if (recovery_buf_count == opt->recovery_window)
            {
                double acc = (double)recovery_correct_sum / (double)opt->recovery_window;
                if (acc >= opt->recovery_threshold)
                {
                    recovery_count++;
                    adaptation_lag_sum += (double)(step - drift_start);
                    recovering = 0;
                }
            }
        }

        if (buffer_count > 0u && (step % opt->hamming_sample_rate == 0u))
        {
            unsigned exact = 257u;
            size_t bi;
            for (bi = 0u; bi < buffer_count; ++bi)
            {
                size_t idx = (buffer_start + bi) % opt->buffer_cap;
                unsigned h = bench_popcount_xor_256(key, buffer[idx].key);
                if (h < exact) exact = h;
                if (exact == 0u) break;
            }
            if (exact <= 256u)
            {
                double approx = (double)(256u - matched_max); /* proxy from current structure */
                hamming_sum += (double)exact;
                hamming_gap_sum += fabs(approx - (double)exact);
                hamming_samples++;
            }
        }

        if (pending_tail >= pending_cap)
        {
            fprintf(stderr, "bench_core256_tool online: pending overflow\n");
            goto fail;
        }
        memcpy(pending[pending_tail].key, key, 32u);
        pending[pending_tail].label = label;
        pending[pending_tail].release_step = step + opt->delay;
        pending_tail++;
    }

    while (pending_head < pending_tail)
    {
        pending_event_t *ev = &pending[pending_head];
        uint64_t s_ns;
        uint64_t e_ns;
        bt256_status_t rc;

        s_ns = bench_now_ns();
        rc = bt256_insert(trees[ev->label], ev->key, 1);
        e_ns = bench_now_ns();
        if (rc != BT256_OK)
        {
            fprintf(stderr, "bench_core256_tool online: flush insert failed rc=%d\n", (int)rc);
            goto fail;
        }
        update_lat_ms[update_count++] = (double)(e_ns - s_ns) / 1000000.0;
        class_support[ev->label]++;
        pending_head++;
    }

    t_end_ns = bench_now_ns();
    total_time_s = (double)(t_end_ns - t_start_ns) / 1000000000.0;
    qps = total_time_s > 0.0 ? (double)opt->events / total_time_s : 0.0;

    total_state_bits = bt256_nodes(trees[0]) + bt256_nodes(trees[1]);
    total_index_bytes = bt256_used_bytes(trees[0]) + bt256_used_bytes(trees[1]);
    bytes_per_insert = update_count ? (double)total_index_bytes / (double)update_count : 0.0;
    state_bits_per_insert = update_count ? (double)total_state_bits / (double)update_count : 0.0;

    bench_percentiles(update_lat_ms, update_count, &update_p50, &update_p95, &update_p99, &update_mean);

    mean_prefix_gap = predicted ? prefix_gap_sum / (double)predicted : 0.0;
    mean_hamming = hamming_samples ? hamming_sum / (double)hamming_samples : -1.0;
    mean_hamming_gap = hamming_samples ? hamming_gap_sum / (double)hamming_samples : -1.0;

    accuracy = predicted ? (double)correct / (double)predicted : 0.0;
    precision = (tp + fp) ? (double)tp / (double)(tp + fp) : 0.0;
    recall = (tp + fn) ? (double)tp / (double)(tp + fn) : 0.0;
    f1 = (precision + recall) > 0.0 ? (2.0 * precision * recall) / (precision + recall) : 0.0;

    printf("{\"mode\":\"online\","
           "\"classifier_mode\":\"prefix_match\","
           "\"events\":%zu,"
           "\"delay\":%zu,"
           "\"drift_interval\":%zu,"
           "\"seed\":%" PRIu64 ","
           "\"qps\":%.6f,"
           "\"total_time_s\":%.9f,"
           "\"accuracy\":%.9f,"
           "\"f1\":%.9f,"
           "\"precision\":%.9f,"
           "\"recall\":%.9f,"
           "\"update_latency_ms\":%.9f,"
           "\"update_lat_p50_ms\":%.9f,"
           "\"update_lat_p95_ms\":%.9f,"
           "\"update_lat_p99_ms\":%.9f,"
           "\"state_bits\":%zu,"
           "\"index_bytes\":%zu,"
           "\"bytes_per_insert\":%.9f,"
           "\"state_bits_per_insert\":%.9f,"
           "\"error_bits_prefix_gap\":%.9f,"
           "\"error_bits_min_hamming\":%.9f,"
           "\"hamming_audit_gap\":%.9f,"
           "\"hamming_samples\":%zu,"
           "\"drift_count\":%zu,"
           "\"adaptation_lag_steps\":%.9f}\n",
           opt->events,
           opt->delay,
           opt->drift_interval,
           opt->seed,
           qps,
           total_time_s,
           accuracy,
           f1,
           precision,
           recall,
           update_mean,
           update_p50,
           update_p95,
           update_p99,
           total_state_bits,
           total_index_bytes,
           bytes_per_insert,
           state_bits_per_insert,
           mean_prefix_gap,
           mean_hamming,
           mean_hamming_gap,
           hamming_samples,
           drift_count,
           recovery_count ? (adaptation_lag_sum / (double)recovery_count) : -1.0);

    bt256_destroy(trees[0]);
    bt256_destroy(trees[1]);
    free(pending);
    free(buffer);
    free(update_lat_ms);
    free(recovery_window_buf);
    return 0;

fail:
    bt256_destroy(trees[0]);
    bt256_destroy(trees[1]);
    free(pending);
    free(buffer);
    free(update_lat_ms);
    free(recovery_window_buf);
    return 1;
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        print_usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "core") == 0)
    {
        core_opts_t opt;
        int rc = parse_core_opts(argc - 2, argv + 2, &opt);
        if (rc == 1)
        {
            print_usage(argv[0]);
            return 0;
        }
        if (rc != 0)
        {
            print_usage(argv[0]);
            return 1;
        }
        return run_core_mode(&opt);
    }
    if (strcmp(argv[1], "online") == 0)
    {
        online_opts_t opt;
        int rc = parse_online_opts(argc - 2, argv + 2, &opt);
        if (rc == 1)
        {
            print_usage(argv[0]);
            return 0;
        }
        if (rc != 0)
        {
            print_usage(argv[0]);
            return 1;
        }
        return run_online_mode(&opt);
    }

    print_usage(argv[0]);
    return 1;
}
