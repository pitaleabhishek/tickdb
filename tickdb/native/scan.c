/*
 * Tiny native scan kernels for hottest numeric filter loop.
 *
 * Each function reads a fixed-width numeric buffer and writes one byte per row
 * into out_mask: 1 means the row satisfies the predicate, 0 means it does not.
 */
#include <stddef.h>
#include <stdint.h>

void filter_gt_double(
    const double *values,
    size_t count,
    double threshold,
    uint8_t *out_mask
) {
    for (size_t i = 0; i < count; i++) {
        out_mask[i] = values[i] > threshold ? 1 : 0;
    }
}

void filter_ge_double(
    const double *values,
    size_t count,
    double threshold,
    uint8_t *out_mask
) {
    for (size_t i = 0; i < count; i++) {
        out_mask[i] = values[i] >= threshold ? 1 : 0;
    }
}

void filter_lt_double(
    const double *values,
    size_t count,
    double threshold,
    uint8_t *out_mask
) {
    for (size_t i = 0; i < count; i++) {
        out_mask[i] = values[i] < threshold ? 1 : 0;
    }
}

void filter_le_double(
    const double *values,
    size_t count,
    double threshold,
    uint8_t *out_mask
) {
    for (size_t i = 0; i < count; i++) {
        out_mask[i] = values[i] <= threshold ? 1 : 0;
    }
}

void filter_between_double(
    const double *values,
    size_t count,
    double lower,
    uint8_t include_lower,
    double upper,
    uint8_t include_upper,
    uint8_t *out_mask
) {
    /* Inclusive flags let Python map =, >=, <=, and between onto one kernel. */
    for (size_t i = 0; i < count; i++) {
        uint8_t lower_ok = include_lower ? values[i] >= lower : values[i] > lower;
        uint8_t upper_ok = include_upper ? values[i] <= upper : values[i] < upper;
        out_mask[i] = (lower_ok && upper_ok) ? 1 : 0;
    }
}

void filter_gt_int64(
    const int64_t *values,
    size_t count,
    int64_t threshold,
    uint8_t *out_mask
) {
    for (size_t i = 0; i < count; i++) {
        out_mask[i] = values[i] > threshold ? 1 : 0;
    }
}

void filter_ge_int64(
    const int64_t *values,
    size_t count,
    int64_t threshold,
    uint8_t *out_mask
) {
    for (size_t i = 0; i < count; i++) {
        out_mask[i] = values[i] >= threshold ? 1 : 0;
    }
}

void filter_lt_int64(
    const int64_t *values,
    size_t count,
    int64_t threshold,
    uint8_t *out_mask
) {
    for (size_t i = 0; i < count; i++) {
        out_mask[i] = values[i] < threshold ? 1 : 0;
    }
}

void filter_le_int64(
    const int64_t *values,
    size_t count,
    int64_t threshold,
    uint8_t *out_mask
) {
    for (size_t i = 0; i < count; i++) {
        out_mask[i] = values[i] <= threshold ? 1 : 0;
    }
}

void filter_between_int64(
    const int64_t *values,
    size_t count,
    int64_t lower,
    uint8_t include_lower,
    int64_t upper,
    uint8_t include_upper,
    uint8_t *out_mask
) {
    /* Inclusive flags let Python map =, >=, <=, and between onto one kernel. */
    for (size_t i = 0; i < count; i++) {
        uint8_t lower_ok = include_lower ? values[i] >= lower : values[i] > lower;
        uint8_t upper_ok = include_upper ? values[i] <= upper : values[i] < upper;
        out_mask[i] = (lower_ok && upper_ok) ? 1 : 0;
    }
}
