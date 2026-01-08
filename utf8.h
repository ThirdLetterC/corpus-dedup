#ifndef UTF8_UTIL_H
#define UTF8_UTIL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <uchar.h>

#if defined(__clang__)
#if __has_feature(c_char8_t)
#define UTF8_UTIL_HAVE_CHAR8_T 1
#endif
#elif defined(__GNUC__)
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 202311L
#define UTF8_UTIL_HAVE_CHAR8_T 1
#endif
#endif

#if !defined(UTF8_UTIL_HAVE_CHAR8_T)
typedef unsigned char char8_t;
#endif

/**
 * Decode one UTF-8 code point.
 * @param bytes Pointer to the first byte.
 * @param len   Number of available bytes.
 * @param out_codepoint Optional output for the decoded scalar value.
 * @param out_invalid   Optional flag set when an invalid sequence is seen.
 * @return Number of bytes consumed (0 on invalid/incomplete input).
 */
size_t utf8_decode_advance(const char8_t *bytes, size_t len,
                           uint32_t *out_codepoint, bool *out_invalid);

/**
 * Decode a UTF-8 buffer into a newly allocated UTF-32 array.
 * Caller owns the returned buffer.
 */
[[nodiscard]] bool utf8_decode_buffer(const char8_t *input, size_t len,
                                      uint32_t **out, size_t *out_len,
                                      size_t *invalid_count);

#endif
