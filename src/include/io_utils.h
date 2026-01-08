#ifndef IO_UTILS_H
#define IO_UTILS_H

#include "utf8.h"
#include <stddef.h>

/**
 * Read an entire file into a newly allocated buffer.
 */
bool read_file_bytes(const char *path, char8_t **out, size_t *out_len);
/**
 * Write len bytes from data into path atomically when possible.
 */
bool write_file_bytes(const char *path, const char8_t *data, size_t len);
/**
 * Duplicate a C string; caller owns the returned buffer.
 */
[[nodiscard]] char *dup_string(const char *input);
/**
 * Join directory and filename into a newly allocated path.
 */
[[nodiscard]] char *join_path(const char *dir, const char *name);
/**
 * Check whether path refers to a regular file.
 */
bool is_regular_file(const char *path);
/**
 * Ensure the directory exists; create when requested.
 */
bool ensure_directory(const char *path, bool create);

#endif
