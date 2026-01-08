#ifndef IO_UTILS_H
#define IO_UTILS_H

#include "utf8.h"
#include <stddef.h>

bool read_file_bytes(const char *path, char8_t **out, size_t *out_len);
bool write_file_bytes(const char *path, const char8_t *data, size_t len);
[[nodiscard]] char *dup_string(const char *input);
[[nodiscard]] char *join_path(const char *dir, const char *name);
bool is_regular_file(const char *path);
bool ensure_directory(const char *path, bool create);

#endif
