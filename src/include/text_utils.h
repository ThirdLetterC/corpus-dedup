#ifndef TEXT_UTILS_H
#define TEXT_UTILS_H

#include "utf8.h"
#include <stddef.h>

/**
 * Normalize a sentence into out buffer; returns bytes written.
 */
size_t normalize_sentence(const char8_t *data, size_t len, char8_t *out,
                          size_t out_cap);
/**
 * Trim trailing newline and carriage return in-place.
 */
void trim_line(char *line);

#endif
