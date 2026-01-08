#ifndef HASH_UTILS_H
#define HASH_UTILS_H

#include <stddef.h>
#include <stdint.h>

/**
 * Compute the 64-bit FNV-1a hash for a byte buffer.
 */
uint64_t hash_bytes_fnv1a(const unsigned char *data, size_t len);

#endif
