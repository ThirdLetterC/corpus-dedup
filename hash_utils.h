#ifndef HASH_UTILS_H
#define HASH_UTILS_H

#include <stdint.h>
#include <stddef.h>

uint64_t hash_bytes_fnv1a(const unsigned char *data, size_t len);

#endif
