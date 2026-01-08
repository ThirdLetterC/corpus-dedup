#include "hash_utils.h"

uint64_t hash_bytes_fnv1a(const unsigned char *data, size_t len) {
  uint64_t hash = 1469598103934665603ULL;
  for (size_t i = 0; i < len; ++i) {
    hash ^= data[i];
    hash *= 1099511628211ULL;
  }
  return hash;
}
