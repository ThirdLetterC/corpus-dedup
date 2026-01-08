#ifndef CONFIG_H
#define CONFIG_H

#include <stddef.h>
#include <stdint.h>

extern const uint64_t HASH_MOD;
extern const uint64_t HASH_MULT;
extern const size_t THREAD_COUNT_FALLBACK;
extern const size_t ARENA_BLOCK_SIZE;
extern const size_t FILE_BATCH_SIZE;
extern const char *DUPLICATES_FILENAME;
extern const char *DEFAULT_MASK;
extern const size_t SENTENCE_ARENA_BLOCK_SIZE;
extern const size_t HASH_PARALLEL_BASE;
extern const size_t RADIX_SORT_MIN_COUNT;
extern const size_t SEARCH_ARENA_BLOCK_SIZE;
extern const uint64_t SEARCH_HASH_MULT;

#endif
