#ifndef CONFIG_H
#define CONFIG_H

#include <stddef.h>
#include <stdint.h>

constexpr uint64_t HASH_MOD = 4'294'967'296ULL; // 2^32
constexpr uint64_t HASH_MULT = 31ULL;
constexpr size_t THREAD_COUNT_FALLBACK = 4;
constexpr size_t ARENA_BLOCK_SIZE = 64 * 1'024 * 1'024; // 64 MiB
constexpr size_t FILE_BATCH_SIZE = 4'096;
constexpr size_t DEFAULT_MAX_COMPARE_LENGTH = 0; // symbols; 0 = unlimited
extern const char *DUPLICATES_FILENAME;
extern const char *DEFAULT_MASK;
constexpr size_t SENTENCE_ARENA_BLOCK_SIZE = 64 * 1'024;
constexpr size_t HASH_PARALLEL_BASE = 64;
constexpr size_t RADIX_SORT_MIN_COUNT = 64;
constexpr size_t SEARCH_ARENA_BLOCK_SIZE = 1'024 * 1'024;
constexpr uint64_t SEARCH_HASH_MULT = 1'315'423'911ULL;

static_assert(HASH_MOD == 4'294'967'296ULL, "HASH_MOD must remain 2^32");
static_assert(HASH_MULT != 0, "HASH_MULT must be non-zero");
static_assert(THREAD_COUNT_FALLBACK > 0, "THREAD_COUNT_FALLBACK must be set");
static_assert(ARENA_BLOCK_SIZE >= 1'024, "ARENA_BLOCK_SIZE too small");
static_assert(FILE_BATCH_SIZE > 0, "FILE_BATCH_SIZE must be positive");
static_assert(SENTENCE_ARENA_BLOCK_SIZE >= 1'024,
              "SENTENCE_ARENA_BLOCK_SIZE too small");
static_assert(HASH_PARALLEL_BASE > 0, "HASH_PARALLEL_BASE must be positive");
static_assert(RADIX_SORT_MIN_COUNT > 0,
              "RADIX_SORT_MIN_COUNT must be positive");
static_assert(SEARCH_ARENA_BLOCK_SIZE >= 1'024,
              "SEARCH_ARENA_BLOCK_SIZE too small");
static_assert(SEARCH_HASH_MULT != 0, "SEARCH_HASH_MULT must be non-zero");

#endif
