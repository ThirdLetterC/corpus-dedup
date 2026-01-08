#include "config.h"

const uint64_t HASH_MOD = 4294967296ULL; // 2^32
const uint64_t HASH_MULT = 31ULL;
const size_t THREAD_COUNT_FALLBACK = 4;
const size_t ARENA_BLOCK_SIZE = 64 * 1024 * 1024; // 64 MiB
const size_t FILE_BATCH_SIZE = 4096;
const char *DUPLICATES_FILENAME = "duplicates.txt";
const char *DEFAULT_MASK = "*.txt";
const size_t SENTENCE_ARENA_BLOCK_SIZE = 64 * 1024;
const size_t HASH_PARALLEL_BASE = 64;
const size_t RADIX_SORT_MIN_COUNT = 64;
const size_t SEARCH_ARENA_BLOCK_SIZE = 1024 * 1024;
const uint64_t SEARCH_HASH_MULT = 1315423911ULL;
