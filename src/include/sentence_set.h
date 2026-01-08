#ifndef SENTENCE_SET_H
#define SENTENCE_SET_H

#include <stddef.h>
#include <stdint.h>

#include "utf8.h"

typedef struct SentenceArenaBlock SentenceArenaBlock;
typedef struct SentenceSetShard SentenceSetShard;

typedef struct {
  SentenceArenaBlock *head;
  size_t block_size;
} SentenceArena;

typedef struct {
  SentenceSetShard *shards;
  size_t shard_count;
  size_t shard_mask;
} SentenceSet;

/**
 * Initialize a sentence set with the requested bucket count.
 */
[[nodiscard]] bool sentence_set_init(SentenceSet *set, size_t bucket_count);
/**
 * Release all memory associated with the set.
 */
void sentence_set_destroy(SentenceSet *set);
/**
 * Remove all entries while keeping allocated storage.
 */
void sentence_set_clear(SentenceSet *set);
/**
 * Reserve space for an upcoming insertion batch measured in bytes.
 */
void sentence_set_reserve_for_bytes(SentenceSet *set, size_t byte_len);
/**
 * Insert a sentence with a precomputed hash; sets inserted to true on new key.
 */
[[nodiscard]] bool sentence_set_insert_hashed(SentenceSet *set, uint64_t hash,
                                              const char8_t *data, size_t len,
                                              bool *inserted);
/**
 * Insert a sentence and compute its hash internally.
 */
[[nodiscard]] bool sentence_set_insert(SentenceSet *set, const char8_t *data,
                                       size_t len, bool *inserted);

#endif
