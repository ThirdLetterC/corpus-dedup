#ifndef SENTENCE_SET_H
#define SENTENCE_SET_H

#include <stddef.h>
#include <stdint.h>

#include "utf8.h"

typedef struct SentenceArenaBlock SentenceArenaBlock;

typedef struct {
  SentenceArenaBlock *head;
  size_t block_size;
} SentenceArena;

typedef struct {
  uint64_t *hashes;
  size_t *lengths;
  char8_t **data;
  uint8_t *ctrl; // 0xFF = empty, otherwise robin-hood probe distance
  size_t bucket_count;
  size_t entry_count;
  SentenceArena arena;
} SentenceSet;

[[nodiscard]] bool sentence_set_init(SentenceSet *set, size_t bucket_count);
void sentence_set_destroy(SentenceSet *set);
void sentence_set_clear(SentenceSet *set);
void sentence_set_reserve_for_bytes(SentenceSet *set, size_t byte_len);
[[nodiscard]] bool sentence_set_insert_hashed(SentenceSet *set, uint64_t hash,
                                              const char8_t *data, size_t len,
                                              bool *inserted);
[[nodiscard]] bool sentence_set_insert(SentenceSet *set, const char8_t *data,
                                       size_t len, bool *inserted);

#endif
