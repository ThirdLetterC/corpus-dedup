#include "sentence_set.h"
#include "ckdint_compat.h"
#include "config.h"
#include "hash_utils.h"

#include <stdlib.h>
#include <string.h>

static constexpr size_t MIN_BUCKET_COUNT = 16;
static constexpr size_t DEFAULT_BLOCK_SIZE = 1024;
static constexpr size_t AVG_SENTENCE_BYTES = 64;

typedef struct SentenceEntry {
  uint64_t hash;
  size_t len;
  char8_t *data;
  struct SentenceEntry *next;
} SentenceEntry;

typedef struct SentenceArenaBlock {
  uint8_t *data;
  size_t cap;
  size_t offset;
  struct SentenceArenaBlock *next;
} SentenceArenaBlock;

static size_t round_up_pow2(size_t value) {
#if defined(__STDC_VERSION_STDBIT_H__)
  size_t rounded = stdc_bit_ceil(value);
  if (rounded != 0) {
    return rounded;
  }
#endif
  size_t p = 1;
  while (p < value && p <= SIZE_MAX / 2) {
    p <<= 1;
  }
  return p;
}

static void sentence_arena_init(SentenceArena *arena, size_t block_size) {
  if (!arena)
    return;
  arena->head = nullptr;
  arena->block_size = block_size ? block_size : DEFAULT_BLOCK_SIZE;
}

static void sentence_arena_destroy(SentenceArena *arena) {
  if (!arena)
    return;
  SentenceArenaBlock *block = arena->head;
  while (block) {
    SentenceArenaBlock *next = block->next;
    free(block->data);
    free(block);
    block = next;
  }
  arena->head = nullptr;
  arena->block_size = 0;
}

static void *sentence_arena_alloc(SentenceArena *arena, size_t size) {
  if (!arena || size == 0)
    return nullptr;
  size_t aligned = (size + 7) & ~(size_t)7;
  SentenceArenaBlock *block = arena->head;
  if (!block || block->offset + aligned > block->cap) {
    size_t cap = arena->block_size;
    if (cap < aligned)
      cap = aligned;
    auto next = (SentenceArenaBlock *)calloc(1, sizeof(SentenceArenaBlock));
    if (!next)
      return nullptr;
    next->data = (uint8_t *)calloc(cap, sizeof(uint8_t));
    if (!next->data) {
      free(next);
      return nullptr;
    }
    next->cap = cap;
    next->offset = 0;
    next->next = block;
    arena->head = next;
    block = next;
  }
  void *ptr = block->data + block->offset;
  block->offset += aligned;
  return ptr;
}

[[nodiscard]] static bool sentence_set_rehash(SentenceSet *set,
                                              size_t new_bucket_count);

bool sentence_set_init(SentenceSet *set, size_t bucket_count) {
  if (!set)
    return false;
  size_t size = round_up_pow2(bucket_count < MIN_BUCKET_COUNT ? MIN_BUCKET_COUNT
                                                              : bucket_count);
  size_t alloc_size = 0;
  if (ckd_mul(&alloc_size, size, sizeof(*set->buckets)))
    return false;
  set->buckets = calloc(1, alloc_size);
  if (!set->buckets)
    return false;
  set->bucket_count = size;
  set->entry_count = 0;
  sentence_arena_init(&set->arena, SENTENCE_ARENA_BLOCK_SIZE);
  return true;
}

void sentence_set_destroy(SentenceSet *set) {
  if (!set)
    return;
  free(set->buckets);
  set->buckets = nullptr;
  set->bucket_count = 0;
  set->entry_count = 0;
  sentence_arena_destroy(&set->arena);
}

[[nodiscard]] static bool sentence_set_rehash(SentenceSet *set,
                                              size_t new_bucket_count) {
  if (!set)
    return false;
  size_t size = round_up_pow2(new_bucket_count);
  size_t alloc_size = 0;
  if (ckd_mul(&alloc_size, size, sizeof(*set->buckets)))
    return false;
  SentenceEntry **next = calloc(1, alloc_size);
  if (!next)
    return false;

  for (size_t i = 0; i < set->bucket_count; ++i) {
    SentenceEntry *entry = set->buckets[i];
    while (entry) {
      SentenceEntry *next_entry = entry->next;
      size_t idx = entry->hash & (size - 1);
      entry->next = next[idx];
      next[idx] = entry;
      entry = next_entry;
    }
  }

  free(set->buckets);
  set->buckets = next;
  set->bucket_count = size;
  return true;
}

void sentence_set_reserve_for_bytes(SentenceSet *set, size_t byte_len) {
  if (!set || set->bucket_count == 0)
    return;
  size_t expected = byte_len / AVG_SENTENCE_BYTES;
  if (expected < MIN_BUCKET_COUNT)
    expected = MIN_BUCKET_COUNT;
  size_t target = 0;
  if (ckd_add(&target, set->entry_count, expected)) {
    target = SIZE_MAX;
  }
  size_t needed = 0;
  size_t scaled = 0;
  if (ckd_mul(&scaled, target, (size_t)4)) {
    needed = SIZE_MAX;
  } else {
    needed = scaled / 3;
  }
  if (needed <= set->bucket_count)
    return;
  size_t next_size = round_up_pow2(needed);
  if (next_size > set->bucket_count) {
    (void)sentence_set_rehash(set, next_size);
  }
}

bool sentence_set_insert(SentenceSet *set, const char8_t *data, size_t len,
                         bool *inserted) {
  if (!set || !data || !inserted)
    return false;
  if (set->bucket_count == 0) {
    if (!sentence_set_init(set, 1024))
      return false;
  }

  uint64_t hash = hash_bytes_fnv1a(data, len);
  size_t idx = hash & (set->bucket_count - 1);
  for (SentenceEntry *entry = set->buckets[idx]; entry; entry = entry->next) {
    if (entry->hash == hash && entry->len == len &&
        memcmp(entry->data, data, len) == 0) {
      *inserted = false;
      return true;
    }
  }

  uint8_t *mem = sentence_arena_alloc(&set->arena, sizeof(SentenceEntry) + len);
  if (!mem)
    return false;
  SentenceEntry *entry = (SentenceEntry *)mem;
  char8_t *copy = (char8_t *)(mem + sizeof(SentenceEntry));
  if (len > 0)
    memcpy(copy, data, len);
  entry->hash = hash;
  entry->len = len;
  entry->data = copy;
  entry->next = set->buckets[idx];
  set->buckets[idx] = entry;
  set->entry_count++;
  *inserted = true;

  if (set->entry_count > (set->bucket_count * 3) / 4) {
    size_t next_size = 0;
    if (!ckd_mul(&next_size, set->bucket_count, (size_t)2)) {
      (void)sentence_set_rehash(set, next_size);
    }
  }
  return true;
}
