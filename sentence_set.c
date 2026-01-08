#include "sentence_set.h"
#include "ckdint_compat.h"
#include "config.h"
#include "hash_utils.h"

#include <stdlib.h>
#include <string.h>

static constexpr size_t MIN_BUCKET_COUNT = 16;
static constexpr size_t DEFAULT_BLOCK_SIZE = 1024;
static constexpr size_t AVG_SENTENCE_BYTES = 64;
static constexpr uint8_t CTRL_EMPTY = 0xFF;
static constexpr size_t LOAD_FACTOR_NUM = 85;
static constexpr size_t LOAD_FACTOR_DEN = 100;
[[nodiscard]] static bool
sentence_set_insert_internal(SentenceSet *set, uint64_t hash,
                             const char8_t *data, size_t len, bool data_owned,
                             bool *inserted);

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

static void sentence_arena_reset(SentenceArena *arena) {
  if (!arena)
    return;
  for (SentenceArenaBlock *block = arena->head; block; block = block->next) {
    block->offset = 0;
  }
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

static char8_t *sentence_set_copy_data(SentenceSet *set, const char8_t *data,
                                       size_t len) {
  size_t alloc_size = 0;
  if (ckd_add(&alloc_size, len, (size_t)1))
    return nullptr;
  char8_t *copy = (char8_t *)sentence_arena_alloc(&set->arena, alloc_size);
  if (!copy)
    return nullptr;
  if (len > 0)
    memcpy(copy, data, len);
  copy[len] = (char8_t)'\0';
  return copy;
}

[[nodiscard]] static bool sentence_set_rehash(SentenceSet *set,
                                              size_t new_bucket_count);

bool sentence_set_init(SentenceSet *set, size_t bucket_count) {
  if (!set)
    return false;
  size_t size = round_up_pow2(bucket_count < MIN_BUCKET_COUNT ? MIN_BUCKET_COUNT
                                                              : bucket_count);
  size_t alloc_hashes = 0;
  size_t alloc_lengths = 0;
  size_t alloc_data = 0;
  size_t alloc_ctrl = 0;
  if (ckd_mul(&alloc_hashes, size, sizeof(*set->hashes)))
    return false;
  if (ckd_mul(&alloc_lengths, size, sizeof(*set->lengths)))
    return false;
  if (ckd_mul(&alloc_data, size, sizeof(*set->data)))
    return false;
  if (ckd_mul(&alloc_ctrl, size, sizeof(*set->ctrl)))
    return false;

  set->hashes = (uint64_t *)calloc(1, alloc_hashes);
  set->lengths = (size_t *)calloc(1, alloc_lengths);
  set->data = (char8_t **)calloc(1, alloc_data);
  set->ctrl = (uint8_t *)calloc(1, alloc_ctrl);
  if (!set->hashes || !set->lengths || !set->data || !set->ctrl) {
    free(set->hashes);
    free(set->lengths);
    free(set->data);
    free(set->ctrl);
    return false;
  }
  memset(set->ctrl, CTRL_EMPTY, alloc_ctrl);
  set->bucket_count = size;
  set->entry_count = 0;
  sentence_arena_init(&set->arena, SENTENCE_ARENA_BLOCK_SIZE);
  return true;
}

void sentence_set_destroy(SentenceSet *set) {
  if (!set)
    return;
  free(set->hashes);
  free(set->lengths);
  free(set->data);
  free(set->ctrl);
  set->hashes = nullptr;
  set->lengths = nullptr;
  set->data = nullptr;
  set->ctrl = nullptr;
  set->bucket_count = 0;
  set->entry_count = 0;
  sentence_arena_destroy(&set->arena);
}

void sentence_set_clear(SentenceSet *set) {
  if (!set || set->bucket_count == 0)
    return;
  memset(set->ctrl, CTRL_EMPTY, set->bucket_count * sizeof(uint8_t));
  set->entry_count = 0;
  sentence_arena_reset(&set->arena);
}

[[nodiscard]] static bool sentence_set_rehash(SentenceSet *set,
                                              size_t new_bucket_count) {
  if (!set)
    return false;
  size_t size = round_up_pow2(new_bucket_count);
  size_t alloc_hashes = 0;
  size_t alloc_lengths = 0;
  size_t alloc_data = 0;
  size_t alloc_ctrl = 0;
  if (ckd_mul(&alloc_hashes, size, sizeof(*set->hashes)))
    return false;
  if (ckd_mul(&alloc_lengths, size, sizeof(*set->lengths)))
    return false;
  if (ckd_mul(&alloc_data, size, sizeof(*set->data)))
    return false;
  if (ckd_mul(&alloc_ctrl, size, sizeof(*set->ctrl)))
    return false;

  uint64_t *new_hashes = (uint64_t *)calloc(1, alloc_hashes);
  size_t *new_lengths = (size_t *)calloc(1, alloc_lengths);
  char8_t **new_data = (char8_t **)calloc(1, alloc_data);
  uint8_t *new_ctrl = (uint8_t *)calloc(1, alloc_ctrl);
  if (!new_hashes || !new_lengths || !new_data || !new_ctrl) {
    free(new_hashes);
    free(new_lengths);
    free(new_data);
    free(new_ctrl);
    return false;
  }
  memset(new_ctrl, CTRL_EMPTY, alloc_ctrl);

  uint64_t *old_hashes = set->hashes;
  size_t *old_lengths = set->lengths;
  char8_t **old_data = set->data;
  uint8_t *old_ctrl = set->ctrl;
  size_t old_bucket_count = set->bucket_count;

  set->hashes = new_hashes;
  set->lengths = new_lengths;
  set->data = new_data;
  set->ctrl = new_ctrl;
  set->bucket_count = size;
  set->entry_count = 0;

  for (size_t i = 0; i < old_bucket_count; ++i) {
    if (old_ctrl[i] == CTRL_EMPTY)
      continue;
    bool inserted = false;
    // data already owns storage; avoid copying
    if (!sentence_set_insert_internal(set, old_hashes[i], old_data[i],
                                      old_lengths[i], true, &inserted)) {
      free(new_hashes);
      free(new_lengths);
      free(new_data);
      free(new_ctrl);
      set->hashes = old_hashes;
      set->lengths = old_lengths;
      set->data = old_data;
      set->ctrl = old_ctrl;
      set->bucket_count = old_bucket_count;
      set->entry_count = 0;
      for (size_t j = 0; j < old_bucket_count; ++j) {
        if (old_ctrl[j] != CTRL_EMPTY)
          set->entry_count++;
      }
      return false;
    }
    (void)inserted;
  }

  free(old_hashes);
  free(old_lengths);
  free(old_data);
  free(old_ctrl);
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
  if (ckd_mul(&scaled, target, (size_t)5)) {
    needed = SIZE_MAX;
  } else {
    needed = scaled / 4;
  }
  if (needed <= set->bucket_count)
    return;
  size_t next_size = round_up_pow2(needed);
  if (next_size > set->bucket_count) {
    (void)sentence_set_rehash(set, next_size);
  }
}

[[nodiscard]] bool sentence_set_insert_hashed(SentenceSet *set, uint64_t hash,
                                              const char8_t *data, size_t len,
                                              bool *inserted) {
  return sentence_set_insert_internal(set, hash, data, len, false, inserted);
}

[[nodiscard]] static bool
sentence_set_insert_internal(SentenceSet *set, uint64_t hash,
                             const char8_t *data, size_t len, bool data_owned,
                             bool *inserted) {
  if (!set || !data || !inserted)
    return false;
  if (set->bucket_count == 0) {
    if (!sentence_set_init(set, 1024))
      return false;
  }

  size_t threshold_num = set->bucket_count * LOAD_FACTOR_NUM;
  size_t threshold_den = LOAD_FACTOR_DEN;
  size_t threshold = threshold_num / threshold_den;
  if (threshold == 0)
    threshold = 1;
  if (set->entry_count + 1 > threshold) {
    size_t next_size = 0;
    if (!ckd_mul(&next_size, set->bucket_count, (size_t)2)) {
      if (!sentence_set_rehash(set, next_size))
        return false;
    }
  }

  size_t idx = hash & (set->bucket_count - 1);
  uint8_t dist = 0;

  uint64_t cand_hash = hash;
  size_t cand_len = len;
  const char8_t *cand_data = data;
  bool cand_owned = data_owned;

  while (true) {
    uint8_t ctrl = set->ctrl[idx];
    if (ctrl == CTRL_EMPTY) {
      char8_t *stored = cand_owned
                            ? (char8_t *)cand_data
                            : sentence_set_copy_data(set, cand_data, cand_len);
      if (!stored)
        return false;
      set->hashes[idx] = cand_hash;
      set->lengths[idx] = cand_len;
      set->data[idx] = stored;
      set->ctrl[idx] = dist;
      set->entry_count++;
      *inserted = true;
      return true;
    }

    if (set->hashes[idx] == cand_hash && set->lengths[idx] == cand_len &&
        memcmp(set->data[idx], cand_data, cand_len) == 0) {
      *inserted = false;
      return true;
    }

    if (ctrl < dist) {
      uint64_t displaced_hash = set->hashes[idx];
      size_t displaced_len = set->lengths[idx];
      char8_t *displaced_data = set->data[idx];
      uint8_t displaced_ctrl = ctrl;

      char8_t *stored = cand_owned
                            ? (char8_t *)cand_data
                            : sentence_set_copy_data(set, cand_data, cand_len);
      if (!stored)
        return false;
      set->hashes[idx] = cand_hash;
      set->lengths[idx] = cand_len;
      set->data[idx] = stored;
      set->ctrl[idx] = dist;

      cand_hash = displaced_hash;
      cand_len = displaced_len;
      cand_data = displaced_data;
      cand_owned = true;
      dist = displaced_ctrl + 1;
      idx = (idx + 1) & (set->bucket_count - 1);
      continue;
    }

    dist++;
    idx = (idx + 1) & (set->bucket_count - 1);
    if (dist == CTRL_EMPTY) {
      // Table is too dense; grow and retry.
      size_t next_size = 0;
      if (ckd_mul(&next_size, set->bucket_count, (size_t)2))
        return false;
      if (!sentence_set_rehash(set, next_size))
        return false;
      return sentence_set_insert_internal(set, hash, data, len, data_owned,
                                          inserted);
    }
  }
}

bool sentence_set_insert(SentenceSet *set, const char8_t *data, size_t len,
                         bool *inserted) {
  uint64_t hash = hash_bytes_fnv1a(data, len);
  return sentence_set_insert_hashed(set, hash, data, len, inserted);
}
