#include <stdlib.h>
#include <string.h>
#include <threads.h>

#include "sentence_set.h"
#include "ckdint_compat.h"
#include "config.h"
#include "hash_utils.h"

typedef struct SentenceArenaBlock {
  uint8_t *data;
  size_t cap;
  size_t offset;
  struct SentenceArenaBlock *next;
} SentenceArenaBlock;

typedef struct SentenceSetShard {
  uint64_t *hashes;
  size_t *lengths;
  char8_t **data;
  uint8_t *ctrl; // 0xFF = empty, otherwise robin-hood probe distance
  size_t bucket_count;
  size_t entry_count;
  SentenceArena arena;
  mtx_t lock;
  bool lock_init;
} SentenceSetShard;

static constexpr size_t MIN_BUCKET_COUNT = 16;
static constexpr size_t DEFAULT_BLOCK_SIZE = 1024;
static constexpr size_t AVG_SENTENCE_BYTES = 64;
static constexpr uint8_t CTRL_EMPTY = 0xFF;
static constexpr size_t LOAD_FACTOR_NUM = 85;
static constexpr size_t LOAD_FACTOR_DEN = 100;
static constexpr size_t DEFAULT_SHARD_COUNT = 16;

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

static size_t choose_shard_count(size_t bucket_count) {
  if (bucket_count < MIN_BUCKET_COUNT)
    bucket_count = MIN_BUCKET_COUNT;
  size_t shards = DEFAULT_SHARD_COUNT;
  while (shards > 1 && bucket_count / shards < MIN_BUCKET_COUNT) {
    shards >>= 1;
  }
  return shards == 0 ? 1 : shards;
}

static size_t shard_index(const SentenceSet *set, uint64_t hash) {
  if (!set || set->shard_mask == 0)
    return 0;
  constexpr unsigned int HIGH_SHIFT = 48u;
  return (size_t)((hash >> HIGH_SHIFT) & set->shard_mask);
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

static char8_t *sentence_set_copy_data(SentenceSetShard *shard,
                                       const char8_t *data, size_t len) {
  size_t alloc_size = 0;
  if (ckd_add(&alloc_size, len, (size_t)1))
    return nullptr;
  char8_t *copy = (char8_t *)sentence_arena_alloc(&shard->arena, alloc_size);
  if (!copy)
    return nullptr;
  if (len > 0)
    memcpy(copy, data, len);
  copy[len] = (char8_t)'\0';
  return copy;
}

static void shard_destroy(SentenceSetShard *shard) {
  if (!shard)
    return;
  if (shard->lock_init) {
    mtx_destroy(&shard->lock);
    shard->lock_init = false;
  }
  sentence_arena_destroy(&shard->arena);
  free(shard->hashes);
  free(shard->lengths);
  free(shard->data);
  free(shard->ctrl);
  shard->hashes = nullptr;
  shard->lengths = nullptr;
  shard->data = nullptr;
  shard->ctrl = nullptr;
  shard->bucket_count = 0;
  shard->entry_count = 0;
}

static bool shard_init(SentenceSetShard *shard, size_t bucket_count) {
  if (!shard)
    return false;
  sentence_arena_init(&shard->arena, SENTENCE_ARENA_BLOCK_SIZE);
  size_t size = round_up_pow2(bucket_count < MIN_BUCKET_COUNT ? MIN_BUCKET_COUNT
                                                              : bucket_count);
  size_t alloc_hashes = 0;
  size_t alloc_lengths = 0;
  size_t alloc_data = 0;
  size_t alloc_ctrl = 0;
  if (ckd_mul(&alloc_hashes, size, sizeof(*shard->hashes)))
    return false;
  if (ckd_mul(&alloc_lengths, size, sizeof(*shard->lengths)))
    return false;
  if (ckd_mul(&alloc_data, size, sizeof(*shard->data)))
    return false;
  if (ckd_mul(&alloc_ctrl, size, sizeof(*shard->ctrl)))
    return false;

  shard->hashes = (uint64_t *)calloc(1, alloc_hashes);
  shard->lengths = (size_t *)calloc(1, alloc_lengths);
  shard->data = (char8_t **)calloc(1, alloc_data);
  shard->ctrl = (uint8_t *)calloc(1, alloc_ctrl);
  if (!shard->hashes || !shard->lengths || !shard->data || !shard->ctrl) {
    shard_destroy(shard);
    return false;
  }
  memset(shard->ctrl, CTRL_EMPTY, alloc_ctrl);
  shard->bucket_count = size;
  shard->entry_count = 0;
  shard->lock_init = mtx_init(&shard->lock, mtx_plain) == thrd_success;
  if (!shard->lock_init) {
    shard_destroy(shard);
    return false;
  }
  return true;
}

static void shard_clear(SentenceSetShard *shard) {
  if (!shard || shard->bucket_count == 0)
    return;
  memset(shard->ctrl, CTRL_EMPTY, shard->bucket_count * sizeof(uint8_t));
  shard->entry_count = 0;
  sentence_arena_reset(&shard->arena);
}

static bool rehash_insert(uint64_t hash, size_t len, char8_t *data,
                          uint64_t *hashes, size_t *lengths, char8_t **items,
                          uint8_t *ctrl, size_t bucket_count) {
  size_t idx = hash & (bucket_count - 1);
  uint8_t dist = 0;

  while (true) {
    uint8_t slot_ctrl = ctrl[idx];
    if (slot_ctrl == CTRL_EMPTY) {
      hashes[idx] = hash;
      lengths[idx] = len;
      items[idx] = data;
      ctrl[idx] = dist;
      return true;
    }

    if (hashes[idx] == hash && lengths[idx] == len &&
        memcmp(items[idx], data, len) == 0) {
      return true;
    }

    if (slot_ctrl < dist) {
      uint64_t displaced_hash = hashes[idx];
      size_t displaced_len = lengths[idx];
      char8_t *displaced_data = items[idx];
      uint8_t displaced_ctrl = slot_ctrl;

      hashes[idx] = hash;
      lengths[idx] = len;
      items[idx] = data;
      ctrl[idx] = dist;

      hash = displaced_hash;
      len = displaced_len;
      data = displaced_data;
      dist = displaced_ctrl + 1;
      idx = (idx + 1) & (bucket_count - 1);
      continue;
    }

    dist++;
    idx = (idx + 1) & (bucket_count - 1);
    if (dist == CTRL_EMPTY) {
      return false;
    }
  }
}

static bool sentence_set_rehash_shard(SentenceSetShard *shard,
                                      size_t new_bucket_count) {
  if (!shard)
    return false;
  size_t size =
      round_up_pow2(new_bucket_count < MIN_BUCKET_COUNT ? MIN_BUCKET_COUNT
                                                        : new_bucket_count);
  size_t alloc_hashes = 0;
  size_t alloc_lengths = 0;
  size_t alloc_data = 0;
  size_t alloc_ctrl = 0;
  if (ckd_mul(&alloc_hashes, size, sizeof(*shard->hashes)))
    return false;
  if (ckd_mul(&alloc_lengths, size, sizeof(*shard->lengths)))
    return false;
  if (ckd_mul(&alloc_data, size, sizeof(*shard->data)))
    return false;
  if (ckd_mul(&alloc_ctrl, size, sizeof(*shard->ctrl)))
    return false;

  auto new_hashes = (uint64_t *)calloc(1, alloc_hashes);
  auto new_lengths = (size_t *)calloc(1, alloc_lengths);
  auto new_data = (char8_t **)calloc(1, alloc_data);
  auto new_ctrl = (uint8_t *)calloc(1, alloc_ctrl);
  if (!new_hashes || !new_lengths || !new_data || !new_ctrl) {
    free(new_hashes);
    free(new_lengths);
    free(new_data);
    free(new_ctrl);
    return false;
  }
  memset(new_ctrl, CTRL_EMPTY, alloc_ctrl);

  for (size_t i = 0; i < shard->bucket_count; ++i) {
    if (shard->ctrl[i] == CTRL_EMPTY)
      continue;
    if (!rehash_insert(shard->hashes[i], shard->lengths[i], shard->data[i],
                       new_hashes, new_lengths, new_data, new_ctrl, size)) {
      free(new_hashes);
      free(new_lengths);
      free(new_data);
      free(new_ctrl);
      return false;
    }
  }

  free(shard->hashes);
  free(shard->lengths);
  free(shard->data);
  free(shard->ctrl);

  shard->hashes = new_hashes;
  shard->lengths = new_lengths;
  shard->data = new_data;
  shard->ctrl = new_ctrl;
  shard->bucket_count = size;
  // entry_count unchanged.
  return true;
}

[[nodiscard]] static bool
sentence_set_insert_internal(SentenceSetShard *shard, uint64_t hash,
                             const char8_t *data, size_t len, bool data_owned,
                             bool *inserted) {
  if (!shard || !data || !inserted)
    return false;

  size_t idx = hash & (shard->bucket_count - 1);
  uint8_t dist = 0;

  uint64_t cand_hash = hash;
  size_t cand_len = len;
  const char8_t *cand_data = data;
  bool cand_owned = data_owned;

  while (true) {
    uint8_t ctrl = shard->ctrl[idx];
    if (ctrl == CTRL_EMPTY) {
      char8_t *stored =
          cand_owned ? (char8_t *)cand_data
                     : sentence_set_copy_data(shard, cand_data, cand_len);
      if (!stored)
        return false;
      shard->hashes[idx] = cand_hash;
      shard->lengths[idx] = cand_len;
      shard->data[idx] = stored;
      shard->ctrl[idx] = dist;
      shard->entry_count++;
      *inserted = true;
      return true;
    }

    if (shard->hashes[idx] == cand_hash && shard->lengths[idx] == cand_len &&
        memcmp(shard->data[idx], cand_data, cand_len) == 0) {
      *inserted = false;
      return true;
    }

    if (ctrl < dist) {
      uint64_t displaced_hash = shard->hashes[idx];
      size_t displaced_len = shard->lengths[idx];
      char8_t *displaced_data = shard->data[idx];
      uint8_t displaced_ctrl = ctrl;

      char8_t *stored =
          cand_owned ? (char8_t *)cand_data
                     : sentence_set_copy_data(shard, cand_data, cand_len);
      if (!stored)
        return false;
      shard->hashes[idx] = cand_hash;
      shard->lengths[idx] = cand_len;
      shard->data[idx] = stored;
      shard->ctrl[idx] = dist;

      cand_hash = displaced_hash;
      cand_len = displaced_len;
      cand_data = displaced_data;
      cand_owned = true;
      dist = displaced_ctrl + 1;
      idx = (idx + 1) & (shard->bucket_count - 1);
      continue;
    }

    dist++;
    idx = (idx + 1) & (shard->bucket_count - 1);
    if (dist == CTRL_EMPTY) {
      size_t next_size = 0;
      if (ckd_mul(&next_size, shard->bucket_count, (size_t)2))
        return false;
      if (!sentence_set_rehash_shard(shard, next_size))
        return false;
      return sentence_set_insert_internal(shard, hash, data, len, data_owned,
                                          inserted);
    }
  }
}

bool sentence_set_init(SentenceSet *set, size_t bucket_count) {
  if (!set)
    return false;
  *set = (SentenceSet){0};
  size_t shards = choose_shard_count(bucket_count);
  set->shard_count = shards;
  set->shard_mask = shards - 1;

  set->shards = (SentenceSetShard *)calloc(shards, sizeof(SentenceSetShard));
  if (!set->shards)
    return false;

  size_t per_shard = bucket_count / shards;
  if (per_shard < MIN_BUCKET_COUNT)
    per_shard = MIN_BUCKET_COUNT;
  per_shard = round_up_pow2(per_shard);

  for (size_t i = 0; i < shards; ++i) {
    if (!shard_init(&set->shards[i], per_shard)) {
      for (size_t j = 0; j < i; ++j) {
        shard_destroy(&set->shards[j]);
      }
      free(set->shards);
      *set = (SentenceSet){0};
      return false;
    }
  }
  return true;
}

void sentence_set_destroy(SentenceSet *set) {
  if (!set)
    return;
  if (set->shards) {
    for (size_t i = 0; i < set->shard_count; ++i) {
      shard_destroy(&set->shards[i]);
    }
    free(set->shards);
  }
  set->shards = nullptr;
  set->shard_count = 0;
  set->shard_mask = 0;
}

void sentence_set_clear(SentenceSet *set) {
  if (!set || !set->shards)
    return;
  for (size_t i = 0; i < set->shard_count; ++i) {
    shard_clear(&set->shards[i]);
  }
}

void sentence_set_reserve_for_bytes(SentenceSet *set, size_t byte_len) {
  if (!set || !set->shards || set->shard_count == 0)
    return;
  size_t expected = byte_len / AVG_SENTENCE_BYTES;
  if (expected < MIN_BUCKET_COUNT)
    expected = MIN_BUCKET_COUNT;

  size_t total_entries = 0;
  size_t total_buckets = 0;
  for (size_t i = 0; i < set->shard_count; ++i) {
    SentenceSetShard *shard = &set->shards[i];
    if (shard->lock_init)
      mtx_lock(&shard->lock);
    total_entries += shard->entry_count;
    total_buckets += shard->bucket_count;
    if (shard->lock_init)
      mtx_unlock(&shard->lock);
  }

  size_t target = 0;
  if (ckd_add(&target, total_entries, expected)) {
    target = SIZE_MAX;
  }
  size_t needed = 0;
  size_t scaled = 0;
  if (ckd_mul(&scaled, target, (size_t)5)) {
    needed = SIZE_MAX;
  } else {
    needed = scaled / 4;
  }

  if (needed <= total_buckets)
    return;

  size_t per_needed = needed / set->shard_count;
  if (per_needed < MIN_BUCKET_COUNT)
    per_needed = MIN_BUCKET_COUNT;
  per_needed = round_up_pow2(per_needed);

  for (size_t i = 0; i < set->shard_count; ++i) {
    SentenceSetShard *shard = &set->shards[i];
    if (shard->lock_init)
      mtx_lock(&shard->lock);
    if (per_needed > shard->bucket_count) {
      (void)sentence_set_rehash_shard(shard, per_needed);
    }
    if (shard->lock_init)
      mtx_unlock(&shard->lock);
  }
}

[[nodiscard]] bool sentence_set_insert_hashed(SentenceSet *set, uint64_t hash,
                                              const char8_t *data, size_t len,
                                              bool *inserted) {
  if (!set || !data || !inserted)
    return false;
  if (!set->shards || set->shard_count == 0) {
    if (!sentence_set_init(set, 1024))
      return false;
  }

  size_t shard_idx = shard_index(set, hash);
  SentenceSetShard *shard = &set->shards[shard_idx];

  if (shard->lock_init)
    mtx_lock(&shard->lock);

  size_t threshold_num = shard->bucket_count * LOAD_FACTOR_NUM;
  size_t threshold_den = LOAD_FACTOR_DEN;
  size_t threshold = threshold_num / threshold_den;
  if (threshold == 0)
    threshold = 1;
  if (shard->entry_count + 1 > threshold) {
    size_t next_size = shard->bucket_count * 2;
    (void)sentence_set_rehash_shard(shard, next_size);
  }

  bool ok =
      sentence_set_insert_internal(shard, hash, data, len, false, inserted);

  if (shard->lock_init)
    mtx_unlock(&shard->lock);
  return ok;
}

[[nodiscard]] bool sentence_set_insert(SentenceSet *set, const char8_t *data,
                                       size_t len, bool *inserted) {
  uint64_t hash = hash_bytes_fnv1a(data, len);
  return sentence_set_insert_hashed(set, hash, data, len, inserted);
}
