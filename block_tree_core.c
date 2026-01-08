#include "block_tree.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <threads.h>
#include <unistd.h>
#include "ckdint_compat.h"

#include "arena.h"
#include "block_tree_asm_defs.h"
#include "config.h"
#include "hash_pool.h"
#include "node_sort.h"

#ifndef HASH_WORKER_USE_ASM
#define HASH_WORKER_USE_ASM 0
#endif

#define HASH_MULT_POW1 ((uint64_t)HASH_MULT_POW1_IMM)
#define HASH_MULT_POW2 ((uint64_t)HASH_MULT_POW2_IMM)
#define HASH_MULT_POW3 ((uint64_t)HASH_MULT_POW3_IMM)
#define HASH_MULT_POW4 ((uint64_t)HASH_MULT_POW4_IMM)

static size_t parse_thread_env(void) {
  const char *env = getenv("BLOCK_TREE_THREADS");
  if (!env || !*env)
    return 0;
  char *end = NULL;
  long val = strtol(env, &end, 10);
  if (end == env || val <= 0 || val > 1024)
    return 0;
  return (size_t)val;
}

static size_t detect_thread_count(void) {
  size_t env_threads = parse_thread_env();
  if (env_threads > 0)
    return env_threads;
#if defined(_SC_NPROCESSORS_ONLN)
  long n = sysconf(_SC_NPROCESSORS_ONLN);
  if (n > 0)
    return (size_t)n;
#endif
  return THREAD_COUNT_FALLBACK;
}

#if HASH_WORKER_USE_ASM
static_assert(offsetof(ThreadContext, nodes) == CTX_NODES,
              "ThreadContext layout changed");
static_assert(offsetof(ThreadContext, start_idx) == CTX_START_IDX,
              "ThreadContext layout changed");
static_assert(offsetof(ThreadContext, end_idx) == CTX_END_IDX,
              "ThreadContext layout changed");
static_assert(offsetof(ThreadContext, text) == CTX_TEXT,
              "ThreadContext layout changed");
static_assert(offsetof(ThreadContext, text_len) == CTX_TEXT_LEN,
              "ThreadContext layout changed");

int hash_worker(void *arg);
#else
int hash_worker(void *arg) {
  ThreadContext *ctx = (ThreadContext *)arg;

  for (size_t i = ctx->start_idx; i < ctx->end_idx; ++i) {
    BlockNode *node = ctx->nodes[i];

    if (node->start_pos >= ctx->text_len) {
      node->block_id = 0;
      continue;
    }

    size_t effective_len = node->length;
    if (node->start_pos + effective_len > ctx->text_len) {
      effective_len = ctx->text_len - node->start_pos;
    }

    uint64_t h = 0;
    const uint32_t *data = ctx->text + node->start_pos;

    size_t j = 0;
#if HASH_UNROLL == 8
    size_t limit = effective_len & ~(size_t)7;
    for (; j < limit; j += 8) {
#if HASH_PREFETCH_DISTANCE
      __builtin_prefetch(data + j + (HASH_PREFETCH_DISTANCE / sizeof(uint32_t)),
                         0, 3);
#endif
      uint64_t t0 = (uint64_t)data[j] * HASH_MULT_POW3 +
                    (uint64_t)data[j + 1] * HASH_MULT_POW2 +
                    (uint64_t)data[j + 2] * HASH_MULT_POW1 +
                    (uint64_t)data[j + 3];
      uint64_t t1 = (uint64_t)data[j + 4] * HASH_MULT_POW3 +
                    (uint64_t)data[j + 5] * HASH_MULT_POW2 +
                    (uint64_t)data[j + 6] * HASH_MULT_POW1 +
                    (uint64_t)data[j + 7];
      h = h * HASH_MULT_POW4 + t0;
      h = h * HASH_MULT_POW4 + t1;
    }
#else
    size_t limit = effective_len & ~(size_t)3;
    for (; j < limit; j += 4) {
#if HASH_PREFETCH_DISTANCE
      __builtin_prefetch(data + j + (HASH_PREFETCH_DISTANCE / sizeof(uint32_t)),
                         0, 3);
#endif
      uint64_t t0 = (uint64_t)data[j] * HASH_MULT_POW3 +
                    (uint64_t)data[j + 1] * HASH_MULT_POW2 +
                    (uint64_t)data[j + 2] * HASH_MULT_POW1 +
                    (uint64_t)data[j + 3];
      h = h * HASH_MULT_POW4 + t0;
    }
#endif
    for (; j < effective_len; ++j) {
      h = h * HASH_MULT + (uint64_t)data[j];
    }

    node->block_id = h;
  }
  return 0;
}
#endif

void compute_hashes_parallel(BlockNode **candidates, size_t count,
                             const uint32_t *text, size_t len) {
  if (count == 0)
    return;

  size_t thread_count = detect_thread_count();
  if (thread_count == 0)
    thread_count = 1;

  if (thread_count <= 1) {
    ThreadContext ctx = {.nodes = candidates,
                         .start_idx = 0,
                         .end_idx = count,
                         .text = text,
                         .text_len = len};
    hash_worker(&ctx);
    return;
  }

  size_t threshold = HASH_PARALLEL_BASE * thread_count;
  if (count < threshold) {
    ThreadContext ctx = {.nodes = candidates,
                         .start_idx = 0,
                         .end_idx = count,
                         .text = text,
                         .text_len = len};
    hash_worker(&ctx);
    return;
  }

  HashThreadPool *pool = hash_pool_get(thread_count);
  if (!pool) {
    ThreadContext ctx = {.nodes = candidates,
                         .start_idx = 0,
                         .end_idx = count,
                         .text = text,
                         .text_len = len};
    hash_worker(&ctx);
    return;
  }

  size_t active = hash_pool_capacity(pool);
  if (active > count)
    active = count;
  size_t chunk_size = (count + active - 1) / active;

  ThreadContext *ctxs = malloc(active * sizeof(*ctxs));
  if (!ctxs) {
    ThreadContext ctx = {.nodes = candidates,
                         .start_idx = 0,
                         .end_idx = count,
                         .text = text,
                         .text_len = len};
    hash_worker(&ctx);
    return;
  }

  for (size_t i = 0; i < active; ++i) {
    size_t start = i * chunk_size;
    size_t end = start + chunk_size;
    if (start >= count)
      start = count;
    if (end > count)
      end = count;

    ctxs[i] = (ThreadContext){.nodes = candidates,
                              .start_idx = start,
                              .end_idx = end,
                              .text = text,
                              .text_len = len};
  }

  if (!hash_pool_run(pool, ctxs, active)) {
    ThreadContext ctx = {.nodes = candidates,
                         .start_idx = 0,
                         .end_idx = count,
                         .text = text,
                         .text_len = len};
    hash_worker(&ctx);
  }
  free(ctxs);
}

static bool blocks_equal(const BlockNode *a, const BlockNode *b,
                         const uint32_t *text) {
  if (a->length != b->length)
    return false;
  return memcmp(text + a->start_pos, text + b->start_pos,
                a->length * sizeof(uint32_t)) == 0;
}

static bool ensure_ptr_capacity(BlockNode ***buffer, size_t *cap,
                                size_t needed) {
  if (*cap >= needed)
    return true;
  size_t new_cap = *cap ? *cap : 16;
  while (new_cap < needed) {
    size_t next_cap = 0;
    if (ckd_mul(&next_cap, new_cap, (size_t)2))
      return false;
    new_cap = next_cap;
  }
  size_t alloc_size = 0;
  if (ckd_mul(&alloc_size, new_cap, sizeof(**buffer)))
    return false;
  BlockNode **next = realloc(*buffer, alloc_size);
  if (!next)
    return false;
  *buffer = next;
  *cap = new_cap;
  return true;
}

void deduplicate_level(BlockNode **candidates, size_t count,
                       const uint32_t *text, BlockNode **next_marked,
                       size_t next_cap, size_t *out_marked_count) {
  if (count == 0 || !next_marked || next_cap < count) {
    if (out_marked_count)
      *out_marked_count = 0;
    return;
  }

  if (!radix_sort_block_nodes(candidates, next_marked, count)) {
    if (out_marked_count)
      *out_marked_count = 0;
    return;
  }

  size_t marked_idx = 0;

  BlockNode *leader = candidates[0];
  leader->is_marked = true;
  next_marked[marked_idx++] = leader;
  size_t group_start = 0;

  for (size_t i = 1; i < count; ++i) {
    BlockNode *curr = candidates[i];

    if (curr->block_id != leader->block_id || curr->length != leader->length) {
      leader = curr;
      leader->is_marked = true;
      next_marked[marked_idx++] = leader;
      group_start = marked_idx - 1;
      continue;
    }

    bool matched = false;
    for (size_t j = group_start; j < marked_idx; ++j) {
      BlockNode *candidate = next_marked[j];
      if (candidate->block_id != curr->block_id)
        continue;
      if (blocks_equal(curr, candidate, text)) {
        curr->is_marked = false;
        curr->target_pos = candidate->start_pos;
        matched = true;
        break;
      }
    }

    if (!matched) {
      curr->is_marked = true;
      next_marked[marked_idx++] = curr;
    }
  }

  *out_marked_count = marked_idx;
}

BlockNode *create_node(Arena *arena, size_t start, size_t len, int level,
                       BlockNode *parent) {
  BlockNode *n = arena_alloc(arena, sizeof(BlockNode));
  n->start_pos = start;
  n->length = len;
  n->level = level;
  n->parent = parent;
  n->is_marked = false;
  n->target_pos = 0;
  n->children = NULL;
  n->child_count = 0;
  n->block_id = 0;
  return n;
}

BlockNode *build_block_tree(const uint32_t *text, size_t len, int s, int tau,
                            Arena *arena) {
  if (!arena)
    return NULL;

  BlockNode *root = create_node(arena, 0, len, 0, NULL);
  root->is_marked = true;

  BlockNode **current_marked = malloc(sizeof(*current_marked));
  if (!current_marked)
    return NULL;
  BlockNode **next_marked = NULL;
  BlockNode **candidates = NULL;
  size_t current_cap = 1;
  size_t next_cap = 0;
  size_t cand_cap = 0;

  current_marked[0] = root;
  size_t current_count = 1;

  for (int level = 1;; ++level) {
    if (current_count == 0)
      break;

    size_t divisor = (size_t)((level == 1) ? s : tau);
    if (divisor == 0)
      divisor = 1;

    size_t cand_idx = 0;

    for (size_t i = 0; i < current_count; ++i) {
      BlockNode *p = current_marked[i];
      size_t max_len = p->length;
      if (p->start_pos >= len)
        continue;
      if (p->start_pos + max_len > len)
        max_len = len - p->start_pos;
      if (max_len <= 1)
        continue;

      size_t step = max_len / divisor;
      if (step == 0)
        step = 1;

      size_t num_children =
          (step == 1) ? (max_len < divisor ? max_len : divisor) : divisor;

      p->children = arena_alloc(arena, num_children * sizeof(BlockNode *));
      p->child_count = 0;

      if (!ensure_ptr_capacity(&candidates, &cand_cap,
                               cand_idx + num_children)) {
        free(current_marked);
        free(next_marked);
        free(candidates);
        return NULL;
      }

      for (size_t k = 0; k < num_children; ++k) {
        size_t cStart = p->start_pos + k * step;
        size_t cEnd = cStart + step;

        if (k == num_children - 1) {
          cEnd = p->start_pos + max_len;
        }

        if (cStart >= len)
          break;
        if (cStart >= cEnd)
          break;
        if (cEnd > len)
          cEnd = len;

        BlockNode *child = create_node(arena, cStart, cEnd - cStart, level, p);
        candidates[cand_idx++] = child;

        p->children[p->child_count++] = child;
      }
    }

    if (cand_idx == 0) {
      break;
    }

    if (!ensure_ptr_capacity(&next_marked, &next_cap, cand_idx)) {
      free(current_marked);
      free(next_marked);
      free(candidates);
      return NULL;
    }

    compute_hashes_parallel(candidates, cand_idx, text, len);

    size_t next_count = 0;
    deduplicate_level(candidates, cand_idx, text, next_marked, next_cap,
                      &next_count);

    BlockNode **swap = current_marked;
    current_marked = next_marked;
    next_marked = swap;
    size_t swap_cap = current_cap;
    current_cap = next_cap;
    next_cap = swap_cap;
    current_count = next_count;
  }

  free(current_marked);
  free(next_marked);
  free(candidates);

  return root;
}

void print_tree(const BlockNode *node, int depth) {
  if (!node || depth > 3)
    return;

  for (int i = 0; i < depth; ++i)
    printf("  ");

  if (node->is_marked) {
    printf("[M] Hash:%lX Pos:%zu Len:%zu\n", node->block_id, node->start_pos,
           node->length);

    for (size_t i = 0; i < node->child_count; ++i) {
      print_tree(node->children[i], depth + 1);
    }
  } else {
    printf("[P] -> Target:%zu (Hash:%lX)\n", node->target_pos, node->block_id);
  }
}

uint32_t query_access(const BlockNode *node, size_t i, const uint32_t *text) {
  if (!node->is_marked) {
    size_t offset = i - node->start_pos;
    size_t target_global = node->target_pos + offset;
    return text[target_global];
  }

  if (node->child_count == 0) {
    return text[i];
  }

  for (size_t k = 0; k < node->child_count; ++k) {
    BlockNode *child = node->children[k];
    if (i >= child->start_pos && i < child->start_pos + child->length) {
      return query_access(child, i, text);
    }
  }
  return (uint32_t)'?';
}
