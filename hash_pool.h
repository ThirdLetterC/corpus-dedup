#ifndef HASH_POOL_H
#define HASH_POOL_H

#include <stddef.h>
#include <stdint.h>

struct BlockNode;

typedef struct {
  struct BlockNode **nodes;
  size_t start_idx;
  size_t end_idx;
  const uint32_t *text;
  size_t text_len;
} ThreadContext;

typedef struct HashThreadPool HashThreadPool;

/**
 * Acquire (or reuse) a thread pool sized for thread_count workers.
 * Returns nullptr when thread_count <= 1 or allocation fails.
 */
[[nodiscard]] HashThreadPool *hash_pool_get(size_t thread_count);

size_t hash_pool_capacity(const HashThreadPool *pool);

/**
 * Submit work to the pool. contexts must have active_count entries.
 */
bool hash_pool_run(HashThreadPool *pool, const ThreadContext *contexts,
                   size_t active_count);

/**
 * Destroy global pool at process exit (registered via atexit).
 */
void hash_pool_global_cleanup();

#endif
