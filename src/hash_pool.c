#include "hash_pool.h"

#include <stdlib.h>
#include <threads.h>

#ifndef HASH_WORKER_USE_ASM
#define HASH_WORKER_USE_ASM 0
#endif

typedef struct HashWorkerArg {
  struct HashThreadPool *pool;
  size_t index;
} HashWorkerArg;

struct HashThreadPool {
  thrd_t *threads;
  ThreadContext *contexts;
  HashWorkerArg *args;
  size_t thread_count;
  size_t active_count;
  size_t pending;
  uint64_t work_id;
  mtx_t lock;
  cnd_t start_cv;
  cnd_t done_cv;
  bool shutdown;
};

static HashThreadPool g_hash_pool = {0};
static bool g_hash_pool_registered = false;

int hash_worker(void *arg); // Provided by block_tree.c or asm implementation.

static int hash_pool_worker(void *arg) {
  HashWorkerArg *worker = (HashWorkerArg *)arg;
  HashThreadPool *pool = worker->pool;
  size_t index = worker->index;
  uint64_t last_work = 0;

  mtx_lock(&pool->lock);
  for (;;) {
    while (!pool->shutdown && pool->work_id == last_work) {
      cnd_wait(&pool->start_cv, &pool->lock);
    }
    if (pool->shutdown) {
      mtx_unlock(&pool->lock);
      return 0;
    }
    last_work = pool->work_id;

    bool active = index < pool->active_count;
    ThreadContext *ctx = active ? &pool->contexts[index] : nullptr;

    mtx_unlock(&pool->lock);
    if (active && ctx->start_idx < ctx->end_idx) {
      hash_worker(ctx);
    }
    mtx_lock(&pool->lock);

    if (active && pool->pending > 0) {
      pool->pending--;
      if (pool->pending == 0) {
        cnd_signal(&pool->done_cv);
      }
    }
  }
}

static void hash_pool_destroy(HashThreadPool *pool) {
  if (!pool || !pool->threads)
    return;

  mtx_lock(&pool->lock);
  pool->shutdown = true;
  pool->work_id++;
  cnd_broadcast(&pool->start_cv);
  mtx_unlock(&pool->lock);

  for (size_t i = 0; i < pool->thread_count; ++i) {
    thrd_join(pool->threads[i], nullptr);
  }

  cnd_destroy(&pool->done_cv);
  cnd_destroy(&pool->start_cv);
  mtx_destroy(&pool->lock);

  free(pool->threads);
  free(pool->contexts);
  free(pool->args);
  *pool = (HashThreadPool){0};
}

void hash_pool_global_cleanup() { hash_pool_destroy(&g_hash_pool); }

static bool hash_pool_init(HashThreadPool *pool, size_t thread_count) {
  if (!pool || thread_count == 0)
    return false;
  *pool = (HashThreadPool){0};

  pool->threads = calloc(thread_count, sizeof(*pool->threads));
  pool->contexts = calloc(thread_count, sizeof(*pool->contexts));
  pool->args = calloc(thread_count, sizeof(*pool->args));
  if (!pool->threads || !pool->contexts || !pool->args) {
    goto fail;
  }
  if (mtx_init(&pool->lock, mtx_plain) != thrd_success) {
    goto fail;
  }
  if (cnd_init(&pool->start_cv) != thrd_success) {
    goto fail_lock;
  }
  if (cnd_init(&pool->done_cv) != thrd_success) {
    goto fail_start;
  }

  pool->thread_count = thread_count;
  pool->active_count = 0;
  pool->pending = 0;
  pool->work_id = 0;
  pool->shutdown = false;

  size_t created = 0;
  for (; created < thread_count; ++created) {
    pool->args[created] = (HashWorkerArg){.pool = pool, .index = created};
    if (thrd_create(&pool->threads[created], hash_pool_worker,
                    &pool->args[created]) != thrd_success) {
      goto fail_spawn;
    }
  }
  return true;

fail_spawn:
  mtx_lock(&pool->lock);
  pool->shutdown = true;
  pool->work_id++;
  cnd_broadcast(&pool->start_cv);
  mtx_unlock(&pool->lock);
  for (size_t i = 0; i < created; ++i) {
    thrd_join(pool->threads[i], nullptr);
  }
  cnd_destroy(&pool->done_cv);
fail_start:
  cnd_destroy(&pool->start_cv);
fail_lock:
  mtx_destroy(&pool->lock);
fail:
  free(pool->threads);
  free(pool->contexts);
  free(pool->args);
  *pool = (HashThreadPool){0};
  return false;
}

HashThreadPool *hash_pool_get(size_t thread_count) {
  if (thread_count <= 1)
    return nullptr;
  if (g_hash_pool.threads && g_hash_pool.thread_count == thread_count) {
    return &g_hash_pool;
  }

  hash_pool_destroy(&g_hash_pool);
  if (!hash_pool_init(&g_hash_pool, thread_count)) {
    return nullptr;
  }
  if (!g_hash_pool_registered) {
    atexit(hash_pool_global_cleanup);
    g_hash_pool_registered = true;
  }
  return &g_hash_pool;
}

size_t hash_pool_capacity(const HashThreadPool *pool) {
  return pool ? pool->thread_count : 0;
}

bool hash_pool_run(HashThreadPool *pool, const ThreadContext *contexts,
                   size_t active_count) {
  if (!pool || !contexts || active_count == 0 ||
      active_count > pool->thread_count)
    return false;

  mtx_lock(&pool->lock);
  pool->active_count = active_count;
  pool->pending = active_count;
  for (size_t i = 0; i < active_count; ++i) {
    pool->contexts[i] = contexts[i];
  }
  pool->work_id++;
  cnd_broadcast(&pool->start_cv);
  while (pool->pending > 0) {
    cnd_wait(&pool->done_cv, &pool->lock);
  }
  mtx_unlock(&pool->lock);
  return true;
}
