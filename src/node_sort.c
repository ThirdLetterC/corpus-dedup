#include <stdlib.h>
#include <string.h>
#include <threads.h>

#include "node_sort.h"
#include "block_tree_asm_defs.h"
#include "ckdint_compat.h"
#include "config.h"

#ifndef WAVESORT_USE_ASM
#if defined(__AVX2__) && defined(__x86_64__) &&                                \
    (defined(__GNUC__) || defined(__clang__))
#define WAVESORT_USE_ASM 1
#else
#define WAVESORT_USE_ASM 0
#endif
#endif

#ifndef RADIX_SORT_USE_ASM
#define RADIX_SORT_USE_ASM 0
#endif
#ifndef RADIX_SORT_USE_ASM_IMPL
#define RADIX_SORT_USE_ASM_IMPL RADIX_SORT_USE_ASM
#endif
#ifndef WAVESORT_USE_ASM
#define WAVESORT_USE_ASM 0
#endif

#if RADIX_SORT_USE_ASM_IMPL
static_assert(offsetof(BlockNode, length) == RADIX_NODE_LENGTH_OFFSET,
              "BlockNode layout changed");
static_assert(offsetof(BlockNode, block_id) == RADIX_NODE_BLOCK_ID_OFFSET,
              "BlockNode layout changed");
#endif

#if WAVESORT_USE_ASM
static inline int32_t wavesort_block_id_key(uint64_t block_id) {
  // Flip the sign bit so signed int32 sorting matches unsigned order.
  return (int32_t)((uint32_t)block_id ^ 0x80000000u);
}

// Optional ASM wave_sort from asm/wavesort.asm; weak to keep builds working.
extern void wave_sort(int32_t *arr, size_t n) __attribute__((weak));
#endif

int compare_node_ptr(const BlockNode *nodeA, const BlockNode *nodeB) {
  if (nodeA->block_id < nodeB->block_id)
    return -1;
  if (nodeA->block_id > nodeB->block_id)
    return 1;

  if (nodeA->length < nodeB->length)
    return -1;
  if (nodeA->length > nodeB->length)
    return 1;

  if (nodeA->start_pos < nodeB->start_pos)
    return -1;
  if (nodeA->start_pos > nodeB->start_pos)
    return 1;

  return 0;
}

int compare_nodes(const void *a, const void *b) {
  const BlockNode *const *nodeA = a;
  const BlockNode *const *nodeB = b;
  return compare_node_ptr(*nodeA, *nodeB);
}

// ==========================================
// Radix workspace (reuse buffers to avoid per-call malloc)
// ==========================================

typedef struct {
  uint64_t *len_keys;
  uint64_t *hash_keys;
  uint64_t *len_tmp;
  uint64_t *hash_tmp;
  BlockNode **nodes_tmp;
  size_t cap;
  mtx_t lock;
  bool lock_init;
  bool registered;
} RadixWorkspace;

static RadixWorkspace g_radix_ws = {0};

static void free_radix_workspace(void) {
  if (g_radix_ws.lock_init) {
    mtx_destroy(&g_radix_ws.lock);
    g_radix_ws.lock_init = false;
  }
  free(g_radix_ws.len_keys);
  free(g_radix_ws.hash_keys);
  free(g_radix_ws.len_tmp);
  free(g_radix_ws.hash_tmp);
  free(g_radix_ws.nodes_tmp);
  g_radix_ws = (RadixWorkspace){0};
}

static bool radix_workspace_lock(void) {
  if (!g_radix_ws.lock_init) {
    if (mtx_init(&g_radix_ws.lock, mtx_plain) != thrd_success) {
      return false;
    }
    g_radix_ws.lock_init = true;
  }
  if (!g_radix_ws.registered) {
    atexit(free_radix_workspace);
    g_radix_ws.registered = true;
  }
  return mtx_lock(&g_radix_ws.lock) == thrd_success;
}

static void radix_workspace_unlock(void) {
  if (g_radix_ws.lock_init) {
    mtx_unlock(&g_radix_ws.lock);
  }
}

static bool ensure_radix_workspace(size_t count) {
  if (count <= g_radix_ws.cap) {
    return true;
  }

  size_t alloc_u64 = 0;
  size_t alloc_nodes = 0;
  if (ckd_mul(&alloc_u64, count, sizeof(uint64_t)))
    return false;
  if (ckd_mul(&alloc_nodes, count, sizeof(BlockNode *)))
    return false;

  uint64_t *len_keys = g_radix_ws.len_keys
                           ? realloc(g_radix_ws.len_keys, alloc_u64)
                           : malloc(alloc_u64);
  uint64_t *hash_keys = g_radix_ws.hash_keys
                            ? realloc(g_radix_ws.hash_keys, alloc_u64)
                            : malloc(alloc_u64);
  uint64_t *len_tmp = g_radix_ws.len_tmp
                          ? realloc(g_radix_ws.len_tmp, alloc_u64)
                          : malloc(alloc_u64);
  uint64_t *hash_tmp = g_radix_ws.hash_tmp
                           ? realloc(g_radix_ws.hash_tmp, alloc_u64)
                           : malloc(alloc_u64);
  BlockNode **nodes_tmp = g_radix_ws.nodes_tmp
                              ? realloc(g_radix_ws.nodes_tmp, alloc_nodes)
                              : malloc(alloc_nodes);

  if (!len_keys || !hash_keys || !len_tmp || !hash_tmp || !nodes_tmp) {
    return false;
  }

  g_radix_ws.len_keys = len_keys;
  g_radix_ws.hash_keys = hash_keys;
  g_radix_ws.len_tmp = len_tmp;
  g_radix_ws.hash_tmp = hash_tmp;
  g_radix_ws.nodes_tmp = nodes_tmp;
  g_radix_ws.cap = count;
  return true;
}

// ==========================================
// WaveSort helpers
// ==========================================

static inline void wavesort_swap_nodes(BlockNode **a, BlockNode **b) {
  BlockNode *tmp = *a;
  *a = *b;
  *b = tmp;
}

static void wavesort_block_swap_sl(BlockNode **restrict arr, size_t m, size_t p,
                                   size_t ll) {
  BlockNode *tmp = arr[m];
  size_t init = m;
  size_t j = m;
  size_t nm = p - ll + 1;
  size_t total_len = p - m + 1;

  for (size_t count = 0; count < total_len; count++) {
    if (j >= nm) {
      size_t k = j - nm + m;
      if (k == init) {
        init++;
        arr[j] = tmp;
        j = init;
        tmp = arr[j];
      } else {
        arr[j] = arr[k];
        j = k;
      }
    } else {
      size_t k = j + ll;
      arr[j] = arr[k];
      j = k;
    }
  }
}

static void wavesort_block_swap_sr(BlockNode **restrict arr, size_t m, size_t r,
                                   size_t p) {
  size_t i = m;
  BlockNode *tmp = arr[i];
  size_t j = r;
  while (j < p) {
    arr[i] = arr[j];
    i++;
    arr[j] = arr[i];
    j++;
  }
  arr[i] = arr[j];
  arr[j] = tmp;
}

static void wavesort_block_swap(BlockNode **restrict arr, size_t m, size_t r,
                                size_t p) {
  size_t ll = r - m;
  if (ll == 0) {
    return;
  }
  size_t lr = p - r + 1;
  if (lr == 1) {
    wavesort_swap_nodes(&arr[m], &arr[p]);
    return;
  }
  if (lr <= ll) {
    wavesort_block_swap_sr(arr, m, r, p);
  } else {
    wavesort_block_swap_sl(arr, m, p, ll);
  }
}

static size_t wavesort_partition(BlockNode **restrict arr, size_t l, size_t r,
                                 size_t p_idx) {
  const BlockNode *pivot_val = arr[p_idx];

  size_t i = l - 1;
  size_t j = r;

  while (true) {
    while (true) {
      i++;
      if (i == j) {
        return i;
      }
      if (compare_node_ptr(arr[i], pivot_val) >= 0) {
        break;
      }
    }
    while (true) {
      j--;
      if (j == i) {
        return i;
      }
      if (compare_node_ptr(arr[j], pivot_val) <= 0) {
        break;
      }
    }
    wavesort_swap_nodes(&arr[i], &arr[j]);
  }
}

static void wavesort_upwave(BlockNode **restrict arr, size_t start, size_t end);

static void wavesort_downwave(BlockNode **restrict arr, size_t start,
                              size_t sorted_start, size_t end) {
  if (sorted_start == start) {
    return;
  }

  size_t p = sorted_start + (end - sorted_start) / 2;
  size_t m = wavesort_partition(arr, start, sorted_start, p);

  if (m == sorted_start) {
    if (p == sorted_start) {
      if (sorted_start > 0) {
        wavesort_upwave(arr, start, sorted_start - 1);
      }
      return;
    }
    if (p > 0) {
      wavesort_downwave(arr, start, sorted_start, p - 1);
    }
    return;
  }

  wavesort_block_swap(arr, m, sorted_start, p);

  if (m == start) {
    if (p == sorted_start) {
      wavesort_upwave(arr, m + 1, end);
      return;
    }
    size_t p_next = p + 1;
    wavesort_downwave(arr, m + p_next - sorted_start, p_next, end);
    return;
  }

  if (p == sorted_start) {
    if (m > 0) {
      wavesort_upwave(arr, start, m - 1);
    }
    wavesort_upwave(arr, m + 1, end);
    return;
  }

  size_t right_part_len = p - sorted_start;
  size_t split_point = m + right_part_len;

  if (split_point > 0) {
    wavesort_downwave(arr, start, m, split_point - 1);
  }
  wavesort_downwave(arr, split_point + 1, p + 1, end);
}

static void wavesort_upwave(BlockNode **restrict arr, size_t start,
                            size_t end) {
  if (start == end) {
    return;
  }
  size_t sorted_start = end;
  size_t sorted_len = 1;

  if (end == 0) {
    return;
  }

  size_t left_bound = end - 1;
  size_t total_len = end - start + 1;

  while (true) {
    wavesort_downwave(arr, left_bound, sorted_start, end);
    sorted_start = left_bound;
    sorted_len = end - sorted_start + 1;

    if (total_len < (sorted_len << 2)) {
      break;
    }

    size_t next_expansion = (sorted_len << 1) + 1;

    if (end < next_expansion || (end - next_expansion) < start) {
      left_bound = start;
    } else {
      left_bound = end - next_expansion;
    }

    if (left_bound < start) {
      left_bound = start;
    }
    if (sorted_start == start) {
      break;
    }
  }
  wavesort_downwave(arr, start, sorted_start, end);
}

static void wavesort_nodes_c(BlockNode **restrict arr, size_t n) {
  if (!arr || n < 2) {
    return;
  }
  wavesort_upwave(arr, 0, n - 1);
}

static void wavesort_block_nodes(BlockNode **items,
                                 [[maybe_unused]] BlockNode **tmp,
                                 size_t count) {
  if (!items || count < 2) {
    return;
  }

#if WAVESORT_USE_ASM
  if (wave_sort && tmp) {
    enum { WAVESORT_STACK_LIMIT = 128 };
    int32_t keys_stack[WAVESORT_STACK_LIMIT];
    bool used_stack[WAVESORT_STACK_LIMIT];
    int32_t *keys = keys_stack;
    bool *used = used_stack;

    if (count > WAVESORT_STACK_LIMIT) {
      keys = (int32_t *)calloc(count, sizeof(*keys));
      used = (bool *)calloc(count, sizeof(*used));
    }

    if (keys && used) {
      for (size_t i = 0; i < count; ++i) {
        keys[i] = wavesort_block_id_key(items[i]->block_id);
        used[i] = false;
      }

      wave_sort(keys, count);

      bool matched_all = true;
      for (size_t i = 0; i < count; ++i) {
        const int32_t key = keys[i];
        bool matched = false;
        for (size_t j = 0; j < count; ++j) {
          if (!used[j] && wavesort_block_id_key(items[j]->block_id) == key) {
            tmp[i] = items[j];
            used[j] = true;
            matched = true;
            break;
          }
        }
        if (!matched) {
          matched_all = false;
          break;
        }
      }

      if (matched_all) {
        memcpy(items, tmp, count * sizeof(*items));
      }

      if (matched_all) {
        size_t start = 0;
        while (start < count) {
          size_t end = start + 1;
          uint64_t block_id = items[start]->block_id;
          while (end < count && items[end]->block_id == block_id) {
            end++;
          }
          if (end - start > 1) {
            wavesort_nodes_c(items + start, end - start);
          }
          start = end;
        }
      }

      if (count > WAVESORT_STACK_LIMIT) {
        free(keys);
        free(used);
      }
      if (matched_all) {
        return;
      }
    }

    if (count > WAVESORT_STACK_LIMIT) {
      free(keys);
      free(used);
    }
  }
#endif

  wavesort_nodes_c(items, count);
}

// ==========================================
// Radix sort helpers
// ==========================================

static void radix_pass_triplet(const uint64_t *keys_in,
                               const uint64_t *lengths_in,
                               const uint64_t *hashes_in,
                               BlockNode *const *nodes_in, uint64_t *keys_out,
                               uint64_t *lengths_out, uint64_t *hashes_out,
                               BlockNode **nodes_out, size_t count,
                               unsigned int shift) {
  size_t buckets[256] = {0};
  size_t sum = 0;
  for (size_t i = 0; i < count; ++i) {
    size_t idx = (size_t)((keys_in[i] >> shift) & 0xFFu);
    buckets[idx]++;
  }
  for (size_t i = 0; i < 256; ++i) {
    size_t c = buckets[i];
    buckets[i] = sum;
    sum += c;
  }
  for (size_t i = 0; i < count; ++i) {
    size_t idx = (size_t)((keys_in[i] >> shift) & 0xFFu);
    size_t dest = buckets[idx]++;
    keys_out[dest] = keys_in[i];
    lengths_out[dest] = lengths_in[i];
    hashes_out[dest] = hashes_in[i];
    nodes_out[dest] = nodes_in[i];
  }
}

bool radix_sort_block_nodes(BlockNode **items, BlockNode **tmp, size_t count) {
  if (count <= 1)
    return true;
  if (count < RADIX_SORT_MIN_COUNT) {
    wavesort_block_nodes(items, tmp, count);
    return true;
  }

  bool using_workspace = false;
  bool using_fallback = false;
  uint64_t *len_keys = nullptr;
  uint64_t *hash_keys = nullptr;
  uint64_t *len_tmp = nullptr;
  uint64_t *hash_tmp = nullptr;
  BlockNode **node_tmp = tmp;

  if (radix_workspace_lock()) {
    if (ensure_radix_workspace(count)) {
      using_workspace = true;
      len_keys = g_radix_ws.len_keys;
      hash_keys = g_radix_ws.hash_keys;
      len_tmp = g_radix_ws.len_tmp;
      hash_tmp = g_radix_ws.hash_tmp;
      if (!node_tmp) {
        node_tmp = g_radix_ws.nodes_tmp;
      }
    } else {
      radix_workspace_unlock();
    }
  }

  if (!using_workspace) {
    len_keys = (uint64_t *)malloc(count * sizeof(uint64_t));
    hash_keys = (uint64_t *)malloc(count * sizeof(uint64_t));
    len_tmp = (uint64_t *)malloc(count * sizeof(uint64_t));
    hash_tmp = (uint64_t *)malloc(count * sizeof(uint64_t));
    if (!node_tmp) {
      node_tmp = (BlockNode **)malloc(count * sizeof(BlockNode *));
    }
    using_fallback = true;
    if (!len_keys || !hash_keys || !len_tmp || !hash_tmp || !node_tmp) {
      free(len_keys);
      free(hash_keys);
      free(len_tmp);
      free(hash_tmp);
      if (!tmp) {
        free(node_tmp);
      }
      wavesort_block_nodes(items, tmp, count);
      return true;
    }
  }

  for (size_t i = 0; i < count; ++i) {
    len_keys[i] = (uint64_t)items[i]->length;
    hash_keys[i] = items[i]->block_id;
  }

  BlockNode **nodes_src = items;
  BlockNode **nodes_dst = node_tmp;
  uint64_t *len_src = len_keys;
  uint64_t *len_dst = len_tmp;
  uint64_t *hash_src = hash_keys;
  uint64_t *hash_dst = hash_tmp;

  for (size_t pass = 0; pass < sizeof(size_t); ++pass) {
    radix_pass_triplet(len_src, len_src, hash_src, nodes_src, len_dst, len_dst,
                       hash_dst, nodes_dst, count, (unsigned int)(pass * 8));
    uint64_t *len_swap = len_src;
    len_src = len_dst;
    len_dst = len_swap;

    uint64_t *hash_swap = hash_src;
    hash_src = hash_dst;
    hash_dst = hash_swap;

    BlockNode **node_swap = nodes_src;
    nodes_src = nodes_dst;
    nodes_dst = node_swap;
  }

  for (size_t pass = 0; pass < sizeof(uint64_t); ++pass) {
    radix_pass_triplet(hash_src, len_src, hash_src, nodes_src, hash_dst,
                       len_dst, hash_dst, nodes_dst, count,
                       (unsigned int)(pass * 8));
    uint64_t *len_swap = len_src;
    len_src = len_dst;
    len_dst = len_swap;

    uint64_t *hash_swap = hash_src;
    hash_src = hash_dst;
    hash_dst = hash_swap;

    BlockNode **node_swap = nodes_src;
    nodes_src = nodes_dst;
    nodes_dst = node_swap;
  }

  if (nodes_src != items) {
    memcpy(items, nodes_src, count * sizeof(*items));
  }

  if (using_fallback) {
    free(len_keys);
    free(hash_keys);
    free(len_tmp);
    free(hash_tmp);
    if (!tmp) {
      free(node_tmp);
    }
  }
  if (using_workspace) {
    radix_workspace_unlock();
  }
  return true;
}
