#include "node_sort.h"

#include <stdlib.h>
#include <string.h>

#include "block_tree_asm_defs.h"

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
#define RADIX_SORT_USE_ASM_IMPL 0
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

// Optional ASM wave_sort from wavesort.asm; weak to keep builds working.
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

static void wavesort_block_nodes(BlockNode **items, BlockNode **tmp,
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

[[maybe_unused]] static void radix_histogram_length_c(BlockNode **src,
                                                      size_t count,
                                                      unsigned int shift,
                                                      size_t *buckets) {
  for (size_t i = 0; i < count; ++i) {
    size_t key = src[i]->length;
    buckets[(key >> shift) & 0xFF]++;
  }
}

[[maybe_unused]] static void
radix_scatter_length_c(BlockNode **src, BlockNode **dst, size_t count,
                       unsigned int shift, size_t *buckets) {
  for (size_t i = 0; i < count; ++i) {
    size_t key = src[i]->length;
    size_t idx = (key >> shift) & 0xFF;
    dst[buckets[idx]++] = src[i];
  }
}

[[maybe_unused]] static void radix_histogram_block_id_c(BlockNode **src,
                                                        size_t count,
                                                        unsigned int shift,
                                                        size_t *buckets) {
  for (size_t i = 0; i < count; ++i) {
    uint64_t key = src[i]->block_id;
    buckets[(key >> shift) & 0xFF]++;
  }
}

[[maybe_unused]] static void
radix_scatter_block_id_c(BlockNode **src, BlockNode **dst, size_t count,
                         unsigned int shift, size_t *buckets) {
  for (size_t i = 0; i < count; ++i) {
    uint64_t key = src[i]->block_id;
    size_t idx = (key >> shift) & 0xFF;
    dst[buckets[idx]++] = src[i];
  }
}

#if RADIX_SORT_USE_ASM_IMPL
void radix_histogram_length_asm(BlockNode **src, size_t count,
                                unsigned int shift, size_t *buckets);
void radix_scatter_length_asm(BlockNode **src, BlockNode **dst, size_t count,
                              unsigned int shift, size_t *buckets);
void radix_histogram_block_id_asm(BlockNode **src, size_t count,
                                  unsigned int shift, size_t *buckets);
void radix_scatter_block_id_asm(BlockNode **src, BlockNode **dst, size_t count,
                                unsigned int shift, size_t *buckets);

#endif

static void radix_pass_length(BlockNode **src, BlockNode **dst, size_t count,
                              unsigned int shift) {
  size_t buckets[256] = {0};
#if RADIX_SORT_USE_ASM_IMPL
  radix_histogram_length_asm(src, count, shift, buckets);
#else
  radix_histogram_length_c(src, count, shift, buckets);
#endif
  size_t sum = 0;
  for (size_t i = 0; i < 256; ++i) {
    size_t c = buckets[i];
    buckets[i] = sum;
    sum += c;
  }
#if RADIX_SORT_USE_ASM_IMPL
  radix_scatter_length_asm(src, dst, count, shift, buckets);
#else
  radix_scatter_length_c(src, dst, count, shift, buckets);
#endif
}

static void radix_pass_block_id(BlockNode **src, BlockNode **dst, size_t count,
                                unsigned int shift) {
  size_t buckets[256] = {0};
#if RADIX_SORT_USE_ASM_IMPL
  radix_histogram_block_id_asm(src, count, shift, buckets);
#else
  radix_histogram_block_id_c(src, count, shift, buckets);
#endif
  size_t sum = 0;
  for (size_t i = 0; i < 256; ++i) {
    size_t c = buckets[i];
    buckets[i] = sum;
    sum += c;
  }
#if RADIX_SORT_USE_ASM_IMPL
  radix_scatter_block_id_asm(src, dst, count, shift, buckets);
#else
  radix_scatter_block_id_c(src, dst, count, shift, buckets);
#endif
}

bool radix_sort_block_nodes(BlockNode **items, BlockNode **tmp, size_t count) {
  if (count <= 1)
    return true;
  enum { RADIX_SORT_MIN_COUNT = 64 };
  if (count < RADIX_SORT_MIN_COUNT) {
    wavesort_block_nodes(items, tmp, count);
    return true;
  }
  BlockNode **src = items;
  BlockNode **dst = tmp;
  for (size_t pass = 0; pass < sizeof(size_t); ++pass) {
    radix_pass_length(src, dst, count, (unsigned int)(pass * 8));
    BlockNode **swap = src;
    src = dst;
    dst = swap;
  }
  for (size_t pass = 0; pass < sizeof(uint64_t); ++pass) {
    radix_pass_block_id(src, dst, count, (unsigned int)(pass * 8));
    BlockNode **swap = src;
    src = dst;
    dst = swap;
  }
  if (src != items) {
    memcpy(items, src, count * sizeof(*items));
  }
  return true;
}
