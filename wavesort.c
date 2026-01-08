/*
clang -std=c17 -mavx2 -mfma -lm -O3 -march=native -Wall -Wextra -Wpedantic \
 -o wavesort_c wavesort.c
*/

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// ==========================================
// Part 1: WaveSort Implementation
// ==========================================

static inline void swap(int32_t *a, int32_t *b) {
  int32_t temp = *a;
  *a = *b;
  *b = temp;
}

static void block_swap_sl(int32_t *restrict arr, size_t m, size_t p,
                          size_t ll) {
  int32_t tmp = arr[m];
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

static void block_swap_sr(int32_t *restrict arr, size_t m, size_t r, size_t p) {
  size_t i = m;
  int32_t tmp = arr[i];
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

static void block_swap(int32_t *restrict arr, size_t m, size_t r, size_t p) {
  size_t ll = r - m;
  if (ll == 0) {
    return;
  }
  size_t lr = p - r + 1;
  if (lr == 1) {
    swap(&arr[m], &arr[p]);
    return;
  }
  if (lr <= ll) {
    block_swap_sr(arr, m, r, p);
  } else {
    block_swap_sl(arr, m, p, ll);
  }
}

static size_t partition(int32_t *restrict arr, size_t l, size_t r,
                        size_t p_idx) {
  const int32_t pivot_val = arr[p_idx];

  // Note: i is initialized to l - 1. If l == 0, this wraps to SIZE_MAX.
  // This is defined behavior for unsigned size_t. The first increment in the
  // loop wraps it back to 0.
  size_t i = l - 1;
  size_t j = r;

  while (true) {
    while (true) {
      i++;
      if (i == j) {
        return i;
      }
      if (arr[i] >= pivot_val) {
        break;
      }
    }
    while (true) {
      j--;
      if (j == i) {
        return i;
      }
      if (arr[j] <= pivot_val) {
        break;
      }
    }
    swap(&arr[i], &arr[j]);
  }
}

static void upwave(int32_t *restrict arr, size_t start, size_t end);

static void downwave(int32_t *restrict arr, size_t start, size_t sorted_start,
                     size_t end) {
  if (sorted_start == start) {
    return;
  }

  // Calculate pivot index safely to avoid overflow, though inputs are size_t
  size_t p = sorted_start + (end - sorted_start) / 2;
  size_t m = partition(arr, start, sorted_start, p);

  if (m == sorted_start) {
    if (p == sorted_start) {
      if (sorted_start > 0) {
        upwave(arr, start, sorted_start - 1);
      }
      return;
    }
    if (p > 0) {
      downwave(arr, start, sorted_start, p - 1);
    }
    return;
  }

  block_swap(arr, m, sorted_start, p);

  if (m == start) {
    if (p == sorted_start) {
      upwave(arr, m + 1, end);
      return;
    }
    size_t p_next = p + 1;
    downwave(arr, m + p_next - sorted_start, p_next, end);
    return;
  }

  if (p == sorted_start) {
    if (m > 0) {
      upwave(arr, start, m - 1);
    }
    upwave(arr, m + 1, end);
    return;
  }

  size_t right_part_len = p - sorted_start;
  size_t split_point = m + right_part_len;

  if (split_point > 0) {
    downwave(arr, start, m, split_point - 1);
  }
  downwave(arr, split_point + 1, p + 1, end);
}

static void upwave(int32_t *restrict arr, size_t start, size_t end) {
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
    downwave(arr, left_bound, sorted_start, end);
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
  downwave(arr, start, sorted_start, end);
}

void wave_sort(int32_t *restrict arr, size_t n) {
  // n < 2 guard is critical to prevent underflow of (n - 1) passed to upwave
  if (!arr || n < 2) {
    return;
  }
  upwave(arr, 0, n - 1);
}

// ==========================================
// Part 2: Benchmarking Harness
// ==========================================

// Comparator for qsort
int compare_int32(const void *a, const void *b) {
  const int32_t arg1 = *(const int32_t *)a;
  const int32_t arg2 = *(const int32_t *)b;
  if (arg1 < arg2)
    return -1;
  if (arg1 > arg2)
    return 1;
  return 0;
}

// Validation helper
bool is_sorted(const int32_t *arr, size_t n) {
  for (size_t i = 0; i < n - 1; i++) {
    if (arr[i] > arr[i + 1]) {
      return false;
    }
  }
  return true;
}

// Timer helper (C11)
double get_time_sec(void) {
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

int main(void) {
  const size_t N = 100000000; // 100 Million
  printf("Initializing benchmark for %zu integer samples...\n", N);

  // 1. Memory Allocation
  // Note: Allocating 800MB (2 * 400MB) requires a 64-bit system with sufficient
  // RAM.
  int32_t *data_wave = malloc(N * sizeof(int32_t));
  int32_t *data_qsort = malloc(N * sizeof(int32_t));

  if (!data_wave || !data_qsort) {
    fprintf(stderr, "Error: Memory allocation failed.\n");
    free(data_wave);
    free(data_qsort);
    return EXIT_FAILURE;
  }

  // 2. Data Generation
  // Note: rand() is used for simplicity; not cryptographically secure.
  srand((unsigned int)time(NULL));
  for (size_t i = 0; i < N; i++) {
    data_wave[i] = (int32_t)rand();
  }

  // Copy to ensure identical comparison
  memcpy(data_qsort, data_wave, N * sizeof(int32_t));

  printf("Data generated. Starting sort...\n\n");

  // 3. Benchmark WaveSort
  double start_wave = get_time_sec();
  wave_sort(data_wave, N);
  double end_wave = get_time_sec();
  double time_wave = end_wave - start_wave;

  // 4. Benchmark Qsort
  double start_q = get_time_sec();
  qsort(data_qsort, N, sizeof(int32_t), compare_int32);
  double end_q = get_time_sec();
  double time_q = end_q - start_q;

  // 5. Verification
  if (!is_sorted(data_wave, N)) {
    fprintf(stderr, "FAILURE: WaveSort produced unsorted output.\n");
  } else {
    printf("[OK] WaveSort Verification Passed.\n");
  }

  if (!is_sorted(data_qsort, N)) {
    fprintf(stderr, "FAILURE: Qsort produced unsorted output.\n");
  } else {
    printf("[OK] Qsort    Verification Passed.\n");
  }

  // 6. Results
  printf("\n--- Results (Lower is Better) ---\n");
  printf("WaveSort: %.6f seconds\n", time_wave);
  printf("Qsort:    %.6f seconds\n", time_q);

  if (time_q > 0.0) {
    double ratio = time_wave / time_q;
    printf("\nRatio (Wave/Qsort): %.2fx %s\n", ratio,
           ratio < 1.0 ? "(WaveSort is faster)" : "(Qsort is faster)");
  }

  // Cleanup
  free(data_wave);
  free(data_qsort);

  return EXIT_SUCCESS;
}
