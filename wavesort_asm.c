/*
nasm -f elf64 -O3 wavesort.asm -o wavesort.o

clang -std=c17 -mavx2 -mfma -lm -O3 -Wall -Wextra -Wpedantic -o wavesort_asm \
wavesort_asm.c wavesort.o
*/

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/*
 * External assembly function declaration. Assumed to modify the array in-place.
 */
extern void wave_sort(int32_t *arr, size_t n);

/*
 * Comparator for qsort.
 * Marked static as it is local to this translation unit.
 */
static int compare_ints(const void *a, const void *b) {
  const int32_t arg1 = *(const int32_t *)a;
  const int32_t arg2 = *(const int32_t *)b;

  if (arg1 < arg2)
    return -1;
  if (arg1 > arg2)
    return 1;
  return 0;
}

/*
 * Validation function.
 */
static bool is_sorted(const int32_t *arr, size_t n) {
  if (n < 2) {
    return true;
  }
  for (size_t i = 0; i < n - 1; i++) {
    if (arr[i] > arr[i + 1]) {
      return false;
    }
  }
  return true;
}

int main(void) {
  printf("Initializing W-Sort Test...\n");

  // Using strict size_t for object counts. 100,000,000 * 4 bytes = ~381 MB.
  const size_t ARR_SIZE = 100000000;

  int32_t *arr = malloc(ARR_SIZE * sizeof(int32_t));
  int32_t *arr_qsort = malloc(ARR_SIZE * sizeof(int32_t));

  if (!arr || !arr_qsort) {
    /* Standard error stream for failures */
    fprintf(stderr, "Memory allocation failed.\n");
    free(arr);
    free(arr_qsort);
    return EXIT_FAILURE;
  }

  /* Cast time_t to unsigned int for srand (standard requirement) */
  srand((unsigned int)time(NULL));

  printf("Generating %zu random integers...\n", ARR_SIZE);
  for (size_t i = 0; i < ARR_SIZE; i++) {
    int32_t r = (rand() % 2000000) - 1000000;
    arr[i] = r;
    arr_qsort[i] = r;
  }

  // Benchmark QSort
  printf("Running qsort...\n");
  clock_t start = clock();
  qsort(arr_qsort, ARR_SIZE, sizeof(int32_t), compare_ints);
  clock_t end = clock();

  double qsort_time = (double)(end - start) / CLOCKS_PER_SEC;
  printf("qsort time: %.4f seconds\n", qsort_time);

  // Benchmark W-Sort
  printf("Running wave_sort (ASM)...\n");
  start = clock();
  wave_sort(arr, ARR_SIZE);
  end = clock();

  double wave_time = (double)(end - start) / CLOCKS_PER_SEC;
  printf("wave_sort time: %.4f seconds\n", wave_time);

  // Validation
  if (is_sorted(arr, ARR_SIZE)) {
    printf("SUCCESS: Array is sorted correctly.\n");
  } else {
    printf("FAILURE: Array is NOT sorted.\n");

    /* * Preserved logic: Only check mismatch if sorting failed.
     * (Note: This does not verify that wave_sort results match qsort results
     * if wave_sort returns a sorted but corrupted array).
     */
    for (size_t i = 0; i < ARR_SIZE; i++) {
      if (arr[i] != arr_qsort[i]) {
        printf("Mismatch at index %zu: Expected %d, Got %d\n", i, arr_qsort[i],
               arr[i]);
        break;
      }
    }
  }

  free(arr);
  free(arr_qsort);

  return EXIT_SUCCESS;
}
