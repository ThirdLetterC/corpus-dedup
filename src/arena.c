#include "arena.h"

#include <stdio.h>
#include <stdlib.h>

[[nodiscard]] Arena *arena_create(size_t cap) {
  auto a = (Arena *)calloc(1, sizeof(Arena));
  if (!a)
    return nullptr;
  a->ptr = (uint8_t *)calloc(cap, sizeof(uint8_t));
  if (!a->ptr) {
    free(a);
    return nullptr;
  }
  a->cap = cap;
  return a;
}

void *arena_alloc(Arena *a, size_t size) {
  // Align to 8 bytes
  size_t aligned_size = (size + 7) & ~7;

  if (a->offset + aligned_size > a->cap) {
    size_t next_cap = a->cap;
    if (next_cap < aligned_size)
      next_cap = aligned_size;
    auto old_block = (Arena *)calloc(1, sizeof(Arena));
    if (!old_block) {
      fprintf(stderr, "Arena overflow! Increase ARENA_BLOCK_SIZE.\n");
      exit(EXIT_FAILURE);
    }
    *old_block = *a;
    a->ptr = (uint8_t *)calloc(next_cap, sizeof(uint8_t));
    if (!a->ptr) {
      free(old_block);
      fprintf(stderr, "Arena overflow! Increase ARENA_BLOCK_SIZE.\n");
      exit(EXIT_FAILURE);
    }
    a->offset = 0;
    a->cap = next_cap;
    a->next = old_block;
  }

  void *p = a->ptr + a->offset;
  a->offset += aligned_size;
  return p;
}

void arena_destroy(Arena *a) {
  while (a) {
    Arena *next = a->next;
    free(a->ptr);
    free(a);
    a = next;
  }
}
