#ifndef ARENA_H
#define ARENA_H

#include <stddef.h>
#include <stdint.h>

typedef struct Arena {
  uint8_t *ptr;
  size_t offset;
  size_t cap;
  struct Arena *next; // Linked list of arena blocks
} Arena;

[[nodiscard]] Arena *arena_create(size_t cap);
void *arena_alloc(Arena *a, size_t size);
void arena_destroy(Arena *a);

#endif
