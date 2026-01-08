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

/**
 * Allocate a new arena chain with cap bytes in the first block.
 */
[[nodiscard]] Arena *arena_create(size_t cap);
/**
 * Allocate size bytes from the arena chain, growing as needed.
 */
void *arena_alloc(Arena *a, size_t size);
/**
 * Release all arena blocks and their contents.
 */
void arena_destroy(Arena *a);

#endif
