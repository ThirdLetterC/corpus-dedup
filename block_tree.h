#ifndef BLOCK_TREE_H
#define BLOCK_TREE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct Arena;
#include "config.h"
#include "hash_pool.h"

typedef struct BlockNode BlockNode;

struct BlockNode {
  BlockNode **children; // Dynamic array of children
  BlockNode *parent;    // Pointer to parent
  size_t child_count;   // Number of children

  size_t start_pos;  // Global text position
  size_t length;     // Length of the block
  size_t target_pos; // If !is_marked, points to this global position

  uint64_t block_id; // The Rolling Hash
  int level;         // Tree depth
  bool is_marked;    // true = content node, false = pointer node
};

BlockNode *create_node(struct Arena *arena, size_t start, size_t len,
                       int level, BlockNode *parent);
void compute_hashes_parallel(BlockNode **candidates, size_t count,
                             const uint32_t *text, size_t len);
void deduplicate_level(BlockNode **candidates, size_t count,
                       const uint32_t *text, BlockNode **next_marked,
                       size_t next_cap, size_t *out_marked_count);
BlockNode *build_block_tree(const uint32_t *text, size_t len, int s, int tau,
                            struct Arena *arena);
void print_tree(const BlockNode *node, int depth);
uint32_t query_access(const BlockNode *node, size_t i, const uint32_t *text);

#endif
