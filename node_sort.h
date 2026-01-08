#ifndef NODE_SORT_H
#define NODE_SORT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "block_tree.h"

int compare_node_ptr(const BlockNode *nodeA, const BlockNode *nodeB);
int compare_nodes(const void *a, const void *b);
bool radix_sort_block_nodes(BlockNode **items, BlockNode **tmp, size_t count);

#endif
