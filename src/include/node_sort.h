#ifndef NODE_SORT_H
#define NODE_SORT_H

#include <stddef.h>
#include <stdint.h>

#include "block_tree.h"

/**
 * Compare block nodes by start offset then length for qsort.
 */
int compare_node_ptr(const BlockNode *nodeA, const BlockNode *nodeB);
/**
 * Compare pointers to BlockNode for qsort callback compatibility.
 */
int compare_nodes(const void *a, const void *b);
/**
 * Perform radix sort on block nodes by hash and length.
 */
bool radix_sort_block_nodes(BlockNode **items, BlockNode **tmp, size_t count);

#endif
