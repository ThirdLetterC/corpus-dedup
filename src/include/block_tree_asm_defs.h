#ifndef BLOCK_TREE_ASM_DEFS_H
#define BLOCK_TREE_ASM_DEFS_H

// Assembly tuning toggles. Override via -D flags as needed.
#ifndef HASH_WORKER_USE_ASM
#define HASH_WORKER_USE_ASM 1
#endif

#ifndef RADIX_SORT_USE_ASM
#define RADIX_SORT_USE_ASM 1
#endif

#ifndef HASH_UNROLL
#define HASH_UNROLL 4
#endif
#if HASH_UNROLL != 4 && HASH_UNROLL != 8
#error "HASH_UNROLL must be 4 or 8"
#endif

#ifndef HASH_PREFETCH_DISTANCE
// Bytes ahead of the current pointer to prefetch (0 disables prefetch).
#define HASH_PREFETCH_DISTANCE 256
#endif

// Precomputed powers for HASH_MULT == 31.
#define HASH_MULT_POW1_IMM 31
#define HASH_MULT_POW2_IMM 961
#define HASH_MULT_POW3_IMM 29791
#define HASH_MULT_POW4_IMM 923521

// ThreadContext and BlockNode offsets for assembly routines.
#define CTX_NODES 0
#define CTX_START_IDX 8
#define CTX_END_IDX 16
#define CTX_TEXT 24
#define CTX_TEXT_LEN 32

#define NODE_START_POS 24
#define NODE_LENGTH 32
#define NODE_BLOCK_ID 48

// BlockNode offsets for radix-sort assembly routines.
#define RADIX_NODE_LENGTH_OFFSET 32
#define RADIX_NODE_BLOCK_ID_OFFSET 48

#if RADIX_SORT_USE_ASM && defined(__x86_64__) &&                               \
    (defined(__GNUC__) || defined(__clang__))
#define RADIX_SORT_USE_ASM_IMPL 1
#else
#define RADIX_SORT_USE_ASM_IMPL 0
#endif

#endif
