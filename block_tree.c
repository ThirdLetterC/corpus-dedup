// nasm -f elf64 -O3 wavesort.asm -o wavesort.o

// clang -DWAVESORT_USE_ASM=1 -DHASH_PREFETCH_DISTANCE=384 -std=c2x -O3  -mavx2 -march=native -flto=thin -fuse-ld=lld -pthread -DNDEBUG -DHASH_UNROLL=4 \
  -fprofile-generate block_tree.c sentence_splitter.c wavesort.o -o corpus_dedup

// BLOCK_TREE_THREADS=1 ./corpus_dedup data/kobza_1 out

// llvm-profdata merge -output=block_tree.profdata
// default_6674171548242042490_0.profraw

// clang -DHASH_PREFETCH_DISTANCE=384 -std=c2x -O3 -mavx2 -march=native -flto=thin -fuse-ld=lld -pthread -DNDEBUG -DHASH_UNROLL=4 \
  -fprofile-use=block_tree.profdata sentence_splitter.c block_tree.c -o corpus_dedup_optimized

#include <dirent.h>
#include <errno.h>
#include <fnmatch.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <threads.h>
#include <time.h>
#include <unistd.h>

#include "sentence_splitter.h"

// ==========================================
// 1. Constants & Configuration
// ==========================================

const uint64_t HASH_MOD =
    4294967296ULL; // 2^32 (Implicit wrap-around logic used)
const uint64_t HASH_MULT = 31ULL;
const size_t THREAD_COUNT_FALLBACK = 4;
const size_t ARENA_BLOCK_SIZE = 1024 * 1024 * 64; // 64 MiB
const size_t FILE_BATCH_SIZE = 4096;
const char *DUPLICATES_FILENAME = "duplicates.txt";
const char *DEFAULT_MASK = "*.txt";
const size_t SENTENCE_ARENA_BLOCK_SIZE = 1024 * 64;
const size_t HASH_PARALLEL_BASE = 64;
const size_t RADIX_SORT_MIN_COUNT = 64;

// ==========================================
// 2. Data Structures
// ==========================================

// Forward declaration
typedef struct BlockNode BlockNode;

/**
 * @brief Represents a node in the Block Tree.
 * packed to minimize cache line usage.
 */
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

/**
 * @brief Arena Allocator for high-performance object creation.
 * Reduces malloc overhead and ensures locality.
 */
typedef struct Arena {
  uint8_t *ptr;
  size_t offset;
  size_t cap;
  struct Arena *next; // Linked list of arena blocks
} Arena;

/**
 * @brief Thread context for parallel hashing.
 */
typedef struct {
  BlockNode **nodes;
  size_t start_idx;
  size_t end_idx;
  const uint32_t *text;
  size_t text_len;
} ThreadContext;

typedef struct HashThreadPool HashThreadPool;

typedef struct {
  HashThreadPool *pool;
  size_t index;
} HashWorkerArg;

struct HashThreadPool {
  thrd_t *threads;
  ThreadContext *contexts;
  HashWorkerArg *args;
  size_t thread_count;
  size_t active_count;
  size_t pending;
  uint64_t work_id;
  mtx_t lock;
  cnd_t start_cv;
  cnd_t done_cv;
  bool shutdown;
};

typedef struct SentenceEntry SentenceEntry;

struct SentenceEntry {
  uint64_t hash;
  size_t len;
  uint8_t *data;
  SentenceEntry *next;
};

typedef struct SentenceArenaBlock SentenceArenaBlock;

struct SentenceArenaBlock {
  uint8_t *data;
  size_t cap;
  size_t offset;
  SentenceArenaBlock *next;
};

typedef struct {
  SentenceArenaBlock *head;
  size_t block_size;
} SentenceArena;

typedef struct {
  SentenceEntry **buckets;
  size_t bucket_count;
  size_t entry_count;
  SentenceArena arena;
} SentenceSet;

typedef struct {
  char *name;
  char *input_path;
  uint8_t *raw_text;
  size_t byte_len;
} FileItem;

// ==========================================
// 3. Memory Management (Arena)
// ==========================================

Arena *arena_create(size_t cap) {
  Arena *a = malloc(sizeof(Arena));
  if (!a)
    return nullptr;
  a->ptr = malloc(cap);
  if (!a->ptr) {
    free(a);
    return nullptr;
  }
  a->offset = 0;
  a->cap = cap;
  a->next = nullptr;
  return a;
}

void *arena_alloc(Arena *a, size_t size) {
  // Align to 8 bytes
  size_t aligned_size = (size + 7) & ~7;

  if (a->offset + aligned_size > a->cap) {
    // Simple strategy: Requires the caller to handle expansion or
    // implies the initial size was sufficient.
    // For production, we would allocate a new block here.
    fprintf(stderr, "Arena overflow! Increase ARENA_BLOCK_SIZE.\n");
    exit(EXIT_FAILURE);
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

// ==========================================
// 4. Parallel Primitives
// ==========================================

/**
 * @brief Worker function for parallel rolling hash computation.
 */
// UTF-32 hashing has a scalar ASM path; leave disabled unless needed.
#define HASH_WORKER_USE_ASM 1
// Enable radix-sort assembly for dedup sorting (x86_64 + GCC/Clang only).
#ifndef RADIX_SORT_USE_ASM
#define RADIX_SORT_USE_ASM 1
#endif
// Compile-time unroll factor for hash_worker (valid: 4 or 8).
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
#define HASH_MULT_POW1 ((uint64_t)HASH_MULT_POW1_IMM)
#define HASH_MULT_POW2 ((uint64_t)HASH_MULT_POW2_IMM)
#define HASH_MULT_POW3 ((uint64_t)HASH_MULT_POW3_IMM)
#define HASH_MULT_POW4 ((uint64_t)HASH_MULT_POW4_IMM)

#if RADIX_SORT_USE_ASM && defined(__x86_64__) &&                               \
    (defined(__GNUC__) || defined(__clang__))
#define RADIX_SORT_USE_ASM_IMPL 1
#define RADIX_NODE_LENGTH_OFFSET 32
#define RADIX_NODE_BLOCK_ID_OFFSET 48
_Static_assert(offsetof(BlockNode, length) == RADIX_NODE_LENGTH_OFFSET,
               "BlockNode layout changed");
_Static_assert(offsetof(BlockNode, block_id) == RADIX_NODE_BLOCK_ID_OFFSET,
               "BlockNode layout changed");
#else
#define RADIX_SORT_USE_ASM_IMPL 0
#endif

#ifndef WAVESORT_USE_ASM
#if defined(__AVX2__) && defined(__x86_64__) &&                                \
    (defined(__GNUC__) || defined(__clang__))
#define WAVESORT_USE_ASM 1
#else
#define WAVESORT_USE_ASM 0
#endif
#endif

#if HASH_WORKER_USE_ASM
#define CTX_NODES 0
#define CTX_START_IDX 8
#define CTX_END_IDX 16
#define CTX_TEXT 24
#define CTX_TEXT_LEN 32

#define NODE_START_POS 24
#define NODE_LENGTH 32
#define NODE_BLOCK_ID 48

_Static_assert(offsetof(ThreadContext, nodes) == CTX_NODES,
               "ThreadContext layout changed");
_Static_assert(offsetof(ThreadContext, start_idx) == CTX_START_IDX,
               "ThreadContext layout changed");
_Static_assert(offsetof(ThreadContext, end_idx) == CTX_END_IDX,
               "ThreadContext layout changed");
_Static_assert(offsetof(ThreadContext, text) == CTX_TEXT,
               "ThreadContext layout changed");
_Static_assert(offsetof(ThreadContext, text_len) == CTX_TEXT_LEN,
               "ThreadContext layout changed");

_Static_assert(offsetof(BlockNode, start_pos) == NODE_START_POS,
               "BlockNode layout changed");
_Static_assert(offsetof(BlockNode, length) == NODE_LENGTH,
               "BlockNode layout changed");
_Static_assert(offsetof(BlockNode, block_id) == NODE_BLOCK_ID,
               "BlockNode layout changed");

int hash_worker(void *arg);

#define STR2(x) #x
#define STR(x) STR2(x)

#if HASH_PREFETCH_DISTANCE
#define HASH_PREFETCH_ASM                                                      \
  "  prefetcht0 [r8 + " STR(HASH_PREFETCH_DISTANCE) "]\n"
#else
#define HASH_PREFETCH_ASM ""
#endif

#define HASH_ALIGN_ASM "  .p2align 4\n"

#if HASH_UNROLL == 8
__asm__(".text\n"
        ".intel_syntax noprefix\n"
        ".globl hash_worker\n"
        ".type hash_worker,@function\n" HASH_ALIGN_ASM "hash_worker:\n"
        "  push rbx\n"
        "  push r12\n"
        "  mov rbx, [rdi + " STR(
            CTX_NODES) "]\n"
                       "  mov r11, [rdi + " STR(
                           CTX_START_IDX) "]\n"
                                          "  mov r12, [rdi + " STR(CTX_END_IDX) "]\n"
                                                                                "  mov r9, [rdi + " STR(
                                                                                    CTX_TEXT) "]\n"
                                                                                              "  mov r10, [rdi + " STR(CTX_TEXT_LEN) "]\n"
                                                                                                                                     "  jmp .Lcheck_outer\n"
                                                                                                                                     ".Louter:\n"
                                                                                                                                     "  mov rdx, [rbx + r11*8]\n"
                                                                                                                                     "  mov rcx, [rdx + " STR(NODE_START_POS) "]\n"
                                                                                                                                                                              "  cmp rcx, r10\n"
                                                                                                                                                                              "  jae .Lset_zero\n"
                                                                                                                                                                              "  mov rax, [rdx + " STR(NODE_LENGTH) "]\n"
                                                                                                                                                                                                                    "  mov rsi, r10\n"
                                                                                                                                                                                                                    "  sub rsi, rcx\n"
                                                                                                                                                                                                                    "  cmp rax, rsi\n"
                                                                                                                                                                                                                    "  cmova rax, rsi\n"
                                                                                                                                                                                                                    "  lea r8, [r9 + rcx*4]\n"
                                                                                                                                                                                                                    "  xor rcx, rcx\n"
                                                                                                                                                                                                                    "  test rax, rax\n"
                                                                                                                                                                                                                    "  je .Lstore\n"
                                                                                                                                                                                                                    ".Lword_loop:\n"
                                                                                                                                                                                                                    "  cmp rax, 8\n"
                                                                                                                                                                                                                    "  jb .Lword_tail\n" HASH_ALIGN_ASM ".Lword_loop8:\n" HASH_PREFETCH_ASM
                                                                                                                                                                                                                    "  mov edi, dword ptr [r8]\n"
                                                                                                                                                                                                                    "  imul rdi, rdi, " STR(HASH_MULT_POW3_IMM) "\n"
                                                                                                                                                                                                                                                                "  mov esi, dword ptr [r8 + 4]\n"
                                                                                                                                                                                                                                                                "  imul rsi, rsi, " STR(HASH_MULT_POW2_IMM) "\n"
                                                                                                                                                                                                                                                                                                            "  add rdi, rsi\n"
                                                                                                                                                                                                                                                                                                            "  mov esi, dword ptr [r8 + 8]\n"
                                                                                                                                                                                                                                                                                                            "  imul rsi, rsi, " STR(
                                                                                                                                                                                                                                                                                                                HASH_MULT_POW1_IMM) "\n"
                                                                                                                                                                                                                                                                                                                                    "  add rdi, rsi\n"
                                                                                                                                                                                                                                                                                                                                    "  mov esi, dword ptr [r8 + 12]\n"
                                                                                                                                                                                                                                                                                                                                    "  add rdi, rsi\n"
                                                                                                                                                                                                                                                                                                                                    "  imul rcx, rcx, " STR(HASH_MULT_POW4_IMM) "\n"
                                                                                                                                                                                                                                                                                                                                                                                "  add rcx, rdi\n"
                                                                                                                                                                                                                                                                                                                                                                                "  mov edi, dword ptr [r8 + 16]\n"
                                                                                                                                                                                                                                                                                                                                                                                "  imul rdi, rdi, " STR(HASH_MULT_POW3_IMM) "\n"
                                                                                                                                                                                                                                                                                                                                                                                                                            "  mov esi, dword ptr [r8 + 20]\n"
                                                                                                                                                                                                                                                                                                                                                                                                                            "  imul rsi, rsi, " STR(HASH_MULT_POW2_IMM) "\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                        "  add rdi, rsi\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                        "  mov esi, dword ptr [r8 + 24]\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                        "  imul rsi, rsi, " STR(HASH_MULT_POW1_IMM) "\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    "  add rdi, rsi\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    "  mov esi, dword ptr [r8 + 28]\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    "  add rdi, rsi\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    "  imul rcx, rcx, " STR(
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        HASH_MULT_POW4_IMM) "\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  add rcx, rdi\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  add r8, 32\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  sub rax, 8\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  cmp rax, 8\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  jae .Lword_loop8\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            ".Lword_tail:\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  test rax, rax\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  je .Lstore\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            ".Lword_tail_loop:\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  mov edi, dword ptr [r8]\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  imul rcx, rcx, 31\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  add rcx, rdi\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  add r8, 4\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  dec rax\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  jne .Lword_tail_loop\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            ".Lstore:\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            "  mov qword ptr [rdx + " STR(NODE_BLOCK_ID) "], rcx\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         "  jmp .Lnext\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         ".Lset_zero:\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         "  mov qword ptr [rdx + " STR(NODE_BLOCK_ID) "], 0\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      ".Lnext:\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      "  inc r11\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      ".Lcheck_outer:\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      "  cmp r11, r12\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      "  jb .Louter\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      "  xor eax, eax\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      "  pop r12\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      "  pop rbx\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      "  ret\n"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      ".att_syntax prefix\n");
#else
__asm__(
    ".text\n"
    ".intel_syntax noprefix\n"
    ".globl hash_worker\n"
    ".type hash_worker,@function\n" HASH_ALIGN_ASM "hash_worker:\n"
    "  push rbx\n"
    "  push r12\n"
    "  mov rbx, [rdi + " STR(
        CTX_NODES) "]\n"
                   "  mov r11, [rdi + " STR(
                       CTX_START_IDX) "]\n"
                                      "  mov r12, [rdi + " STR(CTX_END_IDX) "]"
                                                                            "\n"
                                                                            "  "
                                                                            "mo"
                                                                            "v "
                                                                            "r9"
                                                                            ", "
                                                                            "[r"
                                                                            "di"
                                                                            " +"
                                                                            " " STR(
                                                                                CTX_TEXT) "]\n"
                                                                                          "  mov r10, [rdi + " STR(CTX_TEXT_LEN) "]\n"
                                                                                                                                 "  jmp .Lcheck_outer\n"
                                                                                                                                 ".Louter:\n"
                                                                                                                                 "  mov rdx, [rbx + r11*8]\n"
                                                                                                                                 "  mov rcx, [rdx + " STR(NODE_START_POS) "]\n"
                                                                                                                                                                          "  cmp rcx, r10\n"
                                                                                                                                                                          "  jae .Lset_zero\n"
                                                                                                                                                                          "  mov rax, [rdx + " STR(NODE_LENGTH) "]\n"
                                                                                                                                                                                                                "  mov rsi, r10\n"
                                                                                                                                                                                                                "  sub rsi, rcx\n"
                                                                                                                                                                                                                "  cmp rax, rsi\n"
                                                                                                                                                                                                                "  cmova rax, rsi\n"
                                                                                                                                                                                                                "  lea r8, [r9 + rcx*4]\n"
                                                                                                                                                                                                                "  xor rcx, rcx\n"
                                                                                                                                                                                                                "  test rax, rax\n"
                                                                                                                                                                                                                "  je .Lstore\n"
                                                                                                                                                                                                                ".Lword_loop:\n"
                                                                                                                                                                                                                "  cmp rax, 4\n"
                                                                                                                                                                                                                "  jb .Lword_tail\n" HASH_ALIGN_ASM ".Lword_loop4:\n" HASH_PREFETCH_ASM "  mov edi, dword ptr [r8]\n"
                                                                                                                                                                                                                "  imul rdi, rdi, " STR(
                                                                                                                                                                                                                    HASH_MULT_POW3_IMM) "\n"
                                                                                                                                                                                                                                        "  mov esi, dword ptr [r8 + 4]\n"
                                                                                                                                                                                                                                        "  imul rsi, rsi, " STR(HASH_MULT_POW2_IMM) "\n"
                                                                                                                                                                                                                                                                                    "  add rdi, rsi\n"
                                                                                                                                                                                                                                                                                    "  mov esi, dword ptr [r8 + 8]\n"
                                                                                                                                                                                                                                                                                    "  imul rsi, rsi, " STR(HASH_MULT_POW1_IMM) "\n"
                                                                                                                                                                                                                                                                                                                                "  add rdi, rsi\n"
                                                                                                                                                                                                                                                                                                                                "  mov esi, dword ptr [r8 + 12]\n"
                                                                                                                                                                                                                                                                                                                                "  add rdi, rsi\n"
                                                                                                                                                                                                                                                                                                                                "  imul rcx, rcx, " STR(
                                                                                                                                                                                                                                                                                                                                    HASH_MULT_POW4_IMM) "\n"
                                                                                                                                                                                                                                                                                                                                                        "  add rcx, rdi\n"
                                                                                                                                                                                                                                                                                                                                                        "  add r8, 16\n"
                                                                                                                                                                                                                                                                                                                                                        "  sub rax, 4\n"
                                                                                                                                                                                                                                                                                                                                                        "  cmp rax, 4\n"
                                                                                                                                                                                                                                                                                                                                                        "  jae .Lword_loop4\n"
                                                                                                                                                                                                                                                                                                                                                        ".Lword_tail:\n"
                                                                                                                                                                                                                                                                                                                                                        "  test rax, rax\n"
                                                                                                                                                                                                                                                                                                                                                        "  je .Lstore\n"
                                                                                                                                                                                                                                                                                                                                                        ".Lword_tail_loop:\n"
                                                                                                                                                                                                                                                                                                                                                        "  mov edi, dword ptr [r8]\n"
                                                                                                                                                                                                                                                                                                                                                        "  imul rcx, rcx, 31\n"
                                                                                                                                                                                                                                                                                                                                                        "  add rcx, rdi\n"
                                                                                                                                                                                                                                                                                                                                                        "  add r8, 4\n"
                                                                                                                                                                                                                                                                                                                                                        "  dec rax\n"
                                                                                                                                                                                                                                                                                                                                                        "  jne .Lword_tail_loop\n"
                                                                                                                                                                                                                                                                                                                                                        ".Lstore:\n"
                                                                                                                                                                                                                                                                                                                                                        "  mov qword ptr [rdx + " STR(
                                                                                                                                                                                                                                                                                                                                                            NODE_BLOCK_ID) "], rcx\n"
                                                                                                                                                                                                                                                                                                                                                                           "  jmp .Lnext\n"
                                                                                                                                                                                                                                                                                                                                                                           ".Lset_zero:\n"
                                                                                                                                                                                                                                                                                                                                                                           "  mov qword ptr [rdx + " STR(NODE_BLOCK_ID) "], 0\n"
                                                                                                                                                                                                                                                                                                                                                                                                                        ".Lnext:\n"
                                                                                                                                                                                                                                                                                                                                                                                                                        "  inc r11\n"
                                                                                                                                                                                                                                                                                                                                                                                                                        ".Lcheck_outer:\n"
                                                                                                                                                                                                                                                                                                                                                                                                                        "  cmp r11, r12\n"
                                                                                                                                                                                                                                                                                                                                                                                                                        "  jb .Louter\n"
                                                                                                                                                                                                                                                                                                                                                                                                                        "  xor eax, eax\n"
                                                                                                                                                                                                                                                                                                                                                                                                                        "  pop r12\n"
                                                                                                                                                                                                                                                                                                                                                                                                                        "  pop rbx\n"
                                                                                                                                                                                                                                                                                                                                                                                                                        "  ret\n"
                                                                                                                                                                                                                                                                                                                                                                                                                        ".att_syntax prefix\n");
#endif
#else
int hash_worker(void *arg) {
  ThreadContext *ctx = (ThreadContext *)arg;

  for (size_t i = ctx->start_idx; i < ctx->end_idx; ++i) {
    BlockNode *node = ctx->nodes[i];

    // Bounds checking
    if (node->start_pos >= ctx->text_len) {
      node->block_id = 0;
      continue;
    }

    size_t effective_len = node->length;
    if (node->start_pos + effective_len > ctx->text_len) {
      effective_len = ctx->text_len - node->start_pos;
    }

    // Compute Rolling Hash
    // Note: For C23, strictly unsigned overflow is well-defined (modulo 2^n).
    uint64_t h = 0;
    const uint32_t *data = ctx->text + node->start_pos;

    // Unrolled polynomial hash loop to improve ILP.
    size_t j = 0;
#if HASH_UNROLL == 8
    size_t limit = effective_len & ~(size_t)7;
    for (; j < limit; j += 8) {
#if HASH_PREFETCH_DISTANCE
      __builtin_prefetch(data + j + (HASH_PREFETCH_DISTANCE / sizeof(uint32_t)),
                         0, 3);
#endif
      uint64_t t0 = (uint64_t)data[j] * HASH_MULT_POW3 +
                    (uint64_t)data[j + 1] * HASH_MULT_POW2 +
                    (uint64_t)data[j + 2] * HASH_MULT_POW1 +
                    (uint64_t)data[j + 3];
      uint64_t t1 = (uint64_t)data[j + 4] * HASH_MULT_POW3 +
                    (uint64_t)data[j + 5] * HASH_MULT_POW2 +
                    (uint64_t)data[j + 6] * HASH_MULT_POW1 +
                    (uint64_t)data[j + 7];
      h = h * HASH_MULT_POW4 + t0;
      h = h * HASH_MULT_POW4 + t1;
    }
#else
    size_t limit = effective_len & ~(size_t)3;
    for (; j < limit; j += 4) {
#if HASH_PREFETCH_DISTANCE
      __builtin_prefetch(data + j + (HASH_PREFETCH_DISTANCE / sizeof(uint32_t)),
                         0, 3);
#endif
      uint64_t t0 = (uint64_t)data[j] * HASH_MULT_POW3 +
                    (uint64_t)data[j + 1] * HASH_MULT_POW2 +
                    (uint64_t)data[j + 2] * HASH_MULT_POW1 +
                    (uint64_t)data[j + 3];
      h = h * HASH_MULT_POW4 + t0;
    }
#endif
    for (; j < effective_len; ++j) {
      h = h * HASH_MULT + (uint64_t)data[j];
    }

    node->block_id = h;
  }
  return 0;
}
#endif

static HashThreadPool g_hash_pool = {0};
static bool g_hash_pool_registered = false;

static int hash_pool_worker(void *arg) {
  HashWorkerArg *worker = (HashWorkerArg *)arg;
  HashThreadPool *pool = worker->pool;
  size_t index = worker->index;
  uint64_t last_work = 0;

  mtx_lock(&pool->lock);
  for (;;) {
    while (!pool->shutdown && pool->work_id == last_work) {
      cnd_wait(&pool->start_cv, &pool->lock);
    }
    if (pool->shutdown) {
      mtx_unlock(&pool->lock);
      return 0;
    }
    last_work = pool->work_id;

    bool active = index < pool->active_count;
    ThreadContext *ctx = active ? &pool->contexts[index] : nullptr;

    mtx_unlock(&pool->lock);
    if (active && ctx->start_idx < ctx->end_idx) {
      hash_worker(ctx);
    }
    mtx_lock(&pool->lock);

    if (active && pool->pending > 0) {
      pool->pending--;
      if (pool->pending == 0) {
        cnd_signal(&pool->done_cv);
      }
    }
  }
}

static void hash_pool_destroy(HashThreadPool *pool) {
  if (!pool || !pool->threads)
    return;

  mtx_lock(&pool->lock);
  pool->shutdown = true;
  pool->work_id++;
  cnd_broadcast(&pool->start_cv);
  mtx_unlock(&pool->lock);

  for (size_t i = 0; i < pool->thread_count; ++i) {
    thrd_join(pool->threads[i], nullptr);
  }

  cnd_destroy(&pool->done_cv);
  cnd_destroy(&pool->start_cv);
  mtx_destroy(&pool->lock);

  free(pool->threads);
  free(pool->contexts);
  free(pool->args);
  *pool = (HashThreadPool){0};
}

static void hash_pool_cleanup(void) { hash_pool_destroy(&g_hash_pool); }

static bool hash_pool_init(HashThreadPool *pool, size_t thread_count) {
  if (!pool || thread_count == 0)
    return false;
  *pool = (HashThreadPool){0};

  pool->threads = calloc(thread_count, sizeof(*pool->threads));
  pool->contexts = calloc(thread_count, sizeof(*pool->contexts));
  pool->args = calloc(thread_count, sizeof(*pool->args));
  if (!pool->threads || !pool->contexts || !pool->args) {
    goto fail;
  }
  if (mtx_init(&pool->lock, mtx_plain) != thrd_success) {
    goto fail;
  }
  if (cnd_init(&pool->start_cv) != thrd_success) {
    goto fail_lock;
  }
  if (cnd_init(&pool->done_cv) != thrd_success) {
    goto fail_start;
  }

  pool->thread_count = thread_count;
  pool->active_count = 0;
  pool->pending = 0;
  pool->work_id = 0;
  pool->shutdown = false;

  size_t created = 0;
  for (; created < thread_count; ++created) {
    pool->args[created] = (HashWorkerArg){.pool = pool, .index = created};
    if (thrd_create(&pool->threads[created], hash_pool_worker,
                    &pool->args[created]) != thrd_success) {
      goto fail_spawn;
    }
  }
  return true;

fail_spawn:
  mtx_lock(&pool->lock);
  pool->shutdown = true;
  pool->work_id++;
  cnd_broadcast(&pool->start_cv);
  mtx_unlock(&pool->lock);
  for (size_t i = 0; i < created; ++i) {
    thrd_join(pool->threads[i], nullptr);
  }
  cnd_destroy(&pool->done_cv);
fail_start:
  cnd_destroy(&pool->start_cv);
fail_lock:
  mtx_destroy(&pool->lock);
fail:
  free(pool->threads);
  free(pool->contexts);
  free(pool->args);
  *pool = (HashThreadPool){0};
  return false;
}

static HashThreadPool *hash_pool_get(size_t thread_count) {
  if (thread_count <= 1)
    return nullptr;
  if (g_hash_pool.threads && g_hash_pool.thread_count == thread_count) {
    return &g_hash_pool;
  }

  hash_pool_destroy(&g_hash_pool);
  if (!hash_pool_init(&g_hash_pool, thread_count)) {
    return nullptr;
  }
  if (!g_hash_pool_registered) {
    atexit(hash_pool_cleanup);
    g_hash_pool_registered = true;
  }
  return &g_hash_pool;
}

static size_t parse_thread_env(void) {
  const char *env = getenv("BLOCK_TREE_THREADS");
  if (!env || !*env)
    return 0;
  char *end = nullptr;
  long val = strtol(env, &end, 10);
  if (end == env || val <= 0 || val > 1024)
    return 0;
  return (size_t)val;
}

static size_t detect_thread_count(void) {
  size_t env_threads = parse_thread_env();
  if (env_threads > 0)
    return env_threads;
#if defined(_SC_NPROCESSORS_ONLN)
  long n = sysconf(_SC_NPROCESSORS_ONLN);
  if (n > 0)
    return (size_t)n;
#endif
  return THREAD_COUNT_FALLBACK;
}

/**
 * @brief Dispatcher for parallel hashing.
 */
void compute_hashes_parallel(BlockNode **candidates, size_t count,
                             const uint32_t *text, size_t len) {
  if (count == 0)
    return;

  size_t thread_count = detect_thread_count();
  if (thread_count == 0)
    thread_count = 1;

  if (thread_count <= 1) {
    ThreadContext ctx = {.nodes = candidates,
                         .start_idx = 0,
                         .end_idx = count,
                         .text = text,
                         .text_len = len};
    hash_worker(&ctx);
    return;
  }

  size_t threshold = HASH_PARALLEL_BASE * thread_count;
  if (count < threshold) {
    ThreadContext ctx = {.nodes = candidates,
                         .start_idx = 0,
                         .end_idx = count,
                         .text = text,
                         .text_len = len};
    hash_worker(&ctx);
    return;
  }

  HashThreadPool *pool = hash_pool_get(thread_count);
  if (!pool) {
    ThreadContext ctx = {.nodes = candidates,
                         .start_idx = 0,
                         .end_idx = count,
                         .text = text,
                         .text_len = len};
    hash_worker(&ctx);
    return;
  }

  size_t active = pool->thread_count;
  if (active > count)
    active = count;
  size_t chunk_size = (count + active - 1) / active;

  mtx_lock(&pool->lock);
  pool->active_count = active;
  pool->pending = active;

  for (size_t i = 0; i < active; ++i) {
    size_t start = i * chunk_size;
    size_t end = start + chunk_size;
    if (start >= count)
      start = count;
    if (end > count)
      end = count;

    pool->contexts[i] = (ThreadContext){.nodes = candidates,
                                        .start_idx = start,
                                        .end_idx = end,
                                        .text = text,
                                        .text_len = len};
  }

  pool->work_id++;
  cnd_broadcast(&pool->start_cv);
  while (pool->pending > 0) {
    cnd_wait(&pool->done_cv, &pool->lock);
  }
  mtx_unlock(&pool->lock);
}

/**
 * @brief Comparator for sorting BlockNodes.
 * Primary Key: Hash
 * Secondary Key: Length
 * Tertiary Key: Position
 */
static inline int compare_node_ptr(const BlockNode *nodeA,
                                   const BlockNode *nodeB) {
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
// 4.1 WaveSort (BlockNode pointers)
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

#if WAVESORT_USE_ASM
static inline int32_t wavesort_block_id_key(uint64_t block_id) {
  // Flip the sign bit so signed int32 sorting matches unsigned order.
  return (int32_t)((uint32_t)block_id ^ 0x80000000u);
}

// Optional ASM wave_sort from wavesort.asm; weak to keep builds working.
extern void wave_sort(int32_t *arr, size_t n) __attribute__((weak));
#endif

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
      keys = malloc(count * sizeof(*keys));
      used = malloc(count * sizeof(*used));
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

static void radix_histogram_length_c(BlockNode **src, size_t count,
                                     unsigned int shift, size_t *buckets) {
  for (size_t i = 0; i < count; ++i) {
    size_t key = src[i]->length;
    buckets[(key >> shift) & 0xFF]++;
  }
}

static void radix_scatter_length_c(BlockNode **src, BlockNode **dst,
                                   size_t count, unsigned int shift,
                                   size_t *buckets) {
  for (size_t i = 0; i < count; ++i) {
    size_t key = src[i]->length;
    size_t idx = (key >> shift) & 0xFF;
    dst[buckets[idx]++] = src[i];
  }
}

static void radix_histogram_block_id_c(BlockNode **src, size_t count,
                                       unsigned int shift, size_t *buckets) {
  for (size_t i = 0; i < count; ++i) {
    uint64_t key = src[i]->block_id;
    buckets[(key >> shift) & 0xFF]++;
  }
}

static void radix_scatter_block_id_c(BlockNode **src, BlockNode **dst,
                                     size_t count, unsigned int shift,
                                     size_t *buckets) {
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

#ifndef STR
#define STR2(x) #x
#define STR(x) STR2(x)
#endif

__asm__(".intel_syntax noprefix\n"
        ".text\n"
        ".globl radix_histogram_length_asm\n"
        ".type radix_histogram_length_asm,@function\n"
        "radix_histogram_length_asm:\n"
        "  test rsi, rsi\n"
        "  jz 1f\n"
        "  mov r8, rcx\n"
        "  mov ecx, edx\n"
        "  xor rax, rax\n"
        "0:\n"
        "  mov r9, qword ptr [rdi + rax*8]\n"
        "  mov r10, qword ptr [r9 + " STR(
            RADIX_NODE_LENGTH_OFFSET) "]\n"
                                      "  shr r10, cl\n"
                                      "  and r10, 0xFF\n"
                                      "  lea r11, [r8 + r10*8]\n"
                                      "  add qword ptr [r11], 1\n"
                                      "  inc rax\n"
                                      "  cmp rax, rsi\n"
                                      "  jb 0b\n"
                                      "1:\n"
                                      "  ret\n"
                                      ".size radix_histogram_length_asm, "
                                      ".-radix_histogram_length_asm\n"
                                      ".att_syntax prefix\n");

__asm__(".intel_syntax noprefix\n"
        ".text\n"
        ".globl radix_scatter_length_asm\n"
        ".type radix_scatter_length_asm,@function\n"
        "radix_scatter_length_asm:\n"
        "  test rdx, rdx\n"
        "  jz 1f\n"
        "  xor rax, rax\n"
        "0:\n"
        "  mov r9, qword ptr [rdi + rax*8]\n"
        "  mov r10, qword ptr [r9 + " STR(
            RADIX_NODE_LENGTH_OFFSET) "]\n"
                                      "  shr r10, cl\n"
                                      "  and r10, 0xFF\n"
                                      "  lea r11, [r8 + r10*8]\n"
                                      "  mov r10, qword ptr [r11]\n"
                                      "  mov qword ptr [rsi + r10*8], r9\n"
                                      "  add qword ptr [r11], 1\n"
                                      "  inc rax\n"
                                      "  cmp rax, rdx\n"
                                      "  jb 0b\n"
                                      "1:\n"
                                      "  ret\n"
                                      ".size radix_scatter_length_asm, "
                                      ".-radix_scatter_length_asm\n"
                                      ".att_syntax prefix\n");

__asm__(".intel_syntax noprefix\n"
        ".text\n"
        ".globl radix_histogram_block_id_asm\n"
        ".type radix_histogram_block_id_asm,@function\n"
        "radix_histogram_block_id_asm:\n"
        "  test rsi, rsi\n"
        "  jz 1f\n"
        "  mov r8, rcx\n"
        "  mov ecx, edx\n"
        "  xor rax, rax\n"
        "0:\n"
        "  mov r9, qword ptr [rdi + rax*8]\n"
        "  mov r10, qword ptr [r9 + " STR(
            RADIX_NODE_BLOCK_ID_OFFSET) "]\n"
                                        "  shr r10, cl\n"
                                        "  and r10, 0xFF\n"
                                        "  lea r11, [r8 + r10*8]\n"
                                        "  add qword ptr [r11], 1\n"
                                        "  inc rax\n"
                                        "  cmp rax, rsi\n"
                                        "  jb 0b\n"
                                        "1:\n"
                                        "  ret\n"
                                        ".size radix_histogram_block_id_asm, "
                                        ".-radix_histogram_block_id_asm\n"
                                        ".att_syntax prefix\n");

__asm__(".intel_syntax noprefix\n"
        ".text\n"
        ".globl radix_scatter_block_id_asm\n"
        ".type radix_scatter_block_id_asm,@function\n"
        "radix_scatter_block_id_asm:\n"
        "  test rdx, rdx\n"
        "  jz 1f\n"
        "  xor rax, rax\n"
        "0:\n"
        "  mov r9, qword ptr [rdi + rax*8]\n"
        "  mov r10, qword ptr [r9 + " STR(
            RADIX_NODE_BLOCK_ID_OFFSET) "]\n"
                                        "  shr r10, cl\n"
                                        "  and r10, 0xFF\n"
                                        "  lea r11, [r8 + r10*8]\n"
                                        "  mov r10, qword ptr [r11]\n"
                                        "  mov qword ptr [rsi + r10*8], r9\n"
                                        "  add qword ptr [r11], 1\n"
                                        "  inc rax\n"
                                        "  cmp rax, rdx\n"
                                        "  jb 0b\n"
                                        "1:\n"
                                        "  ret\n"
                                        ".size radix_scatter_block_id_asm, "
                                        ".-radix_scatter_block_id_asm\n"
                                        ".att_syntax prefix\n");
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

static bool radix_sort_block_nodes(BlockNode **items, BlockNode **tmp,
                                   size_t count) {
  if (count <= 1)
    return true;
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

// ==========================================
// 5. Tree Construction Logic
// ==========================================

BlockNode *create_node(Arena *arena, size_t start, size_t len, int level,
                       BlockNode *parent) {
  BlockNode *n = arena_alloc(arena, sizeof(BlockNode));
  n->start_pos = start;
  n->length = len;
  n->level = level;
  n->parent = parent;
  n->is_marked = false; // Default
  n->target_pos = 0;
  n->children = nullptr;
  n->child_count = 0;
  n->block_id = 0;
  return n;
}

/**
 * @brief Sorts candidates to identify duplicates and link them.
 */
static bool blocks_equal(const BlockNode *a, const BlockNode *b,
                         const uint32_t *text) {
  if (a->length != b->length)
    return false;
  return memcmp(text + a->start_pos, text + b->start_pos,
                a->length * sizeof(uint32_t)) == 0;
}

static bool ensure_ptr_capacity(BlockNode ***buffer, size_t *cap,
                                size_t needed) {
  if (*cap >= needed)
    return true;
  size_t new_cap = *cap ? *cap : 16;
  while (new_cap < needed) {
    if (new_cap > SIZE_MAX / 2)
      return false;
    new_cap *= 2;
  }
  BlockNode **next = realloc(*buffer, new_cap * sizeof(**buffer));
  if (!next)
    return false;
  *buffer = next;
  *cap = new_cap;
  return true;
}

void deduplicate_level(BlockNode **candidates, size_t count,
                       const uint32_t *text, BlockNode **next_marked,
                       size_t next_cap, size_t *out_marked_count) {
  if (count == 0 || !next_marked || next_cap < count) {
    if (out_marked_count)
      *out_marked_count = 0;
    return;
  }

  // 1. Sort to group identical hashes (and lengths) with lower overhead.
  if (!radix_sort_block_nodes(candidates, next_marked, count)) {
    if (out_marked_count)
      *out_marked_count = 0;
    return;
  }

  // 2. Identification Scan
  // The first node in a group of identical hashes is the "Leader" (Marked).
  // Others are "Pointers".

  // Temporary array to hold marked nodes for the next iteration
  // We allocate worst-case size (all unique)
  size_t marked_idx = 0;

  // Process first element
  BlockNode *leader = candidates[0];
  leader->is_marked = true;
  next_marked[marked_idx++] = leader;
  size_t group_start = 0;

  for (size_t i = 1; i < count; ++i) {
    BlockNode *curr = candidates[i];

    if (curr->block_id != leader->block_id || curr->length != leader->length) {
      // New hash group
      leader = curr;
      leader->is_marked = true;
      next_marked[marked_idx++] = leader;
      group_start = marked_idx - 1;
      continue;
    }

    // Same hash: verify content to avoid collision-based false deduplication.
    bool matched = false;
    for (size_t j = group_start; j < marked_idx; ++j) {
      BlockNode *candidate = next_marked[j];
      if (candidate->block_id != curr->block_id)
        continue;
      if (blocks_equal(curr, candidate, text)) {
        curr->is_marked = false;
        curr->target_pos = candidate->start_pos;
        matched = true;
        break;
      }
    }

    if (!matched) {
      curr->is_marked = true;
      next_marked[marked_idx++] = curr;
    }
  }

  *out_marked_count = marked_idx;
}

BlockNode *build_block_tree(const uint32_t *text, size_t len, int s, int tau,
                            Arena *arena) {
  if (!arena)
    return nullptr;

  // Level 0: Root
  BlockNode *root = create_node(arena, 0, len, 0, nullptr);
  root->is_marked = true;

  BlockNode **current_marked = malloc(sizeof(*current_marked));
  if (!current_marked)
    return nullptr;
  BlockNode **next_marked = nullptr;
  BlockNode **candidates = nullptr;
  size_t current_cap = 1;
  size_t next_cap = 0;
  size_t cand_cap = 0;

  current_marked[0] = root;
  size_t current_count = 1;

  // Iterate levels
  for (int level = 1;; ++level) {
    if (current_count == 0)
      break;

    // 1. Generate Candidates (Partitioning)
    size_t divisor = (size_t)((level == 1) ? s : tau);
    if (divisor == 0)
      divisor = 1;

    size_t cand_idx = 0;

    // Create children and append to candidates
    for (size_t i = 0; i < current_count; ++i) {
      BlockNode *p = current_marked[i];
      size_t max_len = p->length;
      if (p->start_pos >= len)
        continue;
      if (p->start_pos + max_len > len)
        max_len = len - p->start_pos;
      if (max_len <= 1)
        continue;

      size_t step = max_len / divisor;
      if (step == 0)
        step = 1;

      size_t num_children =
          (step == 1) ? (max_len < divisor ? max_len : divisor) : divisor;

      // Allocate children array for the parent in the arena
      p->children = arena_alloc(arena, num_children * sizeof(BlockNode *));
      p->child_count = 0;

      if (!ensure_ptr_capacity(&candidates, &cand_cap,
                               cand_idx + num_children)) {
        free(current_marked);
        free(next_marked);
        free(candidates);
        return nullptr;
      }

      for (size_t k = 0; k < num_children; ++k) {
        size_t cStart = p->start_pos + k * step;
        size_t cEnd = cStart + step;

        // Handle jagged end
        if (k == num_children - 1) {
          cEnd = p->start_pos + max_len;
        }

        if (cStart >= len)
          break;
        if (cStart >= cEnd)
          break;
        if (cEnd > len)
          cEnd = len;

        BlockNode *child = create_node(arena, cStart, cEnd - cStart, level, p);
        candidates[cand_idx++] = child;

        // Link to parent
        p->children[p->child_count++] = child;
      }
    }

    if (cand_idx == 0) {
      break;
    }

    if (!ensure_ptr_capacity(&next_marked, &next_cap, cand_idx)) {
      free(current_marked);
      free(next_marked);
      free(candidates);
      return nullptr;
    }

    // 2. Parallel Hashing
    compute_hashes_parallel(candidates, cand_idx, text, len);

    // 3. Deduplication (Sort + Scan)
    size_t next_count = 0;
    deduplicate_level(candidates, cand_idx, text, next_marked, next_cap,
                      &next_count);

    BlockNode **swap = current_marked;
    current_marked = next_marked;
    next_marked = swap;
    size_t swap_cap = current_cap;
    current_cap = next_cap;
    next_cap = swap_cap;
    current_count = next_count;
  }

  free(current_marked);
  free(next_marked);
  free(candidates);

  // Caller owns the arena and should destroy it after the tree is no longer
  // needed.
  return root;
}

// ==========================================
// 6. Verification & Utilities
// ==========================================

void print_tree(const BlockNode *node, int depth) {
  if (!node || depth > 3)
    return; // Limit depth for display

  for (int i = 0; i < depth; ++i)
    printf("  ");

  if (node->is_marked) {
    printf("[M] Hash:%lX Pos:%zu Len:%zu\n", node->block_id, node->start_pos,
           node->length);

    for (size_t i = 0; i < node->child_count; ++i) {
      print_tree(node->children[i], depth + 1);
    }
  } else {
    printf("[P] -> Target:%zu (Hash:%lX)\n", node->target_pos, node->block_id);
  }
}

uint32_t query_access(const BlockNode *node, size_t i, const uint32_t *text) {
  // 1. Handle Pointer Jumping
  if (!node->is_marked) {
    // In this simplified recursive structure, jumping is tricky without
    // a global map or parent pointers.
    // For verification, we cheat and look up the text directly,
    // as the algorithmic "Pointer Jump" requires traversing from the root
    // or a more complex parent-link traversal.

    // The logical position in the target block:
    size_t offset = i - node->start_pos;
    size_t target_global = node->target_pos + offset;
    return text[target_global];
  }

  // 2. Base Case: Leaf
  if (node->child_count == 0) {
    return text[i];
  }

  // 3. Find Child
  for (size_t k = 0; k < node->child_count; ++k) {
    BlockNode *child = node->children[k];
    if (i >= child->start_pos && i < child->start_pos + child->length) {
      return query_access(child, i, text);
    }
  }
  return (uint32_t)'?';
}

static bool decode_utf8(const uint8_t *input, size_t len, uint32_t **out,
                        size_t *out_len, size_t *invalid_count) {
  if (!out || !out_len || !invalid_count)
    return false;
  *out = nullptr;
  *out_len = 0;
  *invalid_count = 0;

  if (len == 0)
    return true;
  if (len > SIZE_MAX / sizeof(uint32_t))
    return false;

  uint32_t *buffer = malloc(len * sizeof(uint32_t));
  if (!buffer)
    return false;

  size_t i = 0;
  size_t count = 0;
  size_t invalid = 0;

  while (i < len) {
    uint8_t b0 = input[i];
    uint32_t codepoint = 0xFFFD;
    size_t advance = 1;

    if (b0 < 0x80) {
      codepoint = b0;
    } else if ((b0 & 0xE0) == 0xC0 && i + 1 < len) {
      uint8_t b1 = input[i + 1];
      if ((b1 & 0xC0) == 0x80) {
        codepoint = ((uint32_t)(b0 & 0x1F) << 6) | (uint32_t)(b1 & 0x3F);
        if (codepoint >= 0x80) {
          advance = 2;
        } else {
          codepoint = 0xFFFD;
        }
      }
    } else if ((b0 & 0xF0) == 0xE0 && i + 2 < len) {
      uint8_t b1 = input[i + 1];
      uint8_t b2 = input[i + 2];
      if ((b1 & 0xC0) == 0x80 && (b2 & 0xC0) == 0x80) {
        codepoint = ((uint32_t)(b0 & 0x0F) << 12) |
                    ((uint32_t)(b1 & 0x3F) << 6) | (uint32_t)(b2 & 0x3F);
        if (codepoint >= 0x800 && (codepoint < 0xD800 || codepoint > 0xDFFF)) {
          advance = 3;
        } else {
          codepoint = 0xFFFD;
        }
      }
    } else if ((b0 & 0xF8) == 0xF0 && i + 3 < len) {
      uint8_t b1 = input[i + 1];
      uint8_t b2 = input[i + 2];
      uint8_t b3 = input[i + 3];
      if ((b1 & 0xC0) == 0x80 && (b2 & 0xC0) == 0x80 && (b3 & 0xC0) == 0x80) {
        codepoint = ((uint32_t)(b0 & 0x07) << 18) |
                    ((uint32_t)(b1 & 0x3F) << 12) |
                    ((uint32_t)(b2 & 0x3F) << 6) | (uint32_t)(b3 & 0x3F);
        if (codepoint >= 0x10000 && codepoint <= 0x10FFFF) {
          advance = 4;
        } else {
          codepoint = 0xFFFD;
        }
      }
    }

    if (codepoint == 0xFFFD && b0 >= 0x80)
      invalid++;
    buffer[count++] = codepoint;
    i += advance;
  }

  *out = buffer;
  *out_len = count;
  *invalid_count = invalid;
  return true;
}

static uint64_t hash_bytes_fnv1a(const uint8_t *data, size_t len) {
  uint64_t hash = 1469598103934665603ULL;
  for (size_t i = 0; i < len; ++i) {
    hash ^= data[i];
    hash *= 1099511628211ULL;
  }
  return hash;
}

static size_t round_up_pow2(size_t value) {
  size_t p = 1;
  while (p < value && p <= SIZE_MAX / 2) {
    p <<= 1;
  }
  return p;
}

static inline bool is_ascii_space(unsigned char c) { return c <= 0x20; }

static void sentence_arena_init(SentenceArena *arena, size_t block_size) {
  if (!arena)
    return;
  arena->head = nullptr;
  arena->block_size = block_size ? block_size : 1024;
}

static void sentence_arena_destroy(SentenceArena *arena) {
  if (!arena)
    return;
  SentenceArenaBlock *block = arena->head;
  while (block) {
    SentenceArenaBlock *next = block->next;
    free(block->data);
    free(block);
    block = next;
  }
  arena->head = nullptr;
  arena->block_size = 0;
}

static void *sentence_arena_alloc(SentenceArena *arena, size_t size) {
  if (!arena || size == 0)
    return nullptr;
  size_t aligned = (size + 7) & ~(size_t)7;
  SentenceArenaBlock *block = arena->head;
  if (!block || block->offset + aligned > block->cap) {
    size_t cap = arena->block_size;
    if (cap < aligned)
      cap = aligned;
    SentenceArenaBlock *next = malloc(sizeof(*next));
    if (!next)
      return nullptr;
    next->data = malloc(cap);
    if (!next->data) {
      free(next);
      return nullptr;
    }
    next->cap = cap;
    next->offset = 0;
    next->next = block;
    arena->head = next;
    block = next;
  }
  void *ptr = block->data + block->offset;
  block->offset += aligned;
  return ptr;
}

static bool sentence_set_init(SentenceSet *set, size_t bucket_count) {
  if (!set)
    return false;
  size_t size = round_up_pow2(bucket_count < 16 ? 16 : bucket_count);
  set->buckets = calloc(size, sizeof(*set->buckets));
  if (!set->buckets)
    return false;
  set->bucket_count = size;
  set->entry_count = 0;
  sentence_arena_init(&set->arena, SENTENCE_ARENA_BLOCK_SIZE);
  return true;
}

static void sentence_set_destroy(SentenceSet *set) {
  if (!set)
    return;
  free(set->buckets);
  set->buckets = nullptr;
  set->bucket_count = 0;
  set->entry_count = 0;
  sentence_arena_destroy(&set->arena);
}

static bool sentence_set_rehash(SentenceSet *set, size_t new_bucket_count) {
  size_t size = round_up_pow2(new_bucket_count);
  SentenceEntry **next = calloc(size, sizeof(*next));
  if (!next)
    return false;

  for (size_t i = 0; i < set->bucket_count; ++i) {
    SentenceEntry *entry = set->buckets[i];
    while (entry) {
      SentenceEntry *next_entry = entry->next;
      size_t idx = entry->hash & (size - 1);
      entry->next = next[idx];
      next[idx] = entry;
      entry = next_entry;
    }
  }

  free(set->buckets);
  set->buckets = next;
  set->bucket_count = size;
  return true;
}

static void sentence_set_reserve_for_bytes(SentenceSet *set, size_t byte_len) {
  if (!set || set->bucket_count == 0)
    return;
  const size_t avg_sentence = 64;
  size_t expected = byte_len / avg_sentence;
  if (expected < 16)
    expected = 16;
  size_t target = set->entry_count + expected;
  size_t needed;
  if (target > SIZE_MAX / 4) {
    needed = SIZE_MAX;
  } else {
    needed = (target * 4) / 3;
  }
  if (needed <= set->bucket_count)
    return;
  size_t next_size = round_up_pow2(needed);
  if (next_size > set->bucket_count) {
    sentence_set_rehash(set, next_size);
  }
}

static bool sentence_set_insert(SentenceSet *set, const uint8_t *data,
                                size_t len, bool *inserted) {
  if (!set || !data || !inserted)
    return false;
  if (set->bucket_count == 0) {
    if (!sentence_set_init(set, 1024))
      return false;
  }

  uint64_t hash = hash_bytes_fnv1a(data, len);
  size_t idx = hash & (set->bucket_count - 1);
  for (SentenceEntry *entry = set->buckets[idx]; entry; entry = entry->next) {
    if (entry->hash == hash && entry->len == len &&
        memcmp(entry->data, data, len) == 0) {
      *inserted = false;
      return true;
    }
  }

  uint8_t *mem = sentence_arena_alloc(&set->arena, sizeof(SentenceEntry) + len);
  if (!mem)
    return false;
  SentenceEntry *entry = (SentenceEntry *)mem;
  uint8_t *copy = mem + sizeof(SentenceEntry);
  if (len > 0)
    memcpy(copy, data, len);
  entry->hash = hash;
  entry->len = len;
  entry->data = copy;
  entry->next = set->buckets[idx];
  set->buckets[idx] = entry;
  set->entry_count++;
  *inserted = true;

  if (set->entry_count > (set->bucket_count * 3) / 4) {
    sentence_set_rehash(set, set->bucket_count * 2);
  }
  return true;
}

static size_t normalize_sentence(const uint8_t *data, size_t len, uint8_t *out,
                                 size_t out_cap) {
  size_t start = 0;
  while (start < len && is_ascii_space(data[start])) {
    start++;
  }
  size_t end = len;
  while (end > start && is_ascii_space(data[end - 1])) {
    end--;
  }

  size_t out_len = 0;
  bool in_space = false;
  for (size_t i = start; i < end; ++i) {
    if (is_ascii_space(data[i])) {
      if (!in_space) {
        if (out_len < out_cap)
          out[out_len++] = ' ';
        in_space = true;
      }
      continue;
    }
    if (out_len < out_cap)
      out[out_len++] = data[i];
    in_space = false;
  }
  return out_len;
}

static bool emit_sentence(const uint8_t *data, size_t len, SentenceSet *seen,
                          uint8_t *norm_buf, size_t norm_cap, uint8_t *out_buf,
                          size_t *out_pos, size_t out_cap, size_t *out_unique,
                          size_t *out_duplicates, FILE *duplicates_fp) {
  size_t norm_len = normalize_sentence(data, len, norm_buf, norm_cap);
  if (norm_len == 0)
    return true;

  bool inserted = false;
  if (!sentence_set_insert(seen, norm_buf, norm_len, &inserted)) {
    return false;
  }

  if (inserted) {
    (*out_unique)++;
    size_t needed = norm_len + (*out_pos > 0 ? 1 : 0);
    if (*out_pos + needed > out_cap) {
      return false;
    }
    if (*out_pos > 0) {
      out_buf[(*out_pos)++] = '\n';
    }
    memcpy(out_buf + *out_pos, norm_buf, norm_len);
    *out_pos += norm_len;
  } else {
    (*out_duplicates)++;
    if (duplicates_fp) {
      if (fwrite(norm_buf, 1, norm_len, duplicates_fp) != norm_len) {
        return false;
      }
      if (fputc('\n', duplicates_fp) == EOF) {
        return false;
      }
    }
  }
  return true;
}

static bool deduplicate_sentences(const uint8_t *input, size_t len,
                                  SentenceSet *seen, uint8_t **out,
                                  size_t *out_len, size_t *out_unique,
                                  size_t *out_duplicates, FILE *duplicates_fp) {
  if (!out || !out_len || !out_unique || !out_duplicates || !seen)
    return false;
  *out = nullptr;
  *out_len = 0;
  *out_unique = 0;
  *out_duplicates = 0;

  if (!input || len == 0)
    return true;

  if (len > (SIZE_MAX - 1) / 2)
    return false;
  size_t out_cap = len * 2 + 1;

  uint8_t *buffer = malloc(out_cap);
  if (!buffer)
    return false;
  uint8_t *norm_buf = malloc(len);
  if (!norm_buf) {
    free(buffer);
    return false;
  }

  size_t out_pos = 0;
  const char *text = (const char *)input;

  SentenceList sentences = split_text_to_sentences(text, len);

  for (size_t i = 0; i < sentences.count; ++i) {
    const char *sentence = sentences.sentences[i].start;
    size_t sentence_len = sentences.sentences[i].len;
    if (!emit_sentence((const uint8_t *)sentence, sentence_len, seen, norm_buf,
                       len, buffer, &out_pos, out_cap, out_unique,
                       out_duplicates, duplicates_fp)) {
      free_sentence_list(&sentences);
      free(norm_buf);
      free(buffer);
      return false;
    }
  }

  free_sentence_list(&sentences);

  if (out_pos == 0) {
    free(norm_buf);
    free(buffer);
    return true;
  }

  free(norm_buf);
  *out = buffer;
  *out_len = out_pos;
  return true;
}

static bool read_file_bytes(const char *path, uint8_t **out, size_t *out_len) {
  if (!path || !out || !out_len)
    return false;

  FILE *fp = fopen(path, "rb");
  if (!fp) {
    fprintf(stderr, "Failed to open input file: %s\n", path);
    return false;
  }

  if (fseek(fp, 0, SEEK_END) != 0) {
    fprintf(stderr, "Failed to seek input file: %s\n", path);
    fclose(fp);
    return false;
  }
  long file_size = ftell(fp);
  if (file_size < 0) {
    fprintf(stderr, "Failed to get input file size: %s\n", path);
    fclose(fp);
    return false;
  }
  if (fseek(fp, 0, SEEK_SET) != 0) {
    fprintf(stderr, "Failed to rewind input file: %s\n", path);
    fclose(fp);
    return false;
  }

  size_t byte_len = (size_t)file_size;
  uint8_t *buffer = malloc(byte_len + 1);
  if (!buffer) {
    fprintf(stderr, "Failed to allocate text buffer for: %s\n", path);
    fclose(fp);
    return false;
  }

  size_t read_count = fread(buffer, 1, byte_len, fp);
  fclose(fp);
  if (read_count != byte_len) {
    fprintf(stderr, "Failed to read full input file: %s\n", path);
    free(buffer);
    return false;
  }
  buffer[byte_len] = '\0';

  *out = buffer;
  *out_len = byte_len;
  return true;
}

static bool write_file_bytes(const char *path, const uint8_t *data,
                             size_t len) {
  FILE *fp = fopen(path, "wb");
  if (!fp) {
    fprintf(stderr, "Failed to open output file: %s\n", path);
    return false;
  }
  size_t written = fwrite(data, 1, len, fp);
  if (written != len) {
    fprintf(stderr, "Failed to write full output file: %s\n", path);
    fclose(fp);
    return false;
  }
  if (fclose(fp) != 0) {
    fprintf(stderr, "Failed to close output file: %s\n", path);
    return false;
  }
  return true;
}

static char *dup_string(const char *input) {
  if (!input)
    return nullptr;
  size_t len = strlen(input);
  char *out = malloc(len + 1);
  if (!out)
    return nullptr;
  memcpy(out, input, len);
  out[len] = '\0';
  return out;
}

static char *join_path(const char *dir, const char *name) {
  if (!dir || !name)
    return nullptr;
  size_t dir_len = strlen(dir);
  size_t name_len = strlen(name);
  bool needs_sep = (dir_len > 0 && dir[dir_len - 1] != '/');
  size_t total = dir_len + (needs_sep ? 1 : 0) + name_len + 1;
  char *path = malloc(total);
  if (!path)
    return nullptr;

  memcpy(path, dir, dir_len);
  size_t pos = dir_len;
  if (needs_sep)
    path[pos++] = '/';
  memcpy(path + pos, name, name_len);
  path[pos + name_len] = '\0';
  return path;
}

static bool is_regular_file(const char *path) {
  struct stat st;
  if (stat(path, &st) != 0) {
    return false;
  }
  return S_ISREG(st.st_mode);
}

static bool ensure_directory(const char *path, bool create) {
  struct stat st;
  if (stat(path, &st) == 0) {
    if (!S_ISDIR(st.st_mode)) {
      fprintf(stderr, "Not a directory: %s\n", path);
      return false;
    }
    return true;
  }
  if (!create) {
    fprintf(stderr, "Directory not found: %s\n", path);
    return false;
  }
  if (errno != ENOENT) {
    fprintf(stderr, "Failed to stat directory: %s\n", path);
    return false;
  }
  if (mkdir(path, 0755) != 0) {
    fprintf(stderr, "Failed to create directory: %s\n", path);
    return false;
  }
  return true;
}

static double now_seconds(void) {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    return 0.0;
  return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void render_progress(size_t done, size_t total, size_t bytes_done,
                            double start_time) {
  const int bar_width = 30;
  static double last_update = 0.0;
  double now = now_seconds();
  if (now > 0.0 && done != 0 && done != total && now - last_update < 0.1) {
    return;
  }
  last_update = now;
  double elapsed = now - start_time;
  if (elapsed < 0.0001)
    elapsed = 0.0001;
  double rate = (double)done / elapsed;
  double mb_done = (double)bytes_done / (1024.0 * 1024.0);
  double mb_rate = mb_done / elapsed;
  double pct = (total > 0) ? (double)done * 100.0 / (double)total : 0.0;
  int filled =
      (total > 0) ? (int)((double)bar_width * (double)done / (double)total) : 0;
  if (filled > bar_width)
    filled = bar_width;
  double eta = 0.0;
  if (total > done && rate > 0.0001) {
    eta = (double)(total - done) / rate;
  }
  double eta_minutes = eta / 60.0;

  fprintf(stderr, "\r[");
  for (int i = 0; i < bar_width; ++i) {
    fputc(i < filled ? '#' : '-', stderr);
  }
  fprintf(stderr, "] %zu/%zu %5.1f%% %.2f docs/s %.2f MB/s ETA %.1fm", done,
          total, pct, rate, mb_rate, eta_minutes);
  fflush(stderr);
}

static void print_usage(const char *prog) {
  fprintf(stderr,
          "Usage: %s <input_dir> <output_dir> [mask] [--write-duplicates]\n",
          prog);
}

static bool process_text(const char *label, const uint8_t *raw_text,
                         size_t byte_len) {
  uint32_t *text = nullptr;
  size_t len = 0;
  size_t invalid = 0;
  if (!decode_utf8(raw_text, byte_len, &text, &len, &invalid)) {
    fprintf(stderr, "Failed to decode UTF-8 input for: %s\n", label);
    return false;
  }
  if (invalid > 0) {
    // fprintf(stderr, "Warning: %s had %zu invalid UTF-8 byte(s) replaced with
    // U+FFFD.\n", label, invalid);
  }

  // printf("\n=== %s ===\n", label);
  // printf("Building Parallel Block Tree...\n");

  Arena *arena = arena_create(ARENA_BLOCK_SIZE);
  if (!arena) {
    fprintf(stderr, "Failed to allocate arena for: %s\n", label);
    free(text);
    return false;
  }

  BlockNode *root = build_block_tree(text, len, 2, 2, arena);
  if (!root) {
    fprintf(stderr, "Failed to build block tree for: %s\n", label);
    arena_destroy(arena);
    free(text);
    return false;
  }

  // printf("\n--- Tree Topology (Top Levels) ---\n");
  // print_tree(root, 0);

  /*
  printf("\n--- Verification ---\n");
  int errors = 0;
  for (size_t i = 0; i < len; ++i) {
      uint32_t got = query_access(root, i, text);
      if (got != text[i]) {
          printf("Error at %zu: Expected U+%04" PRIX32 ", Got U+%04" PRIX32
  "\n", i, text[i], got); errors++;
      }
  }

  if (errors == 0) {
      printf("SUCCESS: All queries resolved correctly.\n");
  } else {
      printf("FAILURE: %d errors found.\n", errors);
  }
  */
  int errors = 0; // Verification disabled for performance

  arena_destroy(arena);
  free(text);
  return errors == 0;
}

static bool process_batch(FileItem *batch, size_t batch_count,
                          const char *output_dir, SentenceSet *seen,
                          FILE *duplicates_fp, size_t *files_written,
                          size_t *files_empty, size_t *unique_sentences,
                          size_t *duplicate_sentences, size_t *errors,
                          size_t *processed, size_t *bytes_processed,
                          size_t total_files, double start_time) {
  if (!batch || batch_count == 0)
    return true;

  for (size_t i = 0; i < batch_count; ++i) {
    batch[i].raw_text = nullptr;
    batch[i].byte_len = 0;
    if (!read_file_bytes(batch[i].input_path, &batch[i].raw_text,
                         &batch[i].byte_len)) {
      (*errors)++;
    }
  }

  for (size_t i = 0; i < batch_count; ++i) {
    FileItem *item = &batch[i];
    if (!item->raw_text) {
      free(item->name);
      free(item->input_path);
      if (processed) {
        (*processed)++;
        render_progress(*processed, total_files,
                        bytes_processed ? *bytes_processed : 0, start_time);
      }
      continue;
    }

    uint8_t *deduped = nullptr;
    size_t deduped_len = 0;
    size_t file_unique = 0;
    size_t file_duplicates = 0;
    sentence_set_reserve_for_bytes(seen, item->byte_len);
    if (!deduplicate_sentences(item->raw_text, item->byte_len, seen, &deduped,
                               &deduped_len, &file_unique, &file_duplicates,
                               duplicates_fp)) {
      fprintf(stderr, "Failed to deduplicate content for: %s\n", item->name);
      (*errors)++;
      free(item->raw_text);
      free(item->name);
      free(item->input_path);
      for (size_t j = i + 1; j < batch_count; ++j) {
        free(batch[j].raw_text);
        free(batch[j].name);
        free(batch[j].input_path);
      }
      free(deduped);
      return false;
    }
    *unique_sentences += file_unique;
    *duplicate_sentences += file_duplicates;

    if (deduped_len == 0) {
      (*files_empty)++;
      free(deduped);
      free(item->raw_text);
      free(item->name);
      free(item->input_path);
      continue;
    }

    char *output_path = join_path(output_dir, item->name);
    if (!output_path) {
      fprintf(stderr, "Failed to allocate output path for: %s\n", item->name);
      (*errors)++;
      free(deduped);
      free(item->raw_text);
      free(item->name);
      free(item->input_path);
      continue;
    }

    if (!write_file_bytes(output_path, deduped, deduped_len)) {
      (*errors)++;
      free(output_path);
      free(deduped);
      free(item->raw_text);
      free(item->name);
      free(item->input_path);
      continue;
    }

    (*files_written)++;

    if (!process_text(item->name, deduped, deduped_len)) {
      (*errors)++;
    }

    free(output_path);
    free(deduped);
    free(item->raw_text);
    free(item->name);
    free(item->input_path);
    if (processed) {
      if (bytes_processed) {
        *bytes_processed += item->byte_len;
      }
      (*processed)++;
      render_progress(*processed, total_files,
                      bytes_processed ? *bytes_processed : 0, start_time);
    }
  }

  return true;
}

int main(int argc, char **argv) {
  double overall_start = now_seconds();
  const char *input_dir = nullptr;
  const char *output_dir = nullptr;
  const char *mask = DEFAULT_MASK;
  bool mask_set = false;
  bool write_duplicates = false;

  for (int i = 1; i < argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "--write-duplicates") == 0) {
      write_duplicates = true;
      continue;
    }
    if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
      print_usage(argv[0]);
      return 0;
    }
    if (!input_dir) {
      input_dir = arg;
      continue;
    }
    if (!output_dir) {
      output_dir = arg;
      continue;
    }
    if (!mask_set) {
      mask = arg;
      mask_set = true;
      continue;
    }
    fprintf(stderr, "Unexpected argument: %s\n", arg);
    print_usage(argv[0]);
    return 1;
  }

  if (!input_dir || !output_dir) {
    print_usage(argv[0]);
    return 1;
  }

  if (!ensure_directory(input_dir, false)) {
    return 1;
  }

  if (!ensure_directory(output_dir, true)) {
    return 1;
  }

  DIR *dir = opendir(input_dir);
  if (!dir) {
    fprintf(stderr, "Failed to open input directory: %s\n", input_dir);
    return 1;
  }

  size_t matched = 0;
  size_t files_written = 0;
  size_t files_empty = 0;
  size_t unique_sentences = 0;
  size_t duplicate_sentences = 0;
  size_t errors = 0;
  bool abort_scan = false;
  FILE *duplicates_fp = nullptr;
  char *duplicates_path = nullptr;

  FileItem *items = nullptr;
  size_t items_count = 0;
  size_t items_cap = 0;

  SentenceSet seen = {0};
  if (!sentence_set_init(&seen, 1024)) {
    fprintf(stderr, "Failed to allocate dedup index.\n");
    closedir(dir);
    return 1;
  }

  if (write_duplicates) {
    duplicates_path = join_path(output_dir, DUPLICATES_FILENAME);
    if (!duplicates_path) {
      fprintf(stderr, "Failed to allocate duplicates output path.\n");
      sentence_set_destroy(&seen);
      closedir(dir);
      return 1;
    }
    duplicates_fp = fopen(duplicates_path, "wb");
    if (!duplicates_fp) {
      fprintf(stderr, "Failed to open duplicates file: %s\n", duplicates_path);
      free(duplicates_path);
      sentence_set_destroy(&seen);
      closedir(dir);
      return 1;
    }
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != nullptr) {
    const char *name = entry->d_name;
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
      continue;
    }
    if (fnmatch(mask, name, 0) != 0) {
      continue;
    }

    char *input_path = join_path(input_dir, name);
    if (!input_path) {
      fprintf(stderr, "Failed to allocate input path for: %s\n", name);
      errors++;
      continue;
    }
    if (!is_regular_file(input_path)) {
      free(input_path);
      continue;
    }
    char *name_copy = dup_string(name);
    if (!name_copy) {
      fprintf(stderr, "Failed to allocate file name for: %s\n", name);
      errors++;
      free(input_path);
      continue;
    }

    if (items_count == items_cap) {
      size_t next_cap = items_cap == 0 ? 256 : items_cap * 2;
      FileItem *next = realloc(items, next_cap * sizeof(*items));
      if (!next) {
        fprintf(stderr, "Failed to grow file list.\n");
        free(name_copy);
        free(input_path);
        errors++;
        break;
      }
      items = next;
      items_cap = next_cap;
    }

    items[items_count++] = (FileItem){.name = name_copy,
                                      .input_path = input_path,
                                      .raw_text = nullptr,
                                      .byte_len = 0};
    matched++;
  }

  closedir(dir);

  if (!abort_scan && items_count > 0) {
    FileItem *batch = calloc(FILE_BATCH_SIZE, sizeof(*batch));
    if (!batch) {
      fprintf(stderr, "Failed to allocate file batch.\n");
      abort_scan = true;
    } else {
      size_t processed = 0;
      size_t bytes_processed = 0;
      double start_time = now_seconds();
      render_progress(0, items_count, 0, start_time);

      for (size_t i = 0; i < items_count; i += FILE_BATCH_SIZE) {
        size_t batch_count = items_count - i;
        if (batch_count > FILE_BATCH_SIZE)
          batch_count = FILE_BATCH_SIZE;

        for (size_t j = 0; j < batch_count; ++j) {
          batch[j] = items[i + j];
          items[i + j].name = nullptr;
          items[i + j].input_path = nullptr;
        }

        if (!process_batch(batch, batch_count, output_dir, &seen, duplicates_fp,
                           &files_written, &files_empty, &unique_sentences,
                           &duplicate_sentences, &errors, &processed,
                           &bytes_processed, items_count, start_time)) {
          abort_scan = true;
          break;
        }
      }

      fprintf(stderr, "\n");
      free(batch);
    }
  }

  if (abort_scan && items_count > 0) {
    for (size_t i = 0; i < items_count; ++i) {
      free(items[i].raw_text);
      free(items[i].name);
      free(items[i].input_path);
    }
  }
  free(items);
  sentence_set_destroy(&seen);

  if (duplicates_fp) {
    if (fclose(duplicates_fp) != 0) {
      fprintf(stderr, "Failed to close duplicates file: %s\n",
              duplicates_path ? duplicates_path : "(unknown)");
      errors++;
    }
  }
  free(duplicates_path);

  if (abort_scan) {
    return 1;
  }

  double elapsed = now_seconds() - overall_start;
  if (elapsed < 0.0)
    elapsed = 0.0;
  double elapsed_min = elapsed / 60.0;
  size_t total_sentences = unique_sentences + duplicate_sentences;
  double duplicate_pct =
      total_sentences == 0
          ? 0.0
          : (double)duplicate_sentences * 100.0 / (double)total_sentences;

  printf("\nDedup summary: matched %zu file(s), wrote %zu, empty %zu, unique "
         "sentences %zu, "
         "duplicate sentences %zu (%.2f%%), errors %zu, elapsed %.2f min\n",
         matched, files_written, files_empty, unique_sentences,
         duplicate_sentences, duplicate_pct, errors, elapsed_min);
  return errors == 0 ? 0 : 1;
}
