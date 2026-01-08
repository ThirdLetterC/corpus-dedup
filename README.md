# Block Tree

Parallel Block Tree construction and sentence deduplication tool in ISO C23.

## Algorithm

High-level pipeline (per run):

1. Scan the input directory for files matching `mask` (default `*.txt`).
2. Read file bytes, split into sentences, normalize whitespace, and insert into
   a hash set; write unique sentences to the output file and optionally append
   duplicates to `duplicates.txt`.
3. (Optional, `--build-block-tree`) Build a Block Tree over the deduplicated
   text for verification/analysis.

Block Tree construction (per file):

1. Decode UTF-8 into a UTF-32 array for stable, fixed-width hashing.
2. Create a root node covering the whole text and mark it as unique.
3. While marked nodes exist, partition each marked node into `divisor` children
   (`s` for level 1, `tau` for later levels) that cover the parent span.
4. Compute rolling hashes for all candidate children in parallel.
5. Sort candidates by `(length, hash)` (radix/wavesort path).
6. Scan each hash group: the first node is the leader (marked), later nodes are
   content-checked against leaders; matches become pointer nodes, mismatches
   become new leaders.
7. Stop when no new candidates exist; the result is a tree of content nodes plus
   pointer nodes that refer to duplicate blocks.

## Build

ASM build (x86_64, clang + nasm):

```sh
nasm -f elf64 -O3 wavesort.asm -o wavesort.o
clang -x assembler-with-cpp -c hash_worker.asm -o hash_worker.o
clang -x assembler-with-cpp -c radix_histogram_length.asm -o radix_histogram_length.o
clang -x assembler-with-cpp -c radix_scatter_length.asm -o radix_scatter_length.o
clang -x assembler-with-cpp -c radix_histogram_block_id.asm -o radix_histogram_block_id.o
clang -x assembler-with-cpp -c radix_scatter_block_id.asm -o radix_scatter_block_id.o
clang -std=c2x -O3 -pthread block_tree.c sentence_splitter.c \
  hash_worker.o radix_histogram_length.o radix_scatter_length.o \
  radix_histogram_block_id.o radix_scatter_block_id.o wavesort.o -o block_tree
```

Pure C build (no ASM):

```sh
clang -std=c2x -O3 -pthread -DHASH_WORKER_USE_ASM=0 -DRADIX_SORT_USE_ASM=0 \
  block_tree.c sentence_splitter.c -o block_tree
```

Optional tuning:

- `-DHASH_UNROLL=8` to use 8-way unrolling in the hash worker (default: 4).
- `BLOCK_TREE_THREADS=8` to override auto-detected thread count at runtime.
- When overriding `HASH_UNROLL` or `HASH_PREFETCH_DISTANCE`, pass the same `-D`
  flags to the `clang -x assembler-with-cpp` steps.

Example:

```sh
clang -std=c2x -O3 -pthread -DHASH_UNROLL=8 -DHASH_WORKER_USE_ASM=0 \
  -DRADIX_SORT_USE_ASM=0 block_tree.c sentence_splitter.c -o block_tree
BLOCK_TREE_THREADS=8 ./block_tree data/dedup out
```

Optional flags:

- `--write-duplicates` to write duplicate sentences into `duplicates.txt` in the
  output directory (disabled by default).
- `--build-block-tree` to construct a Block Tree over the deduplicated output
  (disabled by default).
- `--verify <dedup_dir> [mask]` to scan an already deduplicated folder, verify
  there are no duplicate sentences, and validate the Block Tree per file.
- `--search <input_dir> [mask] [--limit N]` to index matching files (optionally
  stopping after `N`) and run interactive queries over the Block Tree-backed
  text.
