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

## Build (CMake)

Requirements: CMake ≥ 3.20, clang or gcc with C23 support, `nasm` for asm fast
paths on x86_64.

Configure and build:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release    # or Debug
cmake --build build --config Release
```

Options:

- `-DUSE_ASM=ON|OFF` (default ON) — enable NASM fast paths.
- `-DHASH_UNROLL=8` (default 8) — unroll factor for `hash_worker.asm` (4 or 8).
- `-DHASH_PREFETCH_DISTANCE=256` — prefetch distance (bytes) for asm hash
  worker.

When `USE_ASM=ON`, the following asm sources are built: `wavesort.asm`,
`hash_worker.asm`, `radix_histogram_length.asm`, `radix_scatter_length.asm`,
`radix_histogram_block_id.asm`, `radix_scatter_block_id.asm`. With `USE_ASM=OFF`
the pure C fallbacks are used (`WAVESORT_USE_ASM=0`, `HASH_WORKER_USE_ASM=0`,
`RADIX_SORT_USE_ASM=0`).

Runtime tuning:

- `BLOCK_TREE_THREADS=8` overrides auto-detected thread count.
- CLI modes:
  - Dedup: `./corpus_dedup <input_dir> <output_dir> [mask] [--write-duplicates] [--build-block-tree]`
  - Verify: `./corpus_dedup --verify <dedup_dir> [mask]`
  - Search: `./corpus_dedup --search <input_dir> [mask] [--limit N]`

The executable is named `corpus_dedup` in `build/` (or your chosen build
directory). Adjust `--config Debug|Release` if you use multi-config generators
like Ninja Multi-Config or Xcode.

Optional flags:

- `--write-duplicates` to write duplicate sentences into `duplicates.txt` in the
  output directory (disabled by default).
- `--build-block-tree` to construct a Block Tree over the deduplicated output
  (disabled by default).
- `--verify <dedup_dir> [mask]` to scan an already deduplicated folder, verify
  there are no duplicate sentences, and validate the Block Tree per file.
- `--search <input_dir> [mask] [--limit N]` to index matching files into one
  Block Tree (optionally stopping after `N`) and run interactive queries over
  the combined text.
