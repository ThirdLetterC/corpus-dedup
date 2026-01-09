# Corpus Deduplication Tool

Parallel Block Tree construction and text deduplication tool (line, sentence, paragraph, or document level) in ISO C23.

## Algorithm

High-level pipeline (per run):

1. Scan the input directory for files matching `mask` (default `*.txt`).
2. Read file bytes, split into units (sentences by default, or paragraphs /
   whole-document), normalize whitespace, and insert into a hash set; write
   unique units to the output file and optionally append duplicates to
   `duplicates.txt`.
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
make debug
make release
```

Options:

- `-DUSE_ASM=ON|OFF` (default ON) — enable NASM fast paths.
- `-DHASH_UNROLL=8` (default 8) — unroll factor for `asm/hash_worker.asm` (4 or
  8).
- `-DHASH_PREFETCH_DISTANCE=256` — prefetch distance (bytes) for asm hash
  worker.

When `USE_ASM=ON`, the following asm sources are built: `asm/wavesort.asm`,
`asm/hash_worker.asm`, `asm/radix_histogram_length.asm`,
`asm/radix_scatter_length.asm`, `asm/radix_histogram_block_id.asm`,
`asm/radix_scatter_block_id.asm`. With `USE_ASM=OFF` the pure C fallbacks are
used (`WAVESORT_USE_ASM=0`, `HASH_WORKER_USE_ASM=0`, `RADIX_SORT_USE_ASM=0`).

Runtime tuning:

- `DEDUP_THREADS=8` overrides auto-detected thread count for per-file dedup work
  (block-tree hashing still uses `BLOCK_TREE_THREADS`).
- `BLOCK_TREE_THREADS` defaults to 1 when unset; set explicitly to run the block
  tree hash workers on more threads.

## Usage

- Dedup:

```sh
./corpus_dedup <input_dir> <output_dir> [mask] \
  [--dedup-mode <sentence|line|paragraph|document>] \
  [--write-duplicates] [--build-block-tree] [--max-length N]
```

- Verify:

```sh
./corpus_dedup --verify <dedup_dir> [mask] \
  [--dedup-mode <sentence|line|paragraph|document>] [--max-length N]
```

- Search:

```sh
./corpus_dedup --search <input_dir> [mask] [--limit N]
```

The executable is named `corpus_dedup` in `build/` (or your chosen build
directory). Adjust `--config Debug|Release` if you use multi-config generators
like Ninja Multi-Config or Xcode.

Optional flags:

- `mask` defaults to `*.txt` for all modes.
- `--dedup-mode <sentence|line|paragraph|document>` sets dedup granularity
  (default: sentence-level).
- `--max-length N` caps normalized text length used for comparisons
  (default: 0, unlimited) in dedup and verify modes.
- `--write-duplicates` writes duplicate units into `duplicates.txt` in the
  output directory (disabled by default).
- `--build-block-tree` constructs a Block Tree over the deduplicated output
  (disabled by default).
- `--limit N` in search mode stops indexing after `N` files (required to be
  positive when provided).

## Benchmark

```
> DEDUP_THREADS=8 ./build/release/corpus_dedup data/kobza_1 outk

[##############################] 1028000/1028000 100.0% 88806.64 docs/s 426.17 MB/s ETA 0.0m

Dedup summary (sentence-level): matched 1028000 file(s), wrote 1027601, empty 399, unique sentences 23899195, duplicate sentences 2880953 (10.76%), errors 0, elapsed 0.21 min, peak RSS 5923.57 MiB
```

## Acknowledgements

- Practical Parallel Block Tree Construction: https://arxiv.org/abs/2512.23314
- New Sorting Algorithm Wave Sort (W-Sort): https://arxiv.org/abs/2505.13552

## License

This project is licensed under the MIT License. See the LICENSE file for details.
