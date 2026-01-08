# Block Tree

Parallel Block Tree construction and sentence deduplication tool in ISO C23.

## Algorithm

High-level pipeline (per run):

1. Scan the input directory for files matching `mask` (default `*.txt`).
2. Read file bytes, split into sentences, normalize whitespace, and insert into
   a hash set; write unique sentences to the output file and optionally append
   duplicates to `duplicates.txt`.
3. Build a Block Tree over the deduplicated text for verification/analysis.

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

```sh
cc -std=c2x -O3 -pthread block_tree.c -o block_tree
```

Optional tuning:

- `-DHASH_UNROLL=8` to use 8-way unrolling in the hash worker (default: 4).
- `BLOCK_TREE_THREADS=8` to override auto-detected thread count at runtime.

Example:

```sh
cc -std=c2x -O3 -pthread -DHASH_UNROLL=8 block_tree.c -o block_tree
BLOCK_TREE_THREADS=8 ./block_tree data/dedup out
```

Optional flags:

- `--write-duplicates` to write duplicate sentences into `duplicates.txt` in the
  output directory (disabled by default).
