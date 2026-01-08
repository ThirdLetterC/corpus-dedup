# Block Tree

Parallel Block Tree construction and sentence deduplication tool in ISO C23.

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
