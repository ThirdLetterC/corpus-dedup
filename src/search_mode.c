#include <dirent.h>
#include <errno.h>
#include <fnmatch.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <threads.h>
#include <unistd.h>

#include "arena.h"
#include "block_tree.h"
#include "ckdint_compat.h"
#include "config.h"
#include "hash_pool.h"
#include "io_utils.h"
#include "progress.h"
#include "search_mode.h"
#include "text_utils.h"
#include "utf8.h"

typedef struct {
  char *input_path;
  size_t start_pos;
  size_t text_len;
} SearchFile;

static bool parse_size_arg(const char *value, size_t *out) {
  if (!value || !out)
    return false;
  errno = 0;
  char *end = nullptr;
  unsigned long long parsed = strtoull(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0')
    return false;
  if (parsed > SIZE_MAX)
    return false;
  *out = (size_t)parsed;
  return true;
}

static bool ensure_search_capacity(SearchFile **buffer, size_t *cap,
                                   size_t needed) {
  if (*cap >= needed)
    return true;
  size_t new_cap = *cap ? *cap : 16;
  while (new_cap < needed) {
    size_t next_cap = 0;
    if (ckd_mul(&next_cap, new_cap, (size_t)2))
      return false;
    new_cap = next_cap;
  }
  size_t alloc_size = 0;
  if (ckd_mul(&alloc_size, new_cap, sizeof(**buffer)))
    return false;
  SearchFile *next = realloc(*buffer, alloc_size);
  if (!next)
    return false;
  *buffer = next;
  *cap = new_cap;
  return true;
}

static bool ensure_u32_capacity(uint32_t **buffer, size_t *cap, size_t needed) {
  if (*cap >= needed)
    return true;
  size_t new_cap = *cap ? *cap : 1024;
  while (new_cap < needed) {
    size_t next_cap = 0;
    if (ckd_mul(&next_cap, new_cap, (size_t)2))
      return false;
    new_cap = next_cap;
  }
  size_t alloc_size = 0;
  if (ckd_mul(&alloc_size, new_cap, sizeof(**buffer)))
    return false;
  uint32_t *next = realloc(*buffer, alloc_size);
  if (!next)
    return false;
  *buffer = next;
  *cap = new_cap;
  return true;
}

static bool append_global_text(uint32_t **buffer, size_t *len, size_t *cap,
                               const uint32_t *data, size_t data_len) {
  if (!buffer || !len || !cap)
    return false;
  if (data_len == 0)
    return true;
  size_t needed = 0;
  if (ckd_add(&needed, *len, data_len))
    return false;
  if (!ensure_u32_capacity(buffer, cap, needed))
    return false;
  memcpy(*buffer + *len, data, data_len * sizeof(**buffer));
  *len = needed;
  return true;
}

static bool build_hash_tables(const uint32_t *text, size_t len,
                              uint64_t **out_prefix, uint64_t **out_pow) {
  if (!out_prefix || !out_pow)
    return false;
  *out_prefix = nullptr;
  *out_pow = nullptr;
  size_t table_len = 0;
  if (ckd_add(&table_len, len, (size_t)1))
    return false;
  auto prefix = (uint64_t *)calloc(table_len, sizeof(uint64_t));
  auto pow = (uint64_t *)calloc(table_len, sizeof(uint64_t));
  if (!prefix || !pow) {
    free(prefix);
    free(pow);
    return false;
  }
  prefix[0] = 0;
  pow[0] = 1;
  for (size_t i = 0; i < len; ++i) {
    uint64_t value = (uint64_t)text[i] + 1;
    prefix[i + 1] = prefix[i] * SEARCH_HASH_MULT + value;
    pow[i + 1] = pow[i] * SEARCH_HASH_MULT;
  }
  *out_prefix = prefix;
  *out_pow = pow;
  return true;
}

static uint64_t hash_query(const uint32_t *query, size_t len) {
  uint64_t hash = 0;
  for (size_t i = 0; i < len; ++i) {
    uint64_t value = (uint64_t)query[i] + 1;
    hash = hash * SEARCH_HASH_MULT + value;
  }
  return hash;
}

static void free_search_file(SearchFile *file) {
  if (!file)
    return;
  free(file->input_path);
  memset(file, 0, sizeof(*file));
}

static void free_search_files(SearchFile *files, size_t count) {
  if (!files)
    return;
  for (size_t i = 0; i < count; ++i) {
    free_search_file(&files[i]);
  }
  free(files);
}

static bool index_search_file(SearchFile *file, const char *input_dir,
                              const char *name, size_t start_pos,
                              uint32_t **global_text, size_t *global_len,
                              size_t *global_cap, bool *out_added,
                              size_t *out_bytes) {
  if (!file || !input_dir || !name || !global_text || !global_len ||
      !global_cap) {
    return false;
  }
  if (out_added)
    *out_added = false;
  if (out_bytes)
    *out_bytes = 0;
  memset(file, 0, sizeof(*file));
  file->input_path = join_path(input_dir, name);
  if (!file->input_path) {
    free_search_file(file);
    return false;
  }

  char8_t *raw_text = nullptr;
  size_t byte_len = 0;
  if (!read_file_bytes(file->input_path, &raw_text, &byte_len)) {
    free_search_file(file);
    return false;
  }
  if (out_bytes)
    *out_bytes = byte_len;

  uint32_t *decoded = nullptr;
  size_t decoded_len = 0;
  size_t invalid = 0;
  if (!utf8_decode_buffer(raw_text, byte_len, &decoded, &decoded_len,
                          &invalid)) {
    free(raw_text);
    free_search_file(file);
    return false;
  }
  (void)invalid;

  if (decoded_len == 0) {
    free(decoded);
    free(raw_text);
    free_search_file(file);
    return true;
  }

  file->start_pos = start_pos;
  file->text_len = decoded_len;

  if (!append_global_text(global_text, global_len, global_cap, decoded,
                          decoded_len)) {
    free(decoded);
    free(raw_text);
    free_search_file(file);
    return false;
  }

  free(decoded);
  free(raw_text);
  if (out_added)
    *out_added = true;
  return true;
}

typedef struct {
  const BlockNode *root;
  SearchFile *files;
  size_t start_idx;
  size_t end_idx;
  const uint32_t *text;
  const uint64_t *prefix;
  const uint64_t *pow;
  const uint32_t *query;
  size_t query_len;
  uint64_t query_hash;
  mtx_t *print_lock;
  size_t hits;
  size_t files_with_hits;
} SearchWorker;

static int search_worker(void *arg) {
  SearchWorker *worker = (SearchWorker *)arg;
  if (!worker || !worker->root || !worker->files || !worker->text ||
      !worker->prefix || !worker->pow || !worker->query ||
      worker->query_len == 0) {
    return 0;
  }

  size_t local_hits = 0;
  size_t local_files = 0;
  uint32_t first = worker->query[0];

  for (size_t idx = worker->start_idx; idx < worker->end_idx; ++idx) {
    SearchFile *file = &worker->files[idx];
    if (file->text_len < worker->query_len)
      continue;
    size_t file_start = file->start_pos;
    size_t file_end = file_start + file->text_len;
    size_t line = 1;
    size_t col = 1;
    bool file_hit = false;

    for (size_t i = file_start; i + worker->query_len <= file_end; ++i) {
      uint64_t window = worker->prefix[i + worker->query_len] -
                        worker->prefix[i] * worker->pow[worker->query_len];
      if (window == worker->query_hash) {
        bool matched = (query_access(worker->root, i, worker->text) == first);
        if (matched) {
          for (size_t j = 1; j < worker->query_len; ++j) {
            if (query_access(worker->root, i + j, worker->text) !=
                worker->query[j]) {
              matched = false;
              break;
            }
          }
        }
        if (matched) {
          if (worker->print_lock) {
            mtx_lock(worker->print_lock);
          }
          printf("%s:%zu:%zu\n", file->input_path, line, col);
          if (worker->print_lock) {
            mtx_unlock(worker->print_lock);
          }
          local_hits++;
          file_hit = true;
        }
      }

      uint32_t cp = worker->text[i];
      if (cp == (uint32_t)'\n') {
        line++;
        col = 1;
      } else {
        col++;
      }
    }

    if (file_hit) {
      local_files++;
    }
  }

  worker->hits = local_hits;
  worker->files_with_hits = local_files;
  return 0;
}

static size_t search_global_for_query(const BlockNode *root, SearchFile *files,
                                      size_t count, const uint32_t *text,
                                      size_t total_len, const uint32_t *query,
                                      size_t query_len, const uint64_t *prefix,
                                      const uint64_t *pow,
                                      size_t *out_files_with_hits) {
  if (out_files_with_hits)
    *out_files_with_hits = 0;
  if (!root || !files || count == 0 || !text || !query || query_len == 0 ||
      !prefix || !pow) {
    return 0;
  }
  if (total_len < query_len)
    return 0;

  size_t thread_count = 1;
#if defined(_SC_NPROCESSORS_ONLN)
  long n = sysconf(_SC_NPROCESSORS_ONLN);
  if (n > 0)
    thread_count = (size_t)n;
#endif
  if (thread_count == 0)
    thread_count = 1;
  if (thread_count > count)
    thread_count = count;

  uint64_t query_hash = hash_query(query, query_len);
  mtx_t print_lock;
  bool have_lock = mtx_init(&print_lock, mtx_plain) == thrd_success;

  SearchWorker *workers = calloc(thread_count, sizeof(*workers));
  thrd_t *threads = calloc(thread_count, sizeof(*threads));
  bool *created = calloc(thread_count, sizeof(*created));
  if (!workers || !threads || !created) {
    free(workers);
    free(threads);
    free(created);
    SearchWorker worker = {.root = root,
                           .files = files,
                           .start_idx = 0,
                           .end_idx = count,
                           .text = text,
                           .prefix = prefix,
                           .pow = pow,
                           .query = query,
                           .query_len = query_len,
                           .query_hash = query_hash,
                           .print_lock = have_lock ? &print_lock : nullptr};
    search_worker(&worker);
    if (out_files_with_hits)
      *out_files_with_hits = worker.files_with_hits;
    if (have_lock)
      mtx_destroy(&print_lock);
    return worker.hits;
  }

  size_t chunk = (count + thread_count - 1) / thread_count;
  for (size_t i = 0; i < thread_count; ++i) {
    size_t start = i * chunk;
    size_t end = start + chunk;
    if (start >= count) {
      break;
    }
    if (end > count)
      end = count;
    workers[i] =
        (SearchWorker){.root = root,
                       .files = files,
                       .start_idx = start,
                       .end_idx = end,
                       .text = text,
                       .prefix = prefix,
                       .pow = pow,
                       .query = query,
                       .query_len = query_len,
                       .query_hash = query_hash,
                       .print_lock = have_lock ? &print_lock : nullptr};
    if (thread_count == 1) {
      search_worker(&workers[i]);
    } else if (thrd_create(&threads[i], search_worker, &workers[i]) ==
               thrd_success) {
      created[i] = true;
    } else {
      search_worker(&workers[i]);
    }
  }

  if (thread_count > 1) {
    for (size_t i = 0; i < thread_count; ++i) {
      if (created[i]) {
        thrd_join(threads[i], nullptr);
      }
    }
  }

  size_t hits = 0;
  size_t files_with_hits = 0;
  for (size_t i = 0; i < thread_count; ++i) {
    hits += workers[i].hits;
    files_with_hits += workers[i].files_with_hits;
  }

  if (out_files_with_hits)
    *out_files_with_hits = files_with_hits;

  if (have_lock)
    mtx_destroy(&print_lock);
  free(created);
  free(threads);
  free(workers);
  return hits;
}

int run_search(const char *prog, int argc, char **argv) {
  double start_time = now_seconds();
  const char *input_dir = nullptr;
  const char *mask = DEFAULT_MASK;
  bool mask_set = false;
  size_t file_limit = SIZE_MAX;
  bool limit_set = false;

  for (int i = 1; i < argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
      printf("Usage:\n"
             "  %s <input_dir> [mask] [--limit N]\n"
             "  ASM: WAVESORT_USE_ASM=%d HASH_WORKER_USE_ASM=%d "
             "RADIX_SORT_USE_ASM=%d\n",
             prog, WAVESORT_USE_ASM, HASH_WORKER_USE_ASM, RADIX_SORT_USE_ASM);
      return 0;
    }
    if (strcmp(arg, "--limit") == 0) {
      if (i + 1 >= argc) {
        fprintf(stderr, "Missing value for --limit\n");
        return 1;
      }
      size_t parsed = 0;
      if (!parse_size_arg(argv[++i], &parsed) || parsed == 0) {
        fprintf(stderr, "Invalid --limit value: %s\n", argv[i]);
        return 1;
      }
      file_limit = parsed;
      limit_set = true;
      continue;
    }
    if (strncmp(arg, "--limit=", 8) == 0) {
      size_t parsed = 0;
      if (!parse_size_arg(arg + 8, &parsed) || parsed == 0) {
        fprintf(stderr, "Invalid --limit value: %s\n", arg + 8);
        return 1;
      }
      file_limit = parsed;
      limit_set = true;
      continue;
    }
    if (!input_dir) {
      input_dir = arg;
      continue;
    }
    if (!mask_set) {
      mask = arg;
      mask_set = true;
      continue;
    }
    fprintf(stderr, "Unexpected argument: %s\n", arg);
    printf("Usage:\n"
           "  %s <input_dir> [mask] [--limit N]\n"
           "  ASM: WAVESORT_USE_ASM=%d HASH_WORKER_USE_ASM=%d "
           "RADIX_SORT_USE_ASM=%d\n",
           prog, WAVESORT_USE_ASM, HASH_WORKER_USE_ASM, RADIX_SORT_USE_ASM);
    return 1;
  }

  if (!input_dir) {
    printf("Usage:\n"
           "  %s <input_dir> [mask] [--limit N]\n"
           "  ASM: WAVESORT_USE_ASM=%d HASH_WORKER_USE_ASM=%d "
           "RADIX_SORT_USE_ASM=%d\n",
           prog, WAVESORT_USE_ASM, HASH_WORKER_USE_ASM, RADIX_SORT_USE_ASM);
    return 1;
  }

  if (!ensure_directory(input_dir, false)) {
    return 1;
  }

  size_t matched = 0;
  DIR *dir = opendir(input_dir);
  if (!dir) {
    fprintf(stderr, "Failed to open input directory: %s\n", input_dir);
    return 1;
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
      continue;
    }
    bool regular = is_regular_file(input_path);
    free(input_path);
    if (!regular) {
      continue;
    }
    matched++;
    if (limit_set && matched >= file_limit) {
      break;
    }
  }

  closedir(dir);

  if (matched == 0) {
    fprintf(stderr, "No files matched %s in %s\n", mask, input_dir);
    return 1;
  }

  if (limit_set) {
    printf("Indexing up to %zu file(s).\n", file_limit);
  } else {
    printf("Indexing %zu file(s).\n", matched);
  }

  SearchFile *files = nullptr;
  size_t files_count = 0;
  size_t files_cap = 0;
  uint32_t *global_text = nullptr;
  size_t global_len = 0;
  size_t global_cap = 0;
  size_t processed = 0;
  size_t bytes_processed = 0;
  size_t errors = 0;

  dir = opendir(input_dir);
  if (!dir) {
    fprintf(stderr, "Failed to open input directory: %s\n", input_dir);
    return 1;
  }

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
      errors++;
      continue;
    }
    if (!is_regular_file(input_path)) {
      free(input_path);
      continue;
    }
    free(input_path);

    if (limit_set && processed >= file_limit) {
      break;
    }

    if (!ensure_search_capacity(&files, &files_cap, files_count + 1)) {
      fprintf(stderr, "Failed to allocate search index.\n");
      errors++;
      break;
    }

    bool added = false;
    size_t byte_len = 0;
    size_t start_pos = global_len;
    if (!index_search_file(&files[files_count], input_dir, name, start_pos,
                           &global_text, &global_len, &global_cap, &added,
                           &byte_len)) {
      fprintf(stderr, "Failed to index file: %s\n", name);
      errors++;
    } else if (added) {
      files_count++;
      bytes_processed += byte_len;
    }
    processed++;
    render_progress(processed, matched, bytes_processed, start_time);
  }

  closedir(dir);
  fprintf(stderr, "\n");

  if (files_count == 0 || global_len == 0) {
    fprintf(stderr, "No searchable content found.\n");
    free(global_text);
    free_search_files(files, files_count);
    return 1;
  }

  uint64_t *prefix = nullptr;
  uint64_t *pow = nullptr;
  if (!build_hash_tables(global_text, global_len, &prefix, &pow)) {
    fprintf(stderr, "Failed to build rolling hash tables.\n");
    free(global_text);
    free_search_files(files, files_count);
    return 1;
  }

  Arena *search_arena = arena_create(SEARCH_ARENA_BLOCK_SIZE);
  if (!search_arena) {
    fprintf(stderr, "Failed to allocate search arena.\n");
    free(prefix);
    free(pow);
    free(global_text);
    free_search_files(files, files_count);
    return 1;
  }

  BlockNode *root =
      build_block_tree(global_text, global_len, 2, 2, search_arena);
  if (!root) {
    fprintf(stderr, "Failed to build search block tree.\n");
    free(prefix);
    free(pow);
    arena_destroy(search_arena);
    free(global_text);
    free_search_files(files, files_count);
    return 1;
  }

  printf("Indexed %zu file(s) into one Block Tree (codepoints %zu).\n",
         files_count, global_len);
  printf("Enter queries to search (empty line or 'exit' to quit).\n");

  char line[4096];
  while (true) {
    printf("search> ");
    fflush(stdout);
    if (!fgets(line, sizeof(line), stdin)) {
      break;
    }
    trim_line(line);
    if (line[0] == '\0' || strcmp(line, "exit") == 0 ||
        strcmp(line, "quit") == 0) {
      break;
    }

    uint32_t *query = nullptr;
    size_t query_len = 0;
    size_t invalid = 0;
    if (!utf8_decode_buffer((const char8_t *)line, strlen(line), &query,
                            &query_len, &invalid)) {
      fprintf(stderr, "Failed to decode query.\n");
      continue;
    }
    (void)invalid;
    if (query_len == 0) {
      free(query);
      continue;
    }

    size_t files_with_hits = 0;
    uint64_t search_start = now_ns();
    size_t total_hits = search_global_for_query(
        root, files, files_count, global_text, global_len, query, query_len,
        prefix, pow, &files_with_hits);
    uint64_t search_end = now_ns();
    uint64_t search_elapsed =
        (search_end >= search_start) ? (search_end - search_start) : 0;

    if (total_hits == 0) {
      printf("No matches found.\n");
    } else {
      printf("Found %zu match(es) in %zu file(s).\n", total_hits,
             files_with_hits);
    }
    printf("Search time: ");
    print_duration_ns(search_elapsed);
    printf("\n");
    free(query);
  }

  free(prefix);
  free(pow);
  free(global_text);
  arena_destroy(search_arena);
  free_search_files(files, files_count);
  return errors == 0 ? 0 : 1;
}
