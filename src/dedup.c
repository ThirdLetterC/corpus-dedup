#include "dedup.h"

#include <dirent.h>
#include <errno.h>
#include <fnmatch.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <threads.h>
#include <unistd.h>

#include "arena.h"
#include "block_tree.h"
#include "ckdint_compat.h"
#include "config.h"
#include "hash_utils.h"
#include "io_utils.h"
#include "progress.h"
#include "sentence_set.h"
#include "sentence_splitter.h"
#include "text_utils.h"
#include "utf8.h"

typedef struct {
  char *name;
  char *input_path;
  char8_t *raw_text;
  size_t byte_len;
} FileItem;

typedef struct {
  char8_t *dedup_buffer;
  size_t dedup_cap;
  char8_t *norm_buffer;
  size_t norm_cap;
} DedupScratch;

constexpr size_t SPAN_INIT_CAP = 16;

typedef enum {
  DEDUP_MODE_SENTENCE = 0,
  DEDUP_MODE_LINE = 1,
  DEDUP_MODE_PARAGRAPH = 2,
  DEDUP_MODE_DOCUMENT = 3
} DedupMode;

typedef struct {
  atomic_size_t files_written;
  atomic_size_t files_empty;
  atomic_size_t unique_units;
  atomic_size_t duplicate_units;
  atomic_size_t errors;
  atomic_size_t processed;
  atomic_size_t bytes_processed;
} BatchStats;

static const char *dedup_mode_name(DedupMode mode) {
  switch (mode) {
  case DEDUP_MODE_LINE:
    return "line";
  case DEDUP_MODE_SENTENCE:
    return "sentence";
  case DEDUP_MODE_PARAGRAPH:
    return "paragraph";
  case DEDUP_MODE_DOCUMENT:
    return "document";
  }
  return "sentence";
}

static const char *dedup_unit_plural(DedupMode mode) {
  switch (mode) {
  case DEDUP_MODE_LINE:
    return "lines";
  case DEDUP_MODE_SENTENCE:
    return "sentences";
  case DEDUP_MODE_PARAGRAPH:
    return "paragraphs";
  case DEDUP_MODE_DOCUMENT:
    return "documents";
  }
  return "sentences";
}

static bool parse_dedup_mode(const char *arg, DedupMode *mode) {
  if (!arg || !mode)
    return false;
  if (strcmp(arg, "sentence") == 0) {
    *mode = DEDUP_MODE_SENTENCE;
    return true;
  }
  if (strcmp(arg, "line") == 0 || strcmp(arg, "lines") == 0) {
    *mode = DEDUP_MODE_LINE;
    return true;
  }
  if (strcmp(arg, "paragraph") == 0) {
    *mode = DEDUP_MODE_PARAGRAPH;
    return true;
  }
  if (strcmp(arg, "document") == 0) {
    *mode = DEDUP_MODE_DOCUMENT;
    return true;
  }
  return false;
}

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

static bool ensure_scratch(DedupScratch *scratch, size_t input_len) {
  if (!scratch)
    return false;

  size_t needed_out = 0;
  if (ckd_mul(&needed_out, input_len, (size_t)2) ||
      ckd_add(&needed_out, needed_out, (size_t)1)) {
    return false;
  }
  if (scratch->dedup_cap < needed_out) {
    size_t alloc_size = 0;
    if (ckd_mul(&alloc_size, needed_out, sizeof(char8_t)))
      return false;
    auto next = (char8_t *)realloc(scratch->dedup_buffer, alloc_size);
    if (!next)
      return false;
    scratch->dedup_buffer = next;
    scratch->dedup_cap = needed_out;
  }

  if (scratch->norm_cap < input_len) {
    size_t alloc_size = 0;
    if (ckd_mul(&alloc_size, input_len, sizeof(char8_t)))
      return false;
    auto next = (char8_t *)realloc(scratch->norm_buffer, alloc_size);
    if (!next)
      return false;
    scratch->norm_buffer = next;
    scratch->norm_cap = input_len;
  }

  return true;
}

static inline bool is_ascii_space(unsigned char c) { return c <= 0x20; }

static bool has_non_space(const char8_t *data, size_t start, size_t end) {
  for (size_t i = start; i < end; ++i) {
    if (!is_ascii_space((unsigned char)data[i]))
      return true;
  }
  return false;
}

typedef struct {
  SentenceSpan *items;
  size_t count;
  size_t capacity;
} SpanList;

static void free_span_list(SpanList *list) {
  if (!list)
    return;
  free(list->items);
  list->items = nullptr;
  list->count = 0;
  list->capacity = 0;
}

static bool append_span(SpanList *list, const char8_t *start, size_t len) {
  if (!list || !start || len == 0)
    return true;
  if (list->count == list->capacity) {
    size_t next_cap = list->capacity == 0 ? SPAN_INIT_CAP : list->capacity;
    if (list->capacity != 0 && ckd_mul(&next_cap, list->capacity, (size_t)2))
      return false;
    size_t alloc_size = 0;
    if (ckd_mul(&alloc_size, next_cap, sizeof(SentenceSpan)))
      return false;
    auto next = (SentenceSpan *)realloc(list->items, alloc_size);
    if (!next)
      return false;
    list->items = next;
    list->capacity = next_cap;
  }
  list->items[list->count++] = (SentenceSpan){.start = start, .len = len};
  return true;
}

static bool split_text_to_paragraphs(const char8_t *text, size_t len,
                                     SpanList *out) {
  if (!out)
    return false;
  out->items = nullptr;
  out->count = 0;
  out->capacity = 0;
  if (!text || len == 0)
    return true;

  size_t paragraph_start = 0;
  size_t pos = 0;
  while (pos < len) {
    size_t line_start = pos;
    while (pos < len && text[pos] != (char8_t)'\n' &&
           text[pos] != (char8_t)'\r') {
      pos++;
    }
    size_t line_end = pos;
    while (pos < len &&
           (text[pos] == (char8_t)'\n' || text[pos] == (char8_t)'\r')) {
      pos++;
    }
    bool line_blank = !has_non_space(text, line_start, line_end);
    if (line_blank) {
      if (paragraph_start < line_start &&
          has_non_space(text, paragraph_start, line_start)) {
        if (!append_span(out, text + paragraph_start,
                         line_start - paragraph_start)) {
          free_span_list(out);
          return false;
        }
      }
      paragraph_start = pos;
    }
  }

  if (paragraph_start < len && has_non_space(text, paragraph_start, len)) {
    if (!append_span(out, text + paragraph_start, len - paragraph_start)) {
      free_span_list(out);
      return false;
    }
  }
  return true;
}

static bool split_text_to_lines(const char8_t *text, size_t len,
                                SpanList *out) {
  if (!out)
    return false;
  out->items = nullptr;
  out->count = 0;
  out->capacity = 0;
  if (!text || len == 0)
    return true;

  size_t line_start = 0;
  size_t pos = 0;
  while (pos < len) {
    while (pos < len && text[pos] != (char8_t)'\n' &&
           text[pos] != (char8_t)'\r') {
      pos++;
    }
    size_t line_end = pos;
    while (pos < len &&
           (text[pos] == (char8_t)'\n' || text[pos] == (char8_t)'\r')) {
      pos++;
    }
    if (has_non_space(text, line_start, line_end)) {
      if (!append_span(out, text + line_start, line_end - line_start)) {
        free_span_list(out);
        return false;
      }
    }
    line_start = pos;
  }
  return true;
}

static bool emit_unit(const char8_t *data, size_t len, SentenceSet *seen,
                      SentenceSet *local_seen, char8_t *norm_buf,
                      size_t norm_cap, char8_t *out_buf, size_t *out_pos,
                      size_t out_cap, size_t *out_unique, size_t *out_duplicates,
                      FILE *duplicates_fp, mtx_t *duplicates_lock,
                      size_t max_compare_len) {
  size_t norm_len = normalize_sentence(data, len, norm_buf, norm_cap);
  if (max_compare_len != 0 && norm_len > max_compare_len) {
    norm_len = max_compare_len;
  }
  if (norm_len == 0)
    return true;

  uint64_t hash = hash_bytes_fnv1a(norm_buf, norm_len);

  if (local_seen) {
    bool local_inserted = false;
    if (!sentence_set_insert_hashed(local_seen, hash, norm_buf, norm_len,
                                    &local_inserted)) {
      return false;
    }
    if (!local_inserted) {
      (*out_duplicates)++;
      if (duplicates_fp) {
        if (duplicates_lock)
          mtx_lock(duplicates_lock);
        bool ok = fwrite(norm_buf, 1, norm_len, duplicates_fp) == norm_len &&
                  fputc('\n', duplicates_fp) != EOF;
        if (duplicates_lock)
          mtx_unlock(duplicates_lock);
        if (!ok)
          return false;
      }
      return true;
    }
  }

  bool inserted = false;
  bool inserted_ok =
      sentence_set_insert_hashed(seen, hash, norm_buf, norm_len, &inserted);
  if (!inserted_ok) {
    return false;
  }

  if (inserted) {
    (*out_unique)++;
    size_t needed = norm_len + (*out_pos > 0 ? 1 : 0);
    if (*out_pos + needed > out_cap) {
      return false;
    }
    if (*out_pos > 0) {
      out_buf[(*out_pos)++] = (char8_t)'\n';
    }
    memcpy(out_buf + *out_pos, norm_buf, norm_len);
    *out_pos += norm_len;
  } else {
    (*out_duplicates)++;
    if (duplicates_fp) {
      if (duplicates_lock)
        mtx_lock(duplicates_lock);
      bool ok = fwrite(norm_buf, 1, norm_len, duplicates_fp) == norm_len &&
                fputc('\n', duplicates_fp) != EOF;
      if (duplicates_lock)
        mtx_unlock(duplicates_lock);
      if (!ok)
        return false;
    }
  }
  return true;
}

static bool deduplicate_spans(const char8_t *input, size_t input_len,
                              const SentenceSpan *spans, size_t span_count,
                              SentenceSet *local_seen, SentenceSet *seen,
                              DedupScratch *scratch, size_t max_compare_len,
                              char8_t **out, size_t *out_len,
                              size_t *out_unique, size_t *out_duplicates,
                              FILE *duplicates_fp, mtx_t *duplicates_lock) {
  if (!out || !out_len || !out_unique || !out_duplicates || !seen || !scratch)
    return false;
  *out = nullptr;
  *out_len = 0;
  *out_unique = 0;
  *out_duplicates = 0;

  if (!input || input_len == 0 || span_count == 0)
    return true;

  if (!spans)
    return false;

  if (!ensure_scratch(scratch, input_len)) {
    return false;
  }

  size_t out_pos = 0;
  for (size_t i = 0; i < span_count; ++i) {
    const char8_t *segment = spans[i].start;
    size_t segment_len = spans[i].len;
    if (!emit_unit(segment, segment_len, seen, local_seen,
                   scratch->norm_buffer, scratch->norm_cap,
                   scratch->dedup_buffer, &out_pos, scratch->dedup_cap,
                   out_unique, out_duplicates, duplicates_fp, duplicates_lock,
                   max_compare_len)) {
      return false;
    }
  }

  if (out_pos == 0)
    return true;

  *out = scratch->dedup_buffer;
  *out_len = out_pos;
  return true;
}

static bool deduplicate_sentences(const char8_t *input, size_t len,
                                  SentenceSet *local_seen, SentenceSet *seen,
                                  DedupScratch *scratch, char8_t **out,
                                  size_t *out_len, size_t *out_unique,
                                  size_t *out_duplicates, FILE *duplicates_fp,
                                  mtx_t *duplicates_lock,
                                  size_t max_compare_len) {
  SentenceList sentences = split_text_to_sentences(input, len);
  bool ok = deduplicate_spans(input, len, sentences.sentences, sentences.count,
                              local_seen, seen, scratch, max_compare_len, out,
                              out_len, out_unique, out_duplicates,
                              duplicates_fp, duplicates_lock);
  free_sentence_list(&sentences);
  return ok;
}

static bool deduplicate_paragraphs(const char8_t *input, size_t len,
                                   SentenceSet *local_seen, SentenceSet *seen,
                                   DedupScratch *scratch, char8_t **out,
                                   size_t *out_len, size_t *out_unique,
                                   size_t *out_duplicates, FILE *duplicates_fp,
                                   mtx_t *duplicates_lock,
                                   size_t max_compare_len) {
  SpanList paragraphs = {0};
  if (!split_text_to_paragraphs(input, len, &paragraphs)) {
    return false;
  }

  bool ok = deduplicate_spans(input, len, paragraphs.items, paragraphs.count,
                              local_seen, seen, scratch, max_compare_len, out,
                              out_len, out_unique, out_duplicates,
                              duplicates_fp, duplicates_lock);
  free_span_list(&paragraphs);
  return ok;
}

static bool deduplicate_lines(const char8_t *input, size_t len,
                              SentenceSet *local_seen, SentenceSet *seen,
                              DedupScratch *scratch, char8_t **out,
                              size_t *out_len, size_t *out_unique,
                              size_t *out_duplicates, FILE *duplicates_fp,
                              mtx_t *duplicates_lock,
                              size_t max_compare_len) {
  SpanList lines = {0};
  if (!split_text_to_lines(input, len, &lines)) {
    return false;
  }

  bool ok = deduplicate_spans(input, len, lines.items, lines.count, local_seen,
                              seen, scratch, max_compare_len, out, out_len,
                              out_unique, out_duplicates, duplicates_fp,
                              duplicates_lock);
  free_span_list(&lines);
  return ok;
}

static bool deduplicate_document(const char8_t *input, size_t len,
                                 SentenceSet *local_seen, SentenceSet *seen,
                                 DedupScratch *scratch, char8_t **out,
                                 size_t *out_len, size_t *out_unique,
                                 size_t *out_duplicates, FILE *duplicates_fp,
                                 mtx_t *duplicates_lock,
                                 size_t max_compare_len) {
  SentenceSpan single = {.start = input, .len = len};
  size_t span_count = input && len > 0 ? 1 : 0;
  return deduplicate_spans(input, len, &single, span_count, local_seen, seen,
                           scratch, max_compare_len, out, out_len, out_unique,
                           out_duplicates, duplicates_fp, duplicates_lock);
}

static bool deduplicate_with_mode(DedupMode mode, const char8_t *input,
                                  size_t len, size_t max_compare_len,
                                  SentenceSet *local_seen, SentenceSet *seen,
                                  DedupScratch *scratch, char8_t **out,
                                  size_t *out_len,
                                  size_t *out_unique, size_t *out_duplicates,
                                  FILE *duplicates_fp, mtx_t *duplicates_lock) {
  switch (mode) {
  case DEDUP_MODE_DOCUMENT:
    return deduplicate_document(input, len, local_seen, seen, scratch, out,
                                out_len, out_unique, out_duplicates,
                                duplicates_fp, duplicates_lock,
                                max_compare_len);
  case DEDUP_MODE_LINE:
    return deduplicate_lines(input, len, local_seen, seen, scratch, out,
                             out_len, out_unique, out_duplicates,
                             duplicates_fp, duplicates_lock, max_compare_len);
  case DEDUP_MODE_PARAGRAPH:
    return deduplicate_paragraphs(input, len, local_seen, seen, scratch, out,
                                  out_len, out_unique, out_duplicates,
                                  duplicates_fp, duplicates_lock,
                                  max_compare_len);
  case DEDUP_MODE_SENTENCE:
  default:
    return deduplicate_sentences(input, len, local_seen, seen, scratch, out,
                                 out_len, out_unique, out_duplicates,
                                 duplicates_fp, duplicates_lock,
                                 max_compare_len);
  }
}

static bool process_text(const char *label, const char8_t *raw_text,
                         size_t byte_len, bool verify_tree) {
  uint32_t *text = nullptr;
  size_t len = 0;
  size_t invalid = 0;
  if (!utf8_decode_buffer(raw_text, byte_len, &text, &len, &invalid)) {
    fprintf(stderr, "Failed to decode UTF-8 input for: %s\n", label);
    return false;
  }

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

  size_t errors = 0;
  if (verify_tree) {
    for (size_t i = 0; i < len; ++i) {
      uint32_t got = query_access(root, i, text);
      if (got != text[i]) {
        if (errors < 5) {
          fprintf(stderr,
                  "Verification error in %s at %zu: expected U+%04" PRIX32
                  ", got U+%04" PRIX32 "\n",
                  label, i, text[i], got);
        }
        errors++;
      }
    }
    if (errors > 5) {
      fprintf(stderr, "Verification errors in %s: %zu total\n", label, errors);
    }
  }

  arena_destroy(arena);
  free(text);
  return errors == 0;
}

static size_t parse_thread_env() {
  constexpr char ENV_NAME[] = "DEDUP_THREADS";
  const char *env = getenv(ENV_NAME);
  if (!env || !*env)
    return 0;
  char *end = nullptr;
  long val = strtol(env, &end, 10);
  if (end == env || val <= 0 || val > 1024)
    return 0;
  return (size_t)val;
}

static size_t detect_thread_count() {
  static size_t cached = 0;
  if (cached != 0)
    return cached;
  size_t env_threads = parse_thread_env();
  if (env_threads > 0) {
    cached = env_threads;
    return cached;
  }
#if defined(_SC_NPROCESSORS_ONLN)
  long n = sysconf(_SC_NPROCESSORS_ONLN);
  if (n > 0) {
    cached = (size_t)n;
    return cached;
  }
#endif
  cached = THREAD_COUNT_FALLBACK;
  if (cached == 0)
    cached = 1;
  return cached;
}

static size_t peak_rss_bytes() {
  struct rusage usage;
  if (getrusage(RUSAGE_SELF, &usage) != 0)
    return 0;
#if defined(__APPLE__)
  return (size_t)usage.ru_maxrss;
#else
  return (size_t)usage.ru_maxrss * 1024u;
#endif
}

typedef struct {
  FileItem *batch;
  size_t batch_count;
  const char *output_dir;
  SentenceSet *seen;
  FILE *duplicates_fp;
  mtx_t *duplicates_lock;
  bool build_tree;
  DedupMode dedup_mode;
  size_t max_compare_len;
  atomic_size_t next_index;
  BatchStats *stats;
  size_t total_files;
  double start_time;
  mtx_t *progress_lock;
  mtx_t *tree_lock;
} WorkerContext;

static int batch_worker(void *arg) {
  auto ctx = (WorkerContext *)arg;
  DedupScratch scratch = {0};
  SentenceSet local_seen = {0};
  bool local_seen_init = sentence_set_init(&local_seen, 512);

  for (;;) {
    size_t idx =
        atomic_fetch_add_explicit(&ctx->next_index, 1, memory_order_relaxed);
    if (idx >= ctx->batch_count)
      break;

    FileItem *item = &ctx->batch[idx];
    size_t processed_bytes = 0;

    item->raw_text = nullptr;
    item->byte_len = 0;
    if (!read_file_bytes(item->input_path, &item->raw_text, &item->byte_len)) {
      atomic_fetch_add_explicit(&ctx->stats->errors, 1, memory_order_relaxed);
      free(item->name);
      free(item->input_path);
      goto finish_file;
    }
    processed_bytes = item->byte_len;

    sentence_set_reserve_for_bytes(ctx->seen, item->byte_len);

    char8_t *deduped = nullptr;
    size_t deduped_len = 0;
    size_t file_unique = 0;
    size_t file_duplicates = 0;

    if (!deduplicate_with_mode(ctx->dedup_mode, item->raw_text, item->byte_len,
                               ctx->max_compare_len,
                               local_seen_init ? &local_seen : nullptr,
                               ctx->seen, &scratch, &deduped, &deduped_len,
                               &file_unique, &file_duplicates,
                               ctx->duplicates_fp, ctx->duplicates_lock)) {
      fprintf(stderr, "Failed to deduplicate content for: %s\n", item->name);
      atomic_fetch_add_explicit(&ctx->stats->errors, 1, memory_order_relaxed);
      free(item->raw_text);
      free(item->name);
      free(item->input_path);
      goto finish_file;
    }

    atomic_fetch_add_explicit(&ctx->stats->unique_units, file_unique,
                              memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->stats->duplicate_units, file_duplicates,
                              memory_order_relaxed);

    if (deduped_len == 0) {
      atomic_fetch_add_explicit(&ctx->stats->files_empty, 1,
                                memory_order_relaxed);
      free(item->raw_text);
      free(item->name);
      free(item->input_path);
      goto finish_file;
    }

    char *output_path = join_path(ctx->output_dir, item->name);
    if (!output_path) {
      fprintf(stderr, "Failed to allocate output path for: %s\n", item->name);
      atomic_fetch_add_explicit(&ctx->stats->errors, 1, memory_order_relaxed);
      free(item->raw_text);
      free(item->name);
      free(item->input_path);
      goto finish_file;
    }

    if (!write_file_bytes(output_path, deduped, deduped_len)) {
      atomic_fetch_add_explicit(&ctx->stats->errors, 1, memory_order_relaxed);
      free(output_path);
      free(item->raw_text);
      free(item->name);
      free(item->input_path);
      goto finish_file;
    }

    atomic_fetch_add_explicit(&ctx->stats->files_written, 1,
                              memory_order_relaxed);

    if (ctx->build_tree) {
      if (ctx->tree_lock)
        mtx_lock(ctx->tree_lock);
      bool ok = process_text(item->name, deduped, deduped_len, false);
      if (ctx->tree_lock)
        mtx_unlock(ctx->tree_lock);
      if (!ok) {
        atomic_fetch_add_explicit(&ctx->stats->errors, 1, memory_order_relaxed);
      }
    }

    free(output_path);
    free(item->raw_text);
    free(item->name);
    free(item->input_path);

  finish_file:
    if (local_seen_init) {
      sentence_set_clear(&local_seen);
    }
    if (processed_bytes > 0) {
      atomic_fetch_add_explicit(&ctx->stats->bytes_processed, processed_bytes,
                                memory_order_relaxed);
    }
    size_t processed = atomic_fetch_add_explicit(&ctx->stats->processed, 1,
                                                 memory_order_relaxed) +
                       1;
    if (ctx->progress_lock) {
      mtx_lock(ctx->progress_lock);
      size_t current_bytes = atomic_load_explicit(&ctx->stats->bytes_processed,
                                                  memory_order_relaxed);
      render_progress(processed, ctx->total_files, current_bytes,
                      ctx->start_time);
      mtx_unlock(ctx->progress_lock);
    }
  }

  free(scratch.dedup_buffer);
  free(scratch.norm_buffer);
  if (local_seen_init) {
    sentence_set_destroy(&local_seen);
  }
  return 0;
}

static bool process_batch(FileItem *batch, size_t batch_count,
                          const char *output_dir, SentenceSet *seen,
                          FILE *duplicates_fp, mtx_t *duplicates_lock,
                          bool build_tree, DedupMode dedup_mode,
                          size_t max_compare_len, BatchStats *stats,
                          size_t total_files, double start_time,
                          mtx_t *progress_lock, mtx_t *tree_lock) {
  if (!batch || batch_count == 0)
    return true;

  size_t worker_count = detect_thread_count();
  if (worker_count == 0)
    worker_count = 1;
  if (worker_count > batch_count)
    worker_count = batch_count;

  WorkerContext ctx = {.batch = batch,
                       .batch_count = batch_count,
                       .output_dir = output_dir,
                       .seen = seen,
                       .duplicates_fp = duplicates_fp,
                       .duplicates_lock = duplicates_lock,
                       .build_tree = build_tree,
                       .dedup_mode = dedup_mode,
                       .max_compare_len = max_compare_len,
                       .stats = stats,
                       .total_files = total_files,
                       .start_time = start_time,
                       .progress_lock = progress_lock,
                       .tree_lock = tree_lock};
  atomic_init(&ctx.next_index, 0);

  auto threads = (thrd_t *)calloc(worker_count, sizeof(thrd_t));
  if (!threads) {
    return batch_worker(&ctx) == 0;
  }

  size_t launched = 0;
  for (; launched < worker_count; ++launched) {
    if (thrd_create(&threads[launched], batch_worker, &ctx) != thrd_success) {
      atomic_fetch_add_explicit(&stats->errors, 1, memory_order_relaxed);
      break;
    }
  }

  if (launched == 0) {
    free(threads);
    return batch_worker(&ctx) == 0;
  }

  for (size_t i = 0; i < launched; ++i) {
    thrd_join(threads[i], nullptr);
  }

  free(threads);
  return launched > 0;
}

int run_dedup(const char *prog, int argc, char **argv) {
  double overall_start = now_seconds();
  const char *input_dir = nullptr;
  const char *output_dir = nullptr;
  const char *mask = DEFAULT_MASK;
  bool mask_set = false;
  bool write_duplicates = false;
  bool build_block_tree_flag = false;
  DedupMode dedup_mode = DEDUP_MODE_SENTENCE;
  size_t max_compare_len = DEFAULT_MAX_COMPARE_LENGTH;

  for (int i = 1; i < argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "--write-duplicates") == 0) {
      write_duplicates = true;
      continue;
    }
    if (strcmp(arg, "--build-block-tree") == 0) {
      build_block_tree_flag = true;
      continue;
    }
    if (strcmp(arg, "--max-length") == 0) {
      if (i + 1 >= argc) {
        fprintf(stderr, "Missing value for --max-length\n");
        return 1;
      }
      size_t parsed = 0;
      if (!parse_size_arg(argv[++i], &parsed)) {
        fprintf(stderr, "Invalid --max-length value: %s\n", argv[i]);
        return 1;
      }
      max_compare_len = parsed;
      continue;
    }
    if (strncmp(arg, "--max-length=", 13) == 0) {
      size_t parsed = 0;
      if (!parse_size_arg(arg + 13, &parsed)) {
        fprintf(stderr, "Invalid --max-length value: %s\n", arg + 13);
        return 1;
      }
      max_compare_len = parsed;
      continue;
    }
    if (strcmp(arg, "--dedup-mode") == 0) {
      if (i + 1 >= argc) {
        fprintf(
            stderr,
            "--dedup-mode requires one of: sentence, line, paragraph, document\n");
        return 1;
      }
      const char *mode_arg = argv[++i];
      if (!parse_dedup_mode(mode_arg, &dedup_mode)) {
        fprintf(stderr, "Invalid --dedup-mode value: %s (expected sentence, "
                        "line, paragraph, or document)\n",
                mode_arg);
        return 1;
      }
      continue;
    }
    if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
      printf("Usage:\n"
             "  %s <input_dir> <output_dir> [mask] [--dedup-mode "
             "<sentence|line|paragraph|document>] "
             "[--write-duplicates] [--build-block-tree] [--max-length N]\n"
             "  --max-length defaults to %zu symbols (0 disables the limit)\n"
             "  ASM: WAVESORT_USE_ASM=%d HASH_WORKER_USE_ASM=%d "
             "RADIX_SORT_USE_ASM=%d\n",
             prog, DEFAULT_MAX_COMPARE_LENGTH, WAVESORT_USE_ASM,
             HASH_WORKER_USE_ASM, RADIX_SORT_USE_ASM);
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
    printf("Usage:\n"
             "  %s <input_dir> <output_dir> [mask] [--dedup-mode "
             "<sentence|line|paragraph|document>] "
             "[--write-duplicates] [--build-block-tree] [--max-length N]\n"
             "  --max-length defaults to %zu symbols (0 disables the limit)\n"
             "  ASM: WAVESORT_USE_ASM=%d HASH_WORKER_USE_ASM=%d "
             "RADIX_SORT_USE_ASM=%d\n",
           prog, DEFAULT_MAX_COMPARE_LENGTH, WAVESORT_USE_ASM,
           HASH_WORKER_USE_ASM, RADIX_SORT_USE_ASM);
    return 1;
  }

  if (!input_dir || !output_dir) {
    printf("Usage:\n"
           "  %s <input_dir> <output_dir> [mask] [--dedup-mode "
           "<sentence|line|paragraph|document>] "
           "[--write-duplicates] [--build-block-tree] [--max-length N]\n"
           "  --max-length defaults to %zu symbols (0 disables the limit)\n",
           prog, DEFAULT_MAX_COMPARE_LENGTH);
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

  mtx_t duplicates_lock;
  bool duplicates_lock_init = false;

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
    if (mtx_init(&duplicates_lock, mtx_plain) != thrd_success) {
      fprintf(stderr, "Failed to initialize duplicates mutex.\n");
      fclose(duplicates_fp);
      free(duplicates_path);
      sentence_set_destroy(&seen);
      closedir(dir);
      return 1;
    }
    duplicates_lock_init = true;
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
      size_t next_cap = 256;
      if (items_cap != 0 && ckd_mul(&next_cap, items_cap, (size_t)2)) {
        fprintf(stderr, "Failed to grow file list.\n");
        free(name_copy);
        free(input_path);
        errors++;
        break;
      }
      size_t alloc_size = 0;
      if (ckd_mul(&alloc_size, next_cap, sizeof(*items))) {
        fprintf(stderr, "Failed to grow file list.\n");
        free(name_copy);
        free(input_path);
        errors++;
        break;
      }
      FileItem *next = realloc(items, alloc_size);
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

  BatchStats stats = {0};
  atomic_init(&stats.files_written, 0);
  atomic_init(&stats.files_empty, 0);
  atomic_init(&stats.unique_units, 0);
  atomic_init(&stats.duplicate_units, 0);
  atomic_init(&stats.errors, 0);
  atomic_init(&stats.processed, 0);
  atomic_init(&stats.bytes_processed, 0);

  mtx_t progress_lock;
  bool progress_lock_init = false;
  if (mtx_init(&progress_lock, mtx_plain) != thrd_success) {
    fprintf(stderr, "Failed to initialize progress mutex.\n");
    abort_scan = true;
  } else {
    progress_lock_init = true;
  }

  mtx_t tree_lock;
  bool tree_lock_init = false;
  if (mtx_init(&tree_lock, mtx_plain) != thrd_success) {
    fprintf(stderr, "Failed to initialize tree mutex.\n");
    abort_scan = true;
  } else {
    tree_lock_init = true;
  }

  if (!abort_scan && items_count > 0) {
    FileItem *batch = calloc(FILE_BATCH_SIZE, sizeof(*batch));
    if (!batch) {
      fprintf(stderr, "Failed to allocate file batch.\n");
      abort_scan = true;
    } else {
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
                           duplicates_lock_init ? &duplicates_lock : nullptr,
                           build_block_tree_flag, dedup_mode, max_compare_len,
                           &stats, items_count, start_time,
                           progress_lock_init ? &progress_lock : nullptr,
                           tree_lock_init ? &tree_lock : nullptr)) {
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
  if (duplicates_lock_init) {
    mtx_destroy(&duplicates_lock);
  }
  if (progress_lock_init) {
    mtx_destroy(&progress_lock);
  }
  if (tree_lock_init) {
    mtx_destroy(&tree_lock);
  }

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
  size_t files_written =
      atomic_load_explicit(&stats.files_written, memory_order_relaxed);
  size_t files_empty =
      atomic_load_explicit(&stats.files_empty, memory_order_relaxed);
  size_t unique_units =
      atomic_load_explicit(&stats.unique_units, memory_order_relaxed);
  size_t duplicate_units =
      atomic_load_explicit(&stats.duplicate_units, memory_order_relaxed);
  size_t worker_errors =
      atomic_load_explicit(&stats.errors, memory_order_relaxed);
  size_t total_errors = errors + worker_errors;
  size_t total_units = unique_units + duplicate_units;
  double duplicate_pct =
      total_units == 0 ? 0.0
                       : (double)duplicate_units * 100.0 / (double)total_units;
  size_t peak_rss = peak_rss_bytes();
  double peak_mib = peak_rss == 0 ? 0.0 : (double)peak_rss / (1024.0 * 1024.0);

  const char *unit_label = dedup_unit_plural(dedup_mode);
  printf("\nDedup summary (%s-level): matched %zu file(s), wrote %zu, empty "
         "%zu, unique %s %zu, duplicate %s %zu (%.2f%%), errors %zu, "
         "elapsed %.2f min, peak RSS %.2f MiB\n",
         dedup_mode_name(dedup_mode), matched, files_written, files_empty,
         unit_label, unique_units, unit_label, duplicate_units, duplicate_pct,
         total_errors, elapsed_min, peak_mib);
  return total_errors == 0 ? 0 : 1;
}
