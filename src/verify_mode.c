#include <dirent.h>
#include <errno.h>
#include <fnmatch.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ckdint_compat.h"
#include "config.h"
#include "io_utils.h"
#include "progress.h"
#include "sentence_set.h"
#include "sentence_splitter.h"
#include "text_utils.h"
#include "utf8.h"
#include "verify_mode.h"

constexpr size_t SPAN_INIT_CAP = 16;

typedef enum {
  DEDUP_MODE_SENTENCE = 0,
  DEDUP_MODE_LINE = 1,
  DEDUP_MODE_PARAGRAPH = 2,
  DEDUP_MODE_DOCUMENT = 3
} DedupMode;

static void print_verify_help(const char *prog) {
  printf("Usage:\n  %s --verify <dedup_dir> [mask] [--dedup-mode "
         "<sentence|line|paragraph|document>] [--max-length N]\n"
         "  --max-length defaults to %zu symbols (0 is unlimited)\n"
         "  ASM: WAVESORT_USE_ASM=%d HASH_WORKER_USE_ASM=%d "
         "RADIX_SORT_USE_ASM=%d\n"
         "  Author: %s\n"
         "  License: %s\n"
         "  Copyright: %s\n",
         prog, DEFAULT_MAX_COMPARE_LENGTH, WAVESORT_USE_ASM, HASH_WORKER_USE_ASM,
         RADIX_SORT_USE_ASM, PROGRAM_AUTHOR, PROGRAM_LICENSE_NAME,
         PROGRAM_COPYRIGHT);
}

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

static const char *dedup_unit_singular(DedupMode mode) {
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

static bool verify_spans(DedupMode mode, const char8_t *input, size_t input_len,
                         const SentenceSpan *spans, size_t span_count,
                         size_t max_compare_len, SentenceSet *seen,
                         size_t *out_units, size_t *out_duplicates,
                         const char *label) {
  if (!seen || !out_units || !out_duplicates)
    return false;
  if (!input || input_len == 0 || span_count == 0)
    return true;
  if (!spans)
    return false;

  size_t norm_cap = input_len == 0 ? 1 : input_len;
  if (max_compare_len != 0 && max_compare_len < norm_cap) {
    norm_cap = max_compare_len;
  }
  size_t alloc_size = 0;
  if (ckd_mul(&alloc_size, norm_cap, sizeof(char8_t)))
    return false;
  auto norm_buf = (char8_t *)calloc(1, alloc_size);
  if (!norm_buf)
    return false;

  bool reported = false;
  const char *unit_label = dedup_unit_singular(mode);

  for (size_t i = 0; i < span_count; ++i) {
    size_t norm_len =
        normalize_sentence(spans[i].start, spans[i].len, norm_buf, norm_cap);
    if (max_compare_len != 0 && norm_len > max_compare_len) {
      norm_len = max_compare_len;
    }
    if (norm_len == 0)
      continue;
    bool inserted = false;
    if (!sentence_set_insert(seen, norm_buf, norm_len, &inserted)) {
      free(norm_buf);
      return false;
    }
    (*out_units)++;
    if (!inserted) {
      (*out_duplicates)++;
      if (!reported) {
        fprintf(stderr, "Duplicate %s in %s at %zu\n", unit_label, label,
                i + 1);
        reported = true;
      }
    }
  }

  free(norm_buf);
  return true;
}

static bool verify_sentences(const char8_t *input, size_t len,
                             SentenceSet *seen, size_t *out_units,
                             size_t *out_duplicates, const char *label,
                             size_t max_compare_len) {
  SentenceList sentences = split_text_to_sentences(input, len);
  bool ok = verify_spans(DEDUP_MODE_SENTENCE, input, len, sentences.sentences,
                         sentences.count, max_compare_len, seen, out_units,
                         out_duplicates, label);
  free_sentence_list(&sentences);
  return ok;
}

static bool verify_paragraphs(const char8_t *input, size_t len,
                              SentenceSet *seen, size_t *out_units,
                              size_t *out_duplicates, const char *label,
                              size_t max_compare_len) {
  SpanList paragraphs = {0};
  if (!split_text_to_paragraphs(input, len, &paragraphs)) {
    return false;
  }

  bool ok = verify_spans(DEDUP_MODE_PARAGRAPH, input, len, paragraphs.items,
                         paragraphs.count, max_compare_len, seen, out_units,
                         out_duplicates, label);
  free_span_list(&paragraphs);
  return ok;
}

static bool verify_lines(const char8_t *input, size_t len, SentenceSet *seen,
                         size_t *out_units, size_t *out_duplicates,
                         const char *label, size_t max_compare_len) {
  SpanList lines = {0};
  if (!split_text_to_lines(input, len, &lines)) {
    return false;
  }

  bool ok =
      verify_spans(DEDUP_MODE_LINE, input, len, lines.items, lines.count,
                   max_compare_len, seen, out_units, out_duplicates, label);
  free_span_list(&lines);
  return ok;
}

static bool verify_document(const char8_t *input, size_t len, SentenceSet *seen,
                            size_t *out_units, size_t *out_duplicates,
                            const char *label, size_t max_compare_len) {
  SentenceSpan single = {.start = input, .len = len};
  size_t span_count = input && len > 0 ? 1 : 0;
  return verify_spans(DEDUP_MODE_DOCUMENT, input, len, &single, span_count,
                      max_compare_len, seen, out_units, out_duplicates, label);
}

static bool verify_with_mode(DedupMode mode, const char8_t *input, size_t len,
                             size_t max_compare_len, SentenceSet *seen,
                             size_t *out_units, size_t *out_duplicates,
                             const char *label) {
  switch (mode) {
  case DEDUP_MODE_DOCUMENT:
    return verify_document(input, len, seen, out_units, out_duplicates, label,
                           max_compare_len);
  case DEDUP_MODE_LINE:
    return verify_lines(input, len, seen, out_units, out_duplicates, label,
                        max_compare_len);
  case DEDUP_MODE_PARAGRAPH:
    return verify_paragraphs(input, len, seen, out_units, out_duplicates, label,
                             max_compare_len);
  case DEDUP_MODE_SENTENCE:
  default:
    return verify_sentences(input, len, seen, out_units, out_duplicates, label,
                            max_compare_len);
  }
}

int run_verify(const char *prog, int argc, char **argv) {
  double start_time = now_seconds();
  const char *input_dir = nullptr;
  const char *mask = DEFAULT_MASK;
  bool mask_set = false;
  DedupMode dedup_mode = DEDUP_MODE_SENTENCE;
  size_t max_compare_len = DEFAULT_MAX_COMPARE_LENGTH;

  for (int i = 1; i < argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
      print_verify_help(prog);
      return 0;
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
        fprintf(stderr, "--dedup-mode requires one of: sentence, line, "
                        "paragraph, document\n");
        return 1;
      }
      const char *mode_arg = argv[++i];
      if (!parse_dedup_mode(mode_arg, &dedup_mode)) {
        fprintf(stderr,
                "Invalid --dedup-mode value: %s (expected sentence, "
                "line, paragraph, or document)\n",
                mode_arg);
        return 1;
      }
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
    print_verify_help(prog);
    return 1;
  }

  if (!input_dir) {
    print_verify_help(prog);
    return 1;
  }

  const char *unit_label = dedup_unit_plural(dedup_mode);

  if (!ensure_directory(input_dir, false)) {
    return 1;
  }

  SentenceSet seen = {0};
  if (!sentence_set_init(&seen, 1024)) {
    fprintf(stderr, "Failed to allocate dedup index.\n");
    return 1;
  }

  size_t matched = 0;
  size_t files_checked = 0;
  size_t units_checked = 0;
  size_t duplicate_units = 0;
  size_t errors = 0;
  size_t bytes_processed = 0;
  size_t processed = 0;

  DIR *dir = opendir(input_dir);
  if (!dir) {
    fprintf(stderr, "Failed to open input directory: %s\n", input_dir);
    sentence_set_destroy(&seen);
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
      fprintf(stderr, "Failed to allocate input path for: %s\n", name);
      errors++;
      continue;
    }
    if (!is_regular_file(input_path)) {
      free(input_path);
      continue;
    }

    matched++;
    free(input_path);
  }

  closedir(dir);
  dir = opendir(input_dir);
  if (!dir) {
    fprintf(stderr, "Failed to open input directory: %s\n", input_dir);
    sentence_set_destroy(&seen);
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
      fprintf(stderr, "Failed to allocate input path for: %s\n", name);
      errors++;
      continue;
    }
    if (!is_regular_file(input_path)) {
      free(input_path);
      continue;
    }

    char8_t *raw_text = nullptr;
    size_t byte_len = 0;
    if (!read_file_bytes(input_path, &raw_text, &byte_len)) {
      errors++;
      free(input_path);
      processed++;
      render_progress(processed, matched, bytes_processed, start_time);
      continue;
    }

    sentence_set_reserve_for_bytes(&seen, byte_len);
    if (!verify_with_mode(dedup_mode, raw_text, byte_len, max_compare_len,
                          &seen, &units_checked, &duplicate_units, name)) {
      fprintf(stderr, "Failed to verify %s-level duplicates for: %s\n",
              dedup_mode_name(dedup_mode), name);
      errors++;
    }

    free(raw_text);
    free(input_path);
    files_checked++;
    bytes_processed += byte_len;
    processed++;
    render_progress(processed, matched, bytes_processed, start_time);
  }

  closedir(dir);
  sentence_set_destroy(&seen);

  double elapsed = now_seconds() - start_time;
  if (elapsed < 0.0)
    elapsed = 0.0;
  double elapsed_min = elapsed / 60.0;

  printf("\nVerify summary (%s-level): matched %zu file(s), checked %zu, %s "
         "%zu, duplicates %zu, errors %zu, elapsed %.2f min\n",
         dedup_mode_name(dedup_mode), matched, files_checked, unit_label,
         units_checked, duplicate_units, errors, elapsed_min);

  return (errors == 0 && duplicate_units == 0) ? 0 : 1;
}
