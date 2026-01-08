#include "verify_mode.h"

#include <dirent.h>
#include <fnmatch.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ckdint_compat.h"
#include "config.h"
#include "io_utils.h"
#include "progress.h"
#include "sentence_set.h"
#include "text_utils.h"
#include "utf8.h"

static bool verify_deduped_lines(const char8_t *input, size_t len,
                                 SentenceSet *seen, size_t *out_sentences,
                                 size_t *out_duplicates, const char *label) {
  if (!seen || !out_sentences || !out_duplicates)
    return false;
  if (!input || len == 0)
    return true;

  size_t norm_cap = len > 0 ? len : 1;
  auto norm_buf = (char8_t *)calloc(norm_cap, sizeof(char8_t));
  if (!norm_buf)
    return false;

  size_t line_start = 0;
  size_t line_no = 1;
  bool reported = false;

  for (size_t i = 0; i <= len; ++i) {
    if (i == len || input[i] == (char8_t)'\n') {
      size_t line_len = i - line_start;
      size_t norm_len =
          normalize_sentence(input + line_start, line_len, norm_buf, norm_cap);
      if (norm_len > 0) {
        bool inserted = false;
        if (!sentence_set_insert(seen, norm_buf, norm_len, &inserted)) {
          free(norm_buf);
          return false;
        }
        (*out_sentences)++;
        if (!inserted) {
          (*out_duplicates)++;
          if (!reported) {
            fprintf(stderr, "Duplicate sentence in %s at line %zu\n", label,
                    line_no);
            reported = true;
          }
        }
      }
      line_start = i + 1;
      line_no++;
    }
  }

  free(norm_buf);
  return true;
}

int run_verify(const char *prog, int argc, char **argv) {
  double start_time = now_seconds();
  const char *input_dir = nullptr;
  const char *mask = DEFAULT_MASK;
  bool mask_set = false;

  for (int i = 1; i < argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
      printf("Usage:\n  %s --verify <dedup_dir> [mask]\n"
             "  ASM: WAVESORT_USE_ASM=%d HASH_WORKER_USE_ASM=%d "
             "RADIX_SORT_USE_ASM=%d\n",
             prog, WAVESORT_USE_ASM, HASH_WORKER_USE_ASM, RADIX_SORT_USE_ASM);
      return 0;
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
    printf("Usage:\n  %s --verify <dedup_dir> [mask]\n"
           "  ASM: WAVESORT_USE_ASM=%d HASH_WORKER_USE_ASM=%d "
           "RADIX_SORT_USE_ASM=%d\n",
           prog, WAVESORT_USE_ASM, HASH_WORKER_USE_ASM, RADIX_SORT_USE_ASM);
    return 1;
  }

  if (!input_dir) {
    printf("Usage:\n  %s --verify <dedup_dir> [mask]\n"
           "  ASM: WAVESORT_USE_ASM=%d HASH_WORKER_USE_ASM=%d "
           "RADIX_SORT_USE_ASM=%d\n",
           prog, WAVESORT_USE_ASM, HASH_WORKER_USE_ASM, RADIX_SORT_USE_ASM);
    return 1;
  }

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
  size_t sentences_checked = 0;
  size_t duplicate_sentences = 0;
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
    if (!verify_deduped_lines(raw_text, byte_len, &seen, &sentences_checked,
                              &duplicate_sentences, name)) {
      fprintf(stderr, "Failed to verify sentences for: %s\n", name);
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

  printf("\nVerify summary: matched %zu file(s), checked %zu, sentences %zu, "
         "duplicates %zu, errors %zu, elapsed %.2f min\n",
         matched, files_checked, sentences_checked, duplicate_sentences, errors,
         elapsed_min);

  return (errors == 0 && duplicate_sentences == 0) ? 0 : 1;
}
