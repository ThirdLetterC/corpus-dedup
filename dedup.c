#include "dedup.h"

#include <dirent.h>
#include <errno.h>
#include <fnmatch.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "arena.h"
#include "block_tree.h"
#include "ckdint_compat.h"
#include "config.h"
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

static bool emit_sentence(const char8_t *data, size_t len, SentenceSet *seen,
                          char8_t *norm_buf, size_t norm_cap, char8_t *out_buf,
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
      out_buf[(*out_pos)++] = (char8_t)'\n';
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

static bool deduplicate_sentences(const char8_t *input, size_t len,
                                  SentenceSet *seen, char8_t **out,
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

  size_t out_cap = 0;
  if (ckd_mul(&out_cap, len, (size_t)2) ||
      ckd_add(&out_cap, out_cap, (size_t)1)) {
    return false;
  }

  auto buffer = (char8_t *)calloc(out_cap, sizeof(char8_t));
  if (!buffer)
    return false;
  auto norm_buf = (char8_t *)calloc(len, sizeof(char8_t));
  if (!norm_buf) {
    free(buffer);
    return false;
  }

  size_t out_pos = 0;
  SentenceList sentences = split_text_to_sentences(input, len);

  for (size_t i = 0; i < sentences.count; ++i) {
    const char8_t *sentence = sentences.sentences[i].start;
    size_t sentence_len = sentences.sentences[i].len;
    if (!emit_sentence(sentence, sentence_len, seen, norm_buf, len, buffer,
                       &out_pos, out_cap, out_unique, out_duplicates,
                       duplicates_fp)) {
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

static bool process_batch(FileItem *batch, size_t batch_count,
                          const char *output_dir, SentenceSet *seen,
                          FILE *duplicates_fp, bool build_tree,
                          size_t *files_written, size_t *files_empty,
                          size_t *unique_sentences, size_t *duplicate_sentences,
                          size_t *errors, size_t *processed,
                          size_t *bytes_processed, size_t total_files,
                          double start_time) {
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

    char8_t *deduped = nullptr;
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

    if (build_tree) {
      if (!process_text(item->name, deduped, deduped_len, false)) {
        (*errors)++;
      }
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

int run_dedup(int argc, char **argv) {
  double overall_start = now_seconds();
  const char *input_dir = nullptr;
  const char *output_dir = nullptr;
  const char *mask = DEFAULT_MASK;
  bool mask_set = false;
  bool write_duplicates = false;
  bool build_block_tree_flag = false;

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
    if (strcmp(arg, "--help") == 0 || strcmp(arg, "-h") == 0) {
      printf("Usage:\n"
             "  %s <input_dir> <output_dir> [mask] [--write-duplicates] "
             "[--build-block-tree]\n"
             "  ASM: WAVESORT_USE_ASM=%d HASH_WORKER_USE_ASM=%d "
             "RADIX_SORT_USE_ASM=%d\n",
             argv[0], WAVESORT_USE_ASM, HASH_WORKER_USE_ASM,
             RADIX_SORT_USE_ASM);
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
           "  %s <input_dir> <output_dir> [mask] [--write-duplicates] "
           "[--build-block-tree]\n"
           "  ASM: WAVESORT_USE_ASM=%d HASH_WORKER_USE_ASM=%d "
           "RADIX_SORT_USE_ASM=%d\n",
           argv[0], WAVESORT_USE_ASM, HASH_WORKER_USE_ASM, RADIX_SORT_USE_ASM);
    return 1;
  }

  if (!input_dir || !output_dir) {
    printf("Usage:\n"
           "  %s <input_dir> <output_dir> [mask] [--write-duplicates] "
           "[--build-block-tree]\n",
           argv[0]);
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
                           build_block_tree_flag, &files_written, &files_empty,
                           &unique_sentences, &duplicate_sentences, &errors,
                           &processed, &bytes_processed, items_count,
                           start_time)) {
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
