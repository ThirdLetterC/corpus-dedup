#include "io_utils.h"
#include "utf8.h"

#include "ckdint_compat.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

bool read_file_bytes(const char *path, char8_t **out, size_t *out_len) {
  if (!path || !out || !out_len)
    return false;

  FILE *fp = fopen(path, "rb");
  if (!fp) {
    fprintf(stderr, "Failed to open input file: %s\n", path);
    return false;
  }

  if (fseek(fp, 0, SEEK_END) != 0) {
    fprintf(stderr, "Failed to seek input file: %s\n", path);
    fclose(fp);
    return false;
  }
  long file_size = ftell(fp);
  if (file_size < 0) {
    fprintf(stderr, "Failed to get input file size: %s\n", path);
    fclose(fp);
    return false;
  }
  if (fseek(fp, 0, SEEK_SET) != 0) {
    fprintf(stderr, "Failed to rewind input file: %s\n", path);
    fclose(fp);
    return false;
  }

  size_t byte_len = (size_t)file_size;
  size_t alloc_size = 0;
  if (ckd_add(&alloc_size, byte_len, (size_t)1)) {
    fprintf(stderr, "Input file too large: %s\n", path);
    fclose(fp);
    return false;
  }
  char8_t *buffer = (char8_t *)calloc(alloc_size, sizeof(char8_t));
  if (!buffer) {
    fprintf(stderr, "Failed to allocate text buffer for: %s\n", path);
    fclose(fp);
    return false;
  }

  size_t read_count = fread(buffer, 1, byte_len, fp);
  fclose(fp);
  if (read_count != byte_len) {
    fprintf(stderr, "Failed to read full input file: %s\n", path);
    free(buffer);
    return false;
  }

  for (size_t i = 0; i < byte_len; ++i) {
    if (buffer[i] == (char8_t)'\n' || buffer[i] == (char8_t)'\r') {
      buffer[i] = (char8_t)' ';
    }
  }
  buffer[byte_len] = (char8_t)'\0';

  *out = buffer;
  *out_len = byte_len;
  return true;
}

bool write_file_bytes(const char *path, const char8_t *data, size_t len) {
  FILE *fp = fopen(path, "wb");
  if (!fp) {
    fprintf(stderr, "Failed to open output file: %s\n", path);
    return false;
  }
  size_t written = fwrite(data, 1, len, fp);
  if (written != len) {
    fprintf(stderr, "Failed to write full output file: %s\n", path);
    fclose(fp);
    return false;
  }
  if (fclose(fp) != 0) {
    fprintf(stderr, "Failed to close output file: %s\n", path);
    return false;
  }
  return true;
}

char *dup_string(const char *input) {
  if (!input)
    return nullptr;
  const size_t len = strlen(input);
  size_t alloc_size = 0;
  if (ckd_add(&alloc_size, len, (size_t)1))
    return nullptr;
  auto out = (char *)calloc(alloc_size, sizeof(char));
  if (!out)
    return nullptr;
  memcpy(out, input, len);
  out[len] = '\0';
  return out;
}

char *join_path(const char *dir, const char *name) {
  if (!dir || !name)
    return nullptr;
  const size_t dir_len = strlen(dir);
  const size_t name_len = strlen(name);
  const bool needs_sep = (dir_len > 0 && dir[dir_len - 1] != '/');
  size_t total = 0;
  if (ckd_add(&total, dir_len, name_len))
    return nullptr;
  if (needs_sep && ckd_add(&total, total, (size_t)1))
    return nullptr;
  if (ckd_add(&total, total, (size_t)1))
    return nullptr;
  auto path = (char *)calloc(total, sizeof(char));
  if (!path)
    return nullptr;

  memcpy(path, dir, dir_len);
  size_t pos = dir_len;
  if (needs_sep)
    path[pos++] = '/';
  memcpy(path + pos, name, name_len);
  path[pos + name_len] = '\0';
  return path;
}

bool is_regular_file(const char *path) {
  struct stat st;
  if (stat(path, &st) != 0) {
    return false;
  }
  return S_ISREG(st.st_mode);
}

bool ensure_directory(const char *path, bool create) {
  struct stat st;
  if (stat(path, &st) == 0) {
    if (!S_ISDIR(st.st_mode)) {
      fprintf(stderr, "Not a directory: %s\n", path);
      return false;
    }
    return true;
  }
  if (!create) {
    fprintf(stderr, "Directory not found: %s\n", path);
    return false;
  }
  if (errno != ENOENT) {
    fprintf(stderr, "Failed to stat directory: %s\n", path);
    return false;
  }
  if (mkdir(path, 0755) != 0) {
    fprintf(stderr, "Failed to create directory: %s\n", path);
    return false;
  }
  return true;
}
