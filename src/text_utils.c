#include "text_utils.h"

#include <string.h>

static inline bool is_ascii_space(unsigned char c) { return c <= 0x20; }

size_t normalize_sentence(const char8_t *data, size_t len, char8_t *out,
                          size_t out_cap) {
  size_t start = 0;
  while (start < len && is_ascii_space((unsigned char)data[start])) {
    start++;
  }
  size_t end = len;
  while (end > start && is_ascii_space((unsigned char)data[end - 1])) {
    end--;
  }

  size_t out_len = 0;
  bool in_space = false;
  for (size_t i = start; i < end; ++i) {
    if (is_ascii_space((unsigned char)data[i])) {
      if (!in_space) {
        if (out_len < out_cap)
          out[out_len++] = (char8_t)' ';
        in_space = true;
      }
      continue;
    }
    if (out_len < out_cap)
      out[out_len++] = data[i];
    in_space = false;
  }
  return out_len;
}

void trim_line(char *line) {
  if (!line)
    return;
  size_t len = strlen(line);
  while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
    line[len - 1] = '\0';
    len--;
  }
}
