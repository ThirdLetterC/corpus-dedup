#include "utf8.h"

#include <stdlib.h>

size_t utf8_decode_advance(const char8_t *bytes, size_t len,
                           uint32_t *out_codepoint, bool *out_invalid) {
  if (!bytes || len == 0)
    return 0;
  uint8_t b0 = bytes[0];
  uint32_t codepoint = 0xFFFD;
  size_t advance = 1;

  if (b0 < 0x80) {
    codepoint = b0;
  } else if ((b0 & 0xE0) == 0xC0 && len >= 2) {
    uint8_t b1 = bytes[1];
    if ((b1 & 0xC0) == 0x80) {
      codepoint = ((uint32_t)(b0 & 0x1F) << 6) | (uint32_t)(b1 & 0x3F);
      if (codepoint >= 0x80) {
        advance = 2;
      } else {
        codepoint = 0xFFFD;
      }
    }
  } else if ((b0 & 0xF0) == 0xE0 && len >= 3) {
    uint8_t b1 = bytes[1];
    uint8_t b2 = bytes[2];
    if ((b1 & 0xC0) == 0x80 && (b2 & 0xC0) == 0x80) {
      codepoint = ((uint32_t)(b0 & 0x0F) << 12) | ((uint32_t)(b1 & 0x3F) << 6) |
                  (uint32_t)(b2 & 0x3F);
      if (codepoint >= 0x800 && (codepoint < 0xD800 || codepoint > 0xDFFF)) {
        advance = 3;
      } else {
        codepoint = 0xFFFD;
      }
    }
  } else if ((b0 & 0xF8) == 0xF0 && len >= 4) {
    uint8_t b1 = bytes[1];
    uint8_t b2 = bytes[2];
    uint8_t b3 = bytes[3];
    if ((b1 & 0xC0) == 0x80 && (b2 & 0xC0) == 0x80 && (b3 & 0xC0) == 0x80) {
      codepoint = ((uint32_t)(b0 & 0x07) << 18) |
                  ((uint32_t)(b1 & 0x3F) << 12) | ((uint32_t)(b2 & 0x3F) << 6) |
                  (uint32_t)(b3 & 0x3F);
      if (codepoint >= 0x10000 && codepoint <= 0x10FFFF) {
        advance = 4;
      } else {
        codepoint = 0xFFFD;
      }
    }
  }

  if (out_codepoint)
    *out_codepoint = codepoint;
  if (out_invalid)
    *out_invalid = (codepoint == 0xFFFD && b0 >= 0x80);
  return advance;
}

[[nodiscard]] bool utf8_decode_buffer(const char8_t *input, size_t len,
                                      uint32_t **out, size_t *out_len,
                                      size_t *invalid_count) {
  if (!out || !out_len || !invalid_count)
    return false;
  *out = NULL;
  *out_len = 0;
  *invalid_count = 0;

  if (!input || len == 0)
    return true;
  if (len > SIZE_MAX / sizeof(uint32_t))
    return false;

  uint32_t *buffer = malloc(len * sizeof(uint32_t));
  if (!buffer)
    return false;

  const uint8_t *bytes = (const uint8_t *)input;
  size_t i = 0;
  size_t count = 0;
  size_t invalid = 0;

  while (i < len) {
    uint32_t codepoint = 0;
    bool invalid_codepoint = false;
    size_t advance =
        utf8_decode_advance(bytes + i, len - i, &codepoint, &invalid_codepoint);
    if (advance == 0)
      break;
    if (invalid_codepoint)
      invalid++;
    buffer[count++] = codepoint;
    i += advance;
  }

  *out = buffer;
  *out_len = count;
  *invalid_count = invalid;
  return true;
}
