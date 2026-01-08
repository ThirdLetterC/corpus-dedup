#include "sentence_splitter.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uchar.h>

#if defined(__AVX2__) || defined(__SSE2__)
#include <immintrin.h>
#endif
// --- Constants & Helpers ---

static const size_t k_init_capacity = 16;

/**
 * @brief Checks if a code point is a distinct CJK/Wide sentence terminator.
 * Includes: 。 (3002), ？ (FF1F), ！ (FF01), … (2026), ؟ (061F), ｡ (FF61)
 */
static inline bool is_immediate_terminator(char32_t cp) {
  return (cp == 0x3002 || cp == 0xFF1F || cp == 0xFF01 || cp == 0x2026 ||
          cp == 0x061F || cp == 0xFF61);
}

/**
 * @brief Checks if a code point is standard whitespace.
 * Note: Uses wctype's iswspace but casts safely.
 */
static inline bool is_basic_white_space(char32_t cp) {
  if (cp <= 0x20)
    return true;
  if (cp == 0x00A0 || cp == 0x1680 || cp == 0x3000)
    return true;
  if (cp >= 0x2000 && cp <= 0x200A)
    return true;
  if (cp == 0x2028 || cp == 0x2029 || cp == 0x202F || cp == 0x205F)
    return true;
  return false;
}

static inline bool is_ascii_alpha(unsigned char c) {
  unsigned char folded = (unsigned char)(c | 0x20);
  return folded >= 'a' && folded <= 'z';
}

static inline bool is_ascii_lower(unsigned char c) {
  return c >= 'a' && c <= 'z';
}

static inline unsigned char ascii_tolower(unsigned char c) {
  if (c >= 'A' && c <= 'Z')
    return (unsigned char)(c + 32);
  return c;
}

static inline bool is_ascii_closer(unsigned char c) {
  return (c == '"' || c == '\'' || c == ')' || c == ']' || c == '}');
}

static inline bool is_unicode_closer(char32_t cp) {
  return (cp == 0x00BB || cp == 0x2019 || cp == 0x201D || cp == 0x300D ||
          cp == 0x300F || cp == 0x3009 || cp == 0x300B || cp == 0x3011 ||
          cp == 0x3015 || cp == 0x3017 || cp == 0x3019 || cp == 0x301B ||
          cp == 0xFF09 || cp == 0xFF3D || cp == 0xFF5D);
}

static inline size_t decode_utf8(const unsigned char *p, size_t len,
                                 char32_t *out_cp);

static inline const char *skip_white_space(const char *p, const char *end) {
  while (p < end) {
    unsigned char c = (unsigned char)p[0];
    if (c <= 0x20) {
      p++;
      continue;
    }
    if (c < 0x80)
      return p;
    char32_t cp;
    size_t bytes =
        decode_utf8((const unsigned char *)p, (size_t)(end - p), &cp);
    if (bytes == 0)
      return p;
    if (is_basic_white_space(cp)) {
      p += bytes;
      continue;
    }
    return p;
  }
  return p;
}

static inline const char *skip_closing_punct(const char *p, const char *end) {
  while (p < end) {
    unsigned char c = (unsigned char)p[0];
    if (c < 0x80) {
      if (is_ascii_closer(c)) {
        p++;
        continue;
      }
      return p;
    }
    char32_t cp;
    size_t bytes =
        decode_utf8((const unsigned char *)p, (size_t)(end - p), &cp);
    if (bytes == 0)
      return p;
    if (is_unicode_closer(cp)) {
      p += bytes;
      continue;
    }
    return p;
  }
  return p;
}

static inline bool is_common_abbrev(const char *start, size_t len) {
  if (len == 2) {
    unsigned char c0 = ascii_tolower((unsigned char)start[0]);
    unsigned char c1 = ascii_tolower((unsigned char)start[1]);
    if (!is_ascii_alpha(c0) || !is_ascii_alpha(c1))
      return false;
    return ((c0 == 'm' && c1 == 'r') || (c0 == 'm' && c1 == 's') ||
            (c0 == 'd' && c1 == 'r') || (c0 == 'v' && c1 == 's') ||
            (c0 == 'j' && c1 == 'r') || (c0 == 's' && c1 == 'r') ||
            (c0 == 's' && c1 == 't') || (c0 == 'm' && c1 == 't'));
  }
  if (len == 3) {
    unsigned char c0 = ascii_tolower((unsigned char)start[0]);
    unsigned char c1 = ascii_tolower((unsigned char)start[1]);
    unsigned char c2 = ascii_tolower((unsigned char)start[2]);
    if (!is_ascii_alpha(c0) || !is_ascii_alpha(c1) || !is_ascii_alpha(c2))
      return false;
    return ((c0 == 'm' && c1 == 'r' && c2 == 's') ||
            (c0 == 'e' && c1 == 't' && c2 == 'c'));
  }
  return false;
}

static inline bool should_block_split_on_dot(const char *sentence_start,
                                             const char *dot_pos,
                                             const char *next_non_space,
                                             const char *end) {
  if (next_non_space >= end)
    return false;
  size_t len = 0;
  const char *p = dot_pos;
  while (p > sentence_start) {
    unsigned char c = (unsigned char)p[-1];
    if (!is_ascii_alpha(c))
      break;
    len++;
    if (len > 3)
      break;
    p--;
  }
  if (len == 0 || len > 3)
    return false;
  if (is_ascii_lower((unsigned char)next_non_space[0]))
    return true;
  return is_common_abbrev(dot_pos - len, len);
}

static inline size_t decode_utf8(const unsigned char *p, size_t len,
                                 char32_t *out_cp) {
  if (len == 0)
    return 0;
  unsigned char c0 = p[0];
  if (c0 < 0x80) {
    *out_cp = (char32_t)c0;
    return 1;
  }
  if ((c0 & 0xE0) == 0xC0) {
    if (len < 2)
      return 0;
    unsigned char c1 = p[1];
    if ((c1 & 0xC0) != 0x80)
      return 0;
    char32_t cp = (char32_t)(((c0 & 0x1F) << 6) | (c1 & 0x3F));
    if (cp < 0x80)
      return 0;
    *out_cp = cp;
    return 2;
  }
  if ((c0 & 0xF0) == 0xE0) {
    if (len < 3)
      return 0;
    unsigned char c1 = p[1];
    unsigned char c2 = p[2];
    if ((c1 & 0xC0) != 0x80 || (c2 & 0xC0) != 0x80)
      return 0;
    char32_t cp = (char32_t)(((c0 & 0x0F) << 12) | ((c1 & 0x3F) << 6) |
                             (c2 & 0x3F));
    if (cp < 0x800 || (cp >= 0xD800 && cp <= 0xDFFF))
      return 0;
    *out_cp = cp;
    return 3;
  }
  if ((c0 & 0xF8) == 0xF0) {
    if (len < 4)
      return 0;
    unsigned char c1 = p[1];
    unsigned char c2 = p[2];
    unsigned char c3 = p[3];
    if ((c1 & 0xC0) != 0x80 || (c2 & 0xC0) != 0x80 ||
        (c3 & 0xC0) != 0x80)
      return 0;
    char32_t cp = (char32_t)(((c0 & 0x07) << 18) | ((c1 & 0x3F) << 12) |
                             ((c2 & 0x3F) << 6) | (c3 & 0x3F));
    if (cp < 0x10000 || cp > 0x10FFFF)
      return 0;
    *out_cp = cp;
    return 4;
  }
  return 0;
}

static inline size_t find_next_event_ascii(const unsigned char *p, size_t len) {
  size_t i = 0;
#if defined(__AVX2__)
  const __m256i dot = _mm256_set1_epi8('.');
  const __m256i excl = _mm256_set1_epi8('!');
  const __m256i quest = _mm256_set1_epi8('?');
  while (i + 32 <= len) {
    __m256i v = _mm256_loadu_si256((const __m256i *)(p + i));
    __m256i m0 = _mm256_cmpeq_epi8(v, dot);
    __m256i m1 = _mm256_cmpeq_epi8(v, excl);
    __m256i m2 = _mm256_cmpeq_epi8(v, quest);
    __m256i m = _mm256_or_si256(_mm256_or_si256(m0, m1), m2);
    unsigned int mask =
        (unsigned int)_mm256_movemask_epi8(m) |
        (unsigned int)_mm256_movemask_epi8(v);
    if (mask) {
      return i + (size_t)__builtin_ctz(mask);
    }
    i += 32;
  }
#elif defined(__SSE2__)
  const __m128i dot = _mm_set1_epi8('.');
  const __m128i excl = _mm_set1_epi8('!');
  const __m128i quest = _mm_set1_epi8('?');
  while (i + 16 <= len) {
    __m128i v = _mm_loadu_si128((const __m128i *)(p + i));
    __m128i m0 = _mm_cmpeq_epi8(v, dot);
    __m128i m1 = _mm_cmpeq_epi8(v, excl);
    __m128i m2 = _mm_cmpeq_epi8(v, quest);
    __m128i m = _mm_or_si128(_mm_or_si128(m0, m1), m2);
    unsigned int mask =
        (unsigned int)_mm_movemask_epi8(m) |
        (unsigned int)_mm_movemask_epi8(v);
    if (mask) {
      return i + (size_t)__builtin_ctz(mask);
    }
    i += 16;
  }
#endif
  for (; i < len; ++i) {
    unsigned char c = p[i];
    if (c == '.' || c == '!' || c == '?' || c >= 0x80)
      return i;
  }
  return len;
}

// --- Core Logic ---

/**
 * @brief Appends a string slice to the sentence list.
 */
static void add_sentence(SentenceList *list, const char *start,
                         size_t length) {
  if (length == 0)
    return;
  // Grow capacity if needed
  if (list->count >= list->capacity) {
    size_t new_cap = list->capacity == 0 ? k_init_capacity : list->capacity * 2;
    SentenceSpan *new_data =
        realloc(list->sentences, new_cap * sizeof(*list->sentences));
    if (!new_data)
      return; // Allocation failure handling strategy: skip or crash safely
    list->sentences = new_data;
    list->capacity = new_cap;
  }

  list->sentences[list->count++] =
      (SentenceSpan){.start = start, .len = length};
}

/**
 * @brief Main algorithm to split UTF-8 text into sentences.
 * @param text UTF-8 source string (may contain null bytes).
 * @param len Byte length of the source string.
 * @return SentenceList Structure containing results. User must free.
 */
SentenceList split_text_to_sentences(const char *restrict text, size_t len) {
  SentenceList list = {0};
  if (!text || len == 0)
    return list;

  if (len >= 256) {
    size_t estimate = len / 128;
    if (estimate < k_init_capacity)
      estimate = k_init_capacity;
    SentenceSpan *reserved =
        realloc(list.sentences, estimate * sizeof(*list.sentences));
    if (reserved) {
      list.sentences = reserved;
      list.capacity = estimate;
    }
  }

  const char *cursor = text;
  const char *sentence_start = text;
  const char *end = text + len;

  char32_t current_cp;

  sentence_start = skip_white_space(sentence_start, end);
  cursor = sentence_start;

  // We iterate until the cursor hits the end
  while (cursor < end) {
    unsigned char byte0 = (unsigned char)cursor[0];
    size_t bytes_read = 1;
    bool split_here = false;

    if (byte0 < 0x80) {
      size_t remaining = (size_t)(end - cursor);
      size_t offset =
          find_next_event_ascii((const unsigned char *)cursor, remaining);
      if (offset == remaining) {
        cursor = end;
        break;
      }
      cursor += offset;
      byte0 = (unsigned char)cursor[0];
      if (byte0 < 0x80) {
        const char *next_cursor = cursor + 1;
        const char *after_closers = skip_closing_punct(next_cursor, end);
        const char *ws = skip_white_space(after_closers, end);
        if (after_closers >= end) {
          split_here = true;
        } else if (ws > after_closers) {
          if (byte0 == '.') {
            if (!should_block_split_on_dot(sentence_start, cursor, ws, end))
              split_here = true;
          } else {
            split_here = true;
          }
        }
        bytes_read = 1;
        if (split_here) {
          size_t len = (size_t)(after_closers - sentence_start);
          add_sentence(&list, sentence_start, len);
          sentence_start = ws;
          cursor = sentence_start;
        } else {
          cursor = (ws > after_closers) ? ws : after_closers;
        }
        continue;
      }
    }

    // Fast-path common CJK terminators: 。, ？, ！
    if (byte0 == 0xE3 && cursor + 2 < end &&
        (unsigned char)cursor[1] == 0x80 &&
        (unsigned char)cursor[2] == 0x82) {
      bytes_read = 3;
      split_here = true;
    } else if (byte0 == 0xEF && cursor + 2 < end &&
               (unsigned char)cursor[1] == 0xBC &&
               ((unsigned char)cursor[2] == 0x9F ||
                (unsigned char)cursor[2] == 0x81)) {
      bytes_read = 3;
      split_here = true;
    } else {
      // 1. Decode current character
      bytes_read = decode_utf8((const unsigned char *)cursor,
                               (size_t)(end - cursor), &current_cp);

      // Handle decoding errors or incomplete sequences
      if (bytes_read == 0) {
        cursor++;
        continue;
      }

      if (is_immediate_terminator(current_cp)) {
        split_here = true;
      }
    }

    const char *next_cursor = cursor + bytes_read;

    // 3. Execute Split
    if (split_here) {
      const char *after_closers = skip_closing_punct(next_cursor, end);
      // Length includes the terminator (current byte sequence)
      size_t len = (size_t)(after_closers - sentence_start);
      add_sentence(&list, sentence_start, len);

      // Reset start for next sentence
      sentence_start = skip_white_space(after_closers, end);
      cursor = sentence_start;
      continue;
    }

    // Advance
    cursor = next_cursor;
  }

  // 4. Handle remaining text (if any)
  if (cursor > sentence_start) {
    add_sentence(&list, sentence_start, (size_t)(cursor - sentence_start));
  }

  return list;
}

/**
 * @brief Frees all memory associated with the sentence list.
 */
void free_sentence_list(SentenceList *list) {
  if (!list || !list->sentences)
    return;
  free(list->sentences);
  list->sentences = nullptr;
  list->count = 0;
  list->capacity = 0;
}

#ifdef SENTENCE_SPLITTER_DEMO
int main(void) {
  // Example text containing English, numbers, and Japanese
  const char *article =
      "Hello World. This is a test... with numbers 3.14 included. "
      "Also some Japanese: これはテストです。Unicode is handled correctly!";

  SentenceList results = split_text_to_sentences(article, strlen(article));

  printf("Found %zu sentences:\n", results.count);
  for (size_t i = 0; i < results.count; i++) {
    printf("[%zu]: %.*s\n", i + 1, (int)results.sentences[i].len,
           results.sentences[i].start);
  }

  free_sentence_list(&results);
  return EXIT_SUCCESS;
}
#endif
