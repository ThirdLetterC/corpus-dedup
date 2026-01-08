#include "sentence_splitter.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uchar.h>
#include <wctype.h>

#if defined(__AVX2__) || defined(__SSE2__)
#include <immintrin.h>
#endif
// --- Constants & Helpers ---

static const size_t k_init_capacity = 16;

/**
 * @brief Checks if a code point is a distinct CJK/Wide sentence terminator.
 * Includes: 。 (3002), ？ (FF1F), ！ (FF01)
 */
static inline bool is_immediate_terminator(char32_t cp) {
  return (cp == 0x3002 || cp == 0xFF1F || cp == 0xFF01);
}

/**
 * @brief Checks if a code point is a Latin sentence terminator.
 * Includes: . ? !
 */
static inline bool is_latin_terminator(char32_t cp) {
  return (cp == U'.' || cp == U'?' || cp == U'!');
}

/**
 * @brief Checks if a code point is standard whitespace.
 * Note: Uses wctype's iswspace but casts safely.
 */
static inline bool is_white_space(char32_t cp) {
  return (cp < 0x10000 && iswspace((wint_t)cp));
}

static inline bool is_ascii_white_space(unsigned char c) {
  return c <= 0x20;
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
static void add_sentence(SentenceList *list, const char *start, size_t length) {
  // Grow capacity if needed
  if (list->count >= list->capacity) {
    size_t new_cap = list->capacity == 0 ? k_init_capacity : list->capacity * 2;
    char **new_data = realloc(list->sentences, new_cap * sizeof(char *));
    if (!new_data)
      return; // Allocation failure handling strategy: skip or crash safely
    list->sentences = new_data;
    list->capacity = new_cap;
  }

  // Allocate and copy the substring
  char *segment = malloc(length + 1);
  if (segment) {
    memcpy(segment, start, length);
    segment[length] = '\0';

    // Simple trim of leading whitespace (optional, but cleaner)
    char *final_ptr = segment;
    size_t trimmed_len = length;
    while (trimmed_len > 0 && (unsigned char)*final_ptr <= 32) {
      final_ptr++;
      trimmed_len--;
    }

    if (final_ptr != segment) {
      memmove(segment, final_ptr, trimmed_len);
      segment[trimmed_len] = '\0';
    }

    if (trimmed_len > 0) {
      list->sentences[list->count++] = segment;
    } else {
      free(segment); // Don't add empty strings
    }
  }
}

/**
 * @brief Main algorithm to split UTF-8 text into sentences.
 * * @param text Null-terminated UTF-8 source string.
 * @return SentenceList Structure containing results. User must free.
 */
SentenceList split_text_to_sentences(const char *restrict text) {
  SentenceList list = {0};
  if (!text)
    return list;

  mbstate_t state = {0};
  const char *cursor = text;
  const char *sentence_start = text;
  const char *end = text + strlen(text);

  char32_t current_cp, next_cp;

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
        if (next_cursor >= end) {
          split_here = true;
        } else {
          unsigned char next_byte = (unsigned char)next_cursor[0];
          if (is_ascii_white_space(next_byte)) {
            split_here = true;
          } else if (next_byte >= 0x80) {
            mbstate_t peek_state = state;
            size_t next_bytes =
                mbrtoc32(&next_cp, next_cursor, end - next_cursor, &peek_state);
            if (next_bytes == 0 || next_bytes == (size_t)-1 ||
                next_bytes == (size_t)-2 || is_white_space(next_cp)) {
              split_here = true;
            }
          }
        }
        bytes_read = 1;
        if (split_here) {
          size_t len = (size_t)(next_cursor - sentence_start);
          add_sentence(&list, sentence_start, len);
          sentence_start = next_cursor;
        }
        cursor = next_cursor;
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
      bytes_read = mbrtoc32(&current_cp, cursor, end - cursor, &state);

      // Handle decoding errors or incomplete sequences
      if (bytes_read == (size_t)-1 || bytes_read == (size_t)-2) {
        break; // Stop on invalid UTF-8
      }
      if (bytes_read == 0)
        break; // Null terminator

      if (is_immediate_terminator(current_cp)) {
        split_here = true;
      } else if (is_latin_terminator(current_cp)) {
        const char *next_cursor = cursor + bytes_read;
        mbstate_t peek_state = state;
        size_t next_bytes =
            mbrtoc32(&next_cp, next_cursor, end - next_cursor, &peek_state);

        bool is_end = (next_cursor >= end) || (next_bytes == 0);

        if (is_end || is_white_space(next_cp)) {
          split_here = true;
        }
      }
    }

    const char *next_cursor = cursor + bytes_read;

    // 3. Execute Split
    if (split_here) {
      // Length includes the terminator (current byte sequence)
      size_t len = (size_t)(next_cursor - sentence_start);
      add_sentence(&list, sentence_start, len);

      // Reset start for next sentence
      sentence_start = next_cursor;
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
  for (size_t i = 0; i < list->count; ++i) {
    free(list->sentences[i]);
  }
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

  SentenceList results = split_text_to_sentences(article);

  printf("Found %zu sentences:\n", results.count);
  for (size_t i = 0; i < results.count; i++) {
    printf("[%zu]: %s\n", i + 1, results.sentences[i]);
  }

  free_sentence_list(&results);
  return EXIT_SUCCESS;
}
#endif
