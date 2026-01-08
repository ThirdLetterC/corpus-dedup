#ifndef SENTENCE_SPLITTER_H
#define SENTENCE_SPLITTER_H

#include <stddef.h>
#include "utf8.h"

/**
 * @brief Slice into the original UTF-8 buffer.
 */
typedef struct {
  const char8_t *start;
  size_t len;
} SentenceSpan;

/**
 * @brief Container for the list of split sentences.
 * Owns the memory of the span array (not the underlying text).
 */
typedef struct {
  SentenceSpan *sentences;
  size_t count;
  size_t capacity;
} SentenceList;

/**
 * @brief Main algorithm to split UTF-8 text into sentences.
 * @param text UTF-8 source string (may contain null bytes).
 * @param len Byte length of the source string.
 * @return SentenceList Structure containing results. User must free.
 */
SentenceList split_text_to_sentences(const char8_t *restrict text, size_t len);

/**
 * @brief Frees all memory associated with the sentence list.
 */
void free_sentence_list(SentenceList *list);

#endif
