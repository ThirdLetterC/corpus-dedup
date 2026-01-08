#ifndef SENTENCE_SPLITTER_H
#define SENTENCE_SPLITTER_H

#include <stddef.h>

/**
 * @brief Container for the list of split sentences.
 * Owns the memory of the string array and the strings themselves.
 */
typedef struct {
  char **sentences;
  size_t count;
  size_t capacity;
} SentenceList;

/**
 * @brief Main algorithm to split UTF-8 text into sentences.
 * @param text Null-terminated UTF-8 source string.
 * @return SentenceList Structure containing results. User must free.
 */
SentenceList split_text_to_sentences(const char *restrict text);

/**
 * @brief Frees all memory associated with the sentence list.
 */
void free_sentence_list(SentenceList *list);

#endif
