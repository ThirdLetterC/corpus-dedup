#ifndef DEDUP_H
#define DEDUP_H

#include <stddef.h>

/**
 * Entry point for deduplication mode.
 */
int run_dedup(const char *prog, int argc, char **argv);

#endif
