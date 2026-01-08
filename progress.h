#ifndef PROGRESS_H
#define PROGRESS_H

#include <stddef.h>
#include <stdint.h>

double now_seconds(void);
uint64_t now_ns(void);
void print_duration_ns(uint64_t ns);
void render_progress(size_t done, size_t total, size_t bytes_done,
                     double start_time);

#endif
