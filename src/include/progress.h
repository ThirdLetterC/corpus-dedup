#ifndef PROGRESS_H
#define PROGRESS_H

#include <stddef.h>
#include <stdint.h>

/**
 * Wall-clock time in seconds since an unspecified epoch.
 */
[[nodiscard]] double now_seconds();
/**
 * Monotonic nanoseconds counter for timing.
 */
[[nodiscard]] uint64_t now_ns();
/**
 * Print a human-readable duration given nanoseconds.
 */
void print_duration_ns(uint64_t ns);
/**
 * Render a progress bar with counts and throughput.
 */
void render_progress(size_t done, size_t total, size_t bytes_done,
                     double start_time);

#endif
