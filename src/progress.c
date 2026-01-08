#define _POSIX_C_SOURCE 200809L

#include "progress.h"

#include <inttypes.h>
#include <stdio.h>
#include <time.h>

static constexpr uint64_t NS_PER_SEC = 1'000'000'000ull;
static constexpr uint64_t NS_PER_MS = 1'000'000ull;
static constexpr uint64_t NS_PER_US = 1'000ull;
static constexpr double MIN_ELAPSED = 0.0001;
static constexpr double UPDATE_INTERVAL = 0.1;

double now_seconds() {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    return 0.0;
  return (double)ts.tv_sec + (double)ts.tv_nsec / (double)NS_PER_SEC;
}

uint64_t now_ns() {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    return 0;
  return (uint64_t)ts.tv_sec * NS_PER_SEC + (uint64_t)ts.tv_nsec;
}

void print_duration_ns(uint64_t ns) {
  if (ns < NS_PER_US) {
    printf("%" PRIu64 " ns", ns);
    return;
  }
  if (ns < NS_PER_MS) {
    printf("%.3f us", (double)ns / (double)NS_PER_US);
    return;
  }
  if (ns < NS_PER_SEC) {
    printf("%.3f ms", (double)ns / (double)NS_PER_MS);
    return;
  }
  double seconds = (double)ns / (double)NS_PER_SEC;
  if (seconds < 60.0) {
    printf("%.3f s", seconds);
    return;
  }
  double minutes = seconds / 60.0;
  if (minutes < 60.0) {
    printf("%.2f min", minutes);
    return;
  }
  double hours = minutes / 60.0;
  printf("%.2f h", hours);
}

void render_progress(size_t done, size_t total, size_t bytes_done,
                     double start_time) {
  constexpr int bar_width = 30;
  static double last_update = 0.0;
  double now = now_seconds();
  if (now > 0.0 && done != 0 && done != total &&
      now - last_update < UPDATE_INTERVAL) {
    return;
  }
  last_update = now;
  double elapsed = now - start_time;
  if (elapsed < MIN_ELAPSED)
    elapsed = MIN_ELAPSED;
  double rate = (double)done / elapsed;
  double mb_done = (double)bytes_done / (1024.0 * 1024.0);
  double mb_rate = mb_done / elapsed;
  double pct = (total > 0) ? (double)done * 100.0 / (double)total : 0.0;
  int filled =
      (total > 0) ? (int)((double)bar_width * (double)done / (double)total) : 0;
  if (filled > bar_width)
    filled = bar_width;
  double eta = 0.0;
  if (total > done && rate > 0.0001) {
    eta = (double)(total - done) / rate;
  }
  double eta_minutes = eta / 60.0;

  fprintf(stderr, "\r[");
  for (int i = 0; i < bar_width; ++i) {
    fputc(i < filled ? '#' : '-', stderr);
  }
  fprintf(stderr, "] %zu/%zu %5.1f%% %.2f docs/s %.2f MB/s ETA %.1fm", done,
          total, pct, rate, mb_rate, eta_minutes);
  fflush(stderr);
}
