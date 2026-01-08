#include "progress.h"

#include <inttypes.h>
#include <stdio.h>
#include <time.h>

double now_seconds(void) {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    return 0.0;
  return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

uint64_t now_ns(void) {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    return 0;
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

void print_duration_ns(uint64_t ns) {
  if (ns < 1000ull) {
    printf("%" PRIu64 " ns", ns);
    return;
  }
  if (ns < 1000ull * 1000ull) {
    printf("%.3f us", (double)ns / 1e3);
    return;
  }
  if (ns < 1000ull * 1000ull * 1000ull) {
    printf("%.3f ms", (double)ns / 1e6);
    return;
  }
  double seconds = (double)ns / 1e9;
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
  const int bar_width = 30;
  static double last_update = 0.0;
  double now = now_seconds();
  if (now > 0.0 && done != 0 && done != total && now - last_update < 0.1) {
    return;
  }
  last_update = now;
  double elapsed = now - start_time;
  if (elapsed < 0.0001)
    elapsed = 0.0001;
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
