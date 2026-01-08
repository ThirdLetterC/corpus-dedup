#ifndef CKDINT_COMPAT_H
#define CKDINT_COMPAT_H

#if __has_include(<stdckdint.h>)
#include <stdckdint.h>
#else
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// Minimal checked arithmetic helpers using compiler builtins.
static inline bool ckd_add(size_t *r, size_t a, size_t b) {
  return __builtin_add_overflow(a, b, r);
}
static inline bool ckd_mul(size_t *r, size_t a, size_t b) {
  return __builtin_mul_overflow(a, b, r);
}
static inline bool ckd_sub(size_t *r, size_t a, size_t b) {
  return __builtin_sub_overflow(a, b, r);
}
#endif

#endif
