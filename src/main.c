#include <string.h>

#include "dedup.h"
#include "search_mode.h"
#include "verify_mode.h"

int main(int argc, char **argv) {
  if (argc >= 2 && strcmp(argv[1], "--search") == 0) {
    return run_search(argv[0], argc - 1, argv + 1);
  }
  if (argc >= 2 && strcmp(argv[1], "--verify") == 0) {
    return run_verify(argv[0], argc - 1, argv + 1);
  }
  return run_dedup(argv[0], argc, argv);
}
