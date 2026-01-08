cmake_minimum_required(VERSION 3.20)
project(block_tree C)

option(USE_ASM "Enable NASM/asm fast paths" ON)
set(HASH_UNROLL 8 CACHE STRING "Unroll factor for hash worker asm (4 or 8)")
set(HASH_PREFETCH_DISTANCE 256 CACHE STRING "Prefetch distance in bytes for hash worker asm")

set(SRC
    main.c
    block_tree_core.c
    dedup.c
    search_mode.c
    verify_mode.c
    config.c
    arena.c
    hash_pool.c
    hash_utils.c
    io_utils.c
    node_sort.c
    progress.c
    sentence_set.c
    sentence_splitter.c
    text_utils.c
    utf8.c
)

add_executable(corpus_dedup ${SRC})
target_compile_features(corpus_dedup PRIVATE c_std_23)
set_target_properties(corpus_dedup PROPERTIES C_STANDARD 23 C_STANDARD_REQUIRED YES)
target_include_directories(corpus_dedup PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

find_package(Threads REQUIRED)
target_link_libraries(corpus_dedup PRIVATE Threads::Threads)

if(USE_ASM)
  enable_language(ASM_NASM)
  set(ASM_OUT
    ${CMAKE_CURRENT_BINARY_DIR}/hash_worker.o
    ${CMAKE_CURRENT_BINARY_DIR}/radix_histogram_length.o
    ${CMAKE_CURRENT_BINARY_DIR}/radix_scatter_length.o
    ${CMAKE_CURRENT_BINARY_DIR}/radix_histogram_block_id.o
    ${CMAKE_CURRENT_BINARY_DIR}/radix_scatter_block_id.o
    ${CMAKE_CURRENT_BINARY_DIR}/wavesort.o
  )

  add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hash_worker.o
    COMMAND ${CMAKE_C_COMPILER} -x assembler-with-cpp -c
            -DHASH_UNROLL=${HASH_UNROLL}
            -DHASH_PREFETCH_DISTANCE=${HASH_PREFETCH_DISTANCE}
            ${CMAKE_CURRENT_SOURCE_DIR}/hash_worker.asm
            -o ${CMAKE_CURRENT_BINARY_DIR}/hash_worker.o
    DEPENDS hash_worker.asm block_tree_asm_defs.h
    COMMENT "Assembling hash_worker.asm with ${CMAKE_C_COMPILER}"
  )
  foreach(f radix_histogram_length radix_scatter_length radix_histogram_block_id radix_scatter_block_id)
    add_custom_command(
      OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${f}.o
      COMMAND ${CMAKE_C_COMPILER} -x assembler-with-cpp -c
              ${CMAKE_CURRENT_SOURCE_DIR}/${f}.asm
              -o ${CMAKE_CURRENT_BINARY_DIR}/${f}.o
      DEPENDS ${f}.asm block_tree_asm_defs.h
      COMMENT "Assembling ${f}.asm with ${CMAKE_C_COMPILER}"
    )
  endforeach()
  add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/wavesort.o
    COMMAND ${CMAKE_ASM_NASM_COMPILER} -f elf64
            ${CMAKE_CURRENT_SOURCE_DIR}/wavesort.asm
            -o ${CMAKE_CURRENT_BINARY_DIR}/wavesort.o
    DEPENDS wavesort.asm
    COMMENT "Assembling wavesort.asm with NASM"
  )
  add_custom_target(asm_objs ALL DEPENDS ${ASM_OUT})
  add_dependencies(corpus_dedup asm_objs)
  target_sources(corpus_dedup PRIVATE ${ASM_OUT})
  target_compile_definitions(corpus_dedup PRIVATE
    WAVESORT_USE_ASM=1
    HASH_WORKER_USE_ASM=1
    RADIX_SORT_USE_ASM=1
    RADIX_SORT_USE_ASM_IMPL=1
    HASH_PREFETCH_DISTANCE=${HASH_PREFETCH_DISTANCE}
    HASH_UNROLL=${HASH_UNROLL}
  )
else()
  target_compile_definitions(corpus_dedup PRIVATE
    WAVESORT_USE_ASM=0
    HASH_WORKER_USE_ASM=0
    RADIX_SORT_USE_ASM=0
    RADIX_SORT_USE_ASM_IMPL=0
  )
endif()

target_compile_options(corpus_dedup PRIVATE
  $<$<CONFIG:Release>:-O3 -DNDEBUG -march=native>
  $<$<CONFIG:Debug>:-O0 -g>
)
