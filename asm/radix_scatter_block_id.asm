#include "block_tree_asm_defs.h"

#if RADIX_SORT_USE_ASM_IMPL
.intel_syntax noprefix
.text
.globl radix_scatter_block_id_asm
.type radix_scatter_block_id_asm,@function
radix_scatter_block_id_asm:
  test rdx, rdx
  jz 1f
  xor rax, rax
0:
  mov r9, qword ptr [rdi + rax*8]
  mov r10, qword ptr [r9 + RADIX_NODE_BLOCK_ID_OFFSET]
  shr r10, cl
  and r10, 0xFF
  lea r11, [r8 + r10*8]
  mov r10, qword ptr [r11]
  mov qword ptr [rsi + r10*8], r9
  add qword ptr [r11], 1
  inc rax
  cmp rax, rdx
  jb 0b
1:
  ret
.size radix_scatter_block_id_asm, .-radix_scatter_block_id_asm
.att_syntax prefix
#endif
