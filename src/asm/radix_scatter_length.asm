#include "block_tree_asm_defs.h"

#if RADIX_SORT_USE_ASM_IMPL
.intel_syntax noprefix
.text
.globl radix_scatter_length_asm
.type radix_scatter_length_asm,@function
radix_scatter_length_asm:
  test rdx, rdx
  jz .Ldone

  mov r10, rdi
  lea r11, [rdi + rdx*8]
  mov r9, r8

  .p2align 4
.Lloop:
  cmp r10, r11
  jae .Ldone

  mov rax, qword ptr [r10]
  mov r8, qword ptr [rax + RADIX_NODE_LENGTH_OFFSET]
  shr r8, cl
  and r8, 0xFF
  mov rdx, qword ptr [r9 + r8*8]
  mov qword ptr [rsi + rdx*8], rax
  inc rdx
  mov qword ptr [r9 + r8*8], rdx

  add r10, 8
  cmp r10, r11
  jae .Ldone

  mov rax, qword ptr [r10]
  mov r8, qword ptr [rax + RADIX_NODE_LENGTH_OFFSET]
  shr r8, cl
  and r8, 0xFF
  mov rdx, qword ptr [r9 + r8*8]
  mov qword ptr [rsi + rdx*8], rax
  inc rdx
  mov qword ptr [r9 + r8*8], rdx

  add r10, 8
  jmp .Lloop

.Ldone:
  ret
.size radix_scatter_length_asm, .-radix_scatter_length_asm
.section .note.GNU-stack,"",@progbits
.att_syntax prefix
#endif
