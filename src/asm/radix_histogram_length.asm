#include "block_tree_asm_defs.h"

#if RADIX_SORT_USE_ASM_IMPL
.intel_syntax noprefix
.text
.globl radix_histogram_length_asm
.type radix_histogram_length_asm,@function
radix_histogram_length_asm:
  test rsi, rsi
  jz .Ldone

  mov r8, rcx
  mov ecx, edx
  lea r10, [rdi + rsi*8]
  mov r9, rdi

  .p2align 4
.Lloop:
  cmp r9, r10
  jae .Ldone

  mov rax, qword ptr [r9]
  mov r11, qword ptr [rax + RADIX_NODE_LENGTH_OFFSET]
  shr r11, cl
  and r11, 0xFF
  add qword ptr [r8 + r11*8], 1

  add r9, 8
  cmp r9, r10
  jae .Ldone

  mov rax, qword ptr [r9]
  mov r11, qword ptr [rax + RADIX_NODE_LENGTH_OFFSET]
  shr r11, cl
  and r11, 0xFF
  add qword ptr [r8 + r11*8], 1

  add r9, 8
  jmp .Lloop

.Ldone:
  ret
.size radix_histogram_length_asm, .-radix_histogram_length_asm
.section .note.GNU-stack,"",@progbits
.att_syntax prefix
#endif
