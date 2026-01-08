#include "block_tree_asm_defs.h"

#if HASH_WORKER_USE_ASM
#if HASH_UNROLL == 8
.text
.intel_syntax noprefix
.globl hash_worker
.type hash_worker,@function
  .p2align 4
hash_worker:
  push rbx
  push r12
  mov rbx, [rdi + CTX_NODES]
  mov r11, [rdi + CTX_START_IDX]
  mov r12, [rdi + CTX_END_IDX]
  mov r9, [rdi + CTX_TEXT]
  mov r10, [rdi + CTX_TEXT_LEN]
  jmp .Lcheck_outer
.Louter:
  mov rdx, [rbx + r11*8]
  mov rcx, [rdx + NODE_START_POS]
  cmp rcx, r10
  jae .Lset_zero
  mov rax, [rdx + NODE_LENGTH]
  mov rsi, r10
  sub rsi, rcx
  cmp rax, rsi
  cmova rax, rsi
  lea r8, [r9 + rcx*4]
  xor rcx, rcx
  test rax, rax
  je .Lstore
.Lword_loop:
  cmp rax, 8
  jb .Lword_tail
  .p2align 4
.Lword_loop8:
#if HASH_PREFETCH_DISTANCE
  prefetcht0 [r8 + HASH_PREFETCH_DISTANCE]
#endif
  mov edi, dword ptr [r8]
  imul rdi, rdi, HASH_MULT_POW3_IMM
  mov esi, dword ptr [r8 + 4]
  imul rsi, rsi, HASH_MULT_POW2_IMM
  add rdi, rsi
  mov esi, dword ptr [r8 + 8]
  imul rsi, rsi, HASH_MULT_POW1_IMM
  add rdi, rsi
  mov esi, dword ptr [r8 + 12]
  add rdi, rsi
  imul rcx, rcx, HASH_MULT_POW4_IMM
  add rcx, rdi
  mov edi, dword ptr [r8 + 16]
  imul rdi, rdi, HASH_MULT_POW3_IMM
  mov esi, dword ptr [r8 + 20]
  imul rsi, rsi, HASH_MULT_POW2_IMM
  add rdi, rsi
  mov esi, dword ptr [r8 + 24]
  imul rsi, rsi, HASH_MULT_POW1_IMM
  add rdi, rsi
  mov esi, dword ptr [r8 + 28]
  add rdi, rsi
  imul rcx, rcx, HASH_MULT_POW4_IMM
  add rcx, rdi
  add r8, 32
  sub rax, 8
  cmp rax, 8
  jae .Lword_loop8
.Lword_tail:
  test rax, rax
  je .Lstore
.Lword_tail_loop:
  mov edi, dword ptr [r8]
  imul rcx, rcx, 31
  add rcx, rdi
  add r8, 4
  dec rax
  jne .Lword_tail_loop
.Lstore:
  mov qword ptr [rdx + NODE_BLOCK_ID], rcx
  jmp .Lnext
.Lset_zero:
  mov qword ptr [rdx + NODE_BLOCK_ID], 0
.Lnext:
  inc r11
.Lcheck_outer:
  cmp r11, r12
  jb .Louter
  xor eax, eax
  pop r12
  pop rbx
  ret
.att_syntax prefix
#else
.text
.intel_syntax noprefix
.globl hash_worker
.type hash_worker,@function
  .p2align 4
hash_worker:
  push rbx
  push r12
  mov rbx, [rdi + CTX_NODES]
  mov r11, [rdi + CTX_START_IDX]
  mov r12, [rdi + CTX_END_IDX]
  mov r9, [rdi + CTX_TEXT]
  mov r10, [rdi + CTX_TEXT_LEN]
  jmp .Lcheck_outer
.Louter:
  mov rdx, [rbx + r11*8]
  mov rcx, [rdx + NODE_START_POS]
  cmp rcx, r10
  jae .Lset_zero
  mov rax, [rdx + NODE_LENGTH]
  mov rsi, r10
  sub rsi, rcx
  cmp rax, rsi
  cmova rax, rsi
  lea r8, [r9 + rcx*4]
  xor rcx, rcx
  test rax, rax
  je .Lstore
.Lword_loop:
  cmp rax, 4
  jb .Lword_tail
  .p2align 4
.Lword_loop4:
#if HASH_PREFETCH_DISTANCE
  prefetcht0 [r8 + HASH_PREFETCH_DISTANCE]
#endif
  mov edi, dword ptr [r8]
  imul rdi, rdi, HASH_MULT_POW3_IMM
  mov esi, dword ptr [r8 + 4]
  imul rsi, rsi, HASH_MULT_POW2_IMM
  add rdi, rsi
  mov esi, dword ptr [r8 + 8]
  imul rsi, rsi, HASH_MULT_POW1_IMM
  add rdi, rsi
  mov esi, dword ptr [r8 + 12]
  add rdi, rsi
  imul rcx, rcx, HASH_MULT_POW4_IMM
  add rcx, rdi
  add r8, 16
  sub rax, 4
  cmp rax, 4
  jae .Lword_loop4
.Lword_tail:
  test rax, rax
  je .Lstore
.Lword_tail_loop:
  mov edi, dword ptr [r8]
  imul rcx, rcx, 31
  add rcx, rdi
  add r8, 4
  dec rax
  jne .Lword_tail_loop
.Lstore:
  mov qword ptr [rdx + NODE_BLOCK_ID], rcx
  jmp .Lnext
.Lset_zero:
  mov qword ptr [rdx + NODE_BLOCK_ID], 0
.Lnext:
  inc r11
.Lcheck_outer:
  cmp r11, r12
  jb .Louter
  xor eax, eax
  pop r12
  pop rbx
  ret
.att_syntax prefix
#endif
#endif
