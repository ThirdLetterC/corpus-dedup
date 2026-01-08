#include "block_tree_asm_defs.h"

#if HASH_WORKER_USE_ASM
.extern g_prefix_table
.extern g_pow_table
.extern g_prefix_size
#if HASH_UNROLL == 8
.text
.intel_syntax noprefix
.globl hash_worker
.type hash_worker,@function
  .p2align 4
hash_worker:
  push rbx
  push r12
  push r13
  push r14
  push r15
  mov rbx, [rdi + CTX_NODES]
  mov r11, [rdi + CTX_START_IDX]
  mov r12, [rdi + CTX_END_IDX]
  mov r9, [rdi + CTX_TEXT]
  mov r10, [rdi + CTX_TEXT_LEN]
  mov r13, qword ptr [rip + g_prefix_table]
  mov r14, qword ptr [rip + g_pow_table]
  mov r15, qword ptr [rip + g_prefix_size]
  test r13, r13
  je .Lscalar_check_outer
  test r14, r14
  je .Lscalar_check_outer
  mov rax, r10
  inc rax
  cmp r15, rax
  jb .Lscalar_check_outer
  test r10, r10
  je .Lscalar_check_outer
  jmp .Lprefix_check_outer
.Lprefix_outer:
  mov rdi, [rbx + r11*8]
  mov rcx, [rdi + NODE_START_POS]
  cmp rcx, r10
  jae .Lprefix_set_zero
  mov rax, [rdi + NODE_LENGTH]
  mov rsi, r10
  sub rsi, rcx
  cmp rax, rsi
  cmova rax, rsi
  mov rdx, rax
  lea r8, [rcx + rax]
  mov rsi, r15
  dec rsi
  cmp r8, rsi
  cmova r8, rsi
  mov rax, [r13 + rcx*8]
  mov rsi, [r14 + rdx*8]
  mul rsi
  mov rsi, [r13 + r8*8]
  sub rsi, rax
  mov [rdi + NODE_BLOCK_ID], rsi
  jmp .Lprefix_next
.Lprefix_set_zero:
  mov qword ptr [rdi + NODE_BLOCK_ID], 0
.Lprefix_next:
  inc r11
.Lprefix_check_outer:
  cmp r11, r12
  jb .Lprefix_outer
  jmp .Ldone
.Lscalar_outer:
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
.Lscalar_check_outer:
  cmp r11, r12
  jb .Lscalar_outer
.Ldone:
  xor eax, eax
  pop r15
  pop r14
  pop r13
  pop r12
  pop rbx
  ret
.section .note.GNU-stack,"",@progbits
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
  push r13
  push r14
  push r15
  mov rbx, [rdi + CTX_NODES]
  mov r11, [rdi + CTX_START_IDX]
  mov r12, [rdi + CTX_END_IDX]
  mov r9, [rdi + CTX_TEXT]
  mov r10, [rdi + CTX_TEXT_LEN]
  mov r13, qword ptr [rip + g_prefix_table]
  mov r14, qword ptr [rip + g_pow_table]
  mov r15, qword ptr [rip + g_prefix_size]
  test r13, r13
  je .Lscalar_check_outer4
  test r14, r14
  je .Lscalar_check_outer4
  mov rax, r10
  inc rax
  cmp r15, rax
  jb .Lscalar_check_outer4
  test r10, r10
  je .Lscalar_check_outer4
  jmp .Lprefix_check_outer4
.Lprefix_outer4:
  mov rdi, [rbx + r11*8]
  mov rcx, [rdi + NODE_START_POS]
  cmp rcx, r10
  jae .Lprefix_set_zero4
  mov rax, [rdi + NODE_LENGTH]
  mov rsi, r10
  sub rsi, rcx
  cmp rax, rsi
  cmova rax, rsi
  mov rdx, rax
  lea r8, [rcx + rax]
  mov rsi, r15
  dec rsi
  cmp r8, rsi
  cmova r8, rsi
  mov rax, [r13 + rcx*8]
  mov rsi, [r14 + rdx*8]
  mul rsi
  mov rsi, [r13 + r8*8]
  sub rsi, rax
  mov [rdi + NODE_BLOCK_ID], rsi
  jmp .Lprefix_next4
.Lprefix_set_zero4:
  mov qword ptr [rdi + NODE_BLOCK_ID], 0
.Lprefix_next4:
  inc r11
.Lprefix_check_outer4:
  cmp r11, r12
  jb .Lprefix_outer4
  jmp .Ldone4
.Lscalar_outer4:
  mov rdx, [rbx + r11*8]
  mov rcx, [rdx + NODE_START_POS]
  cmp rcx, r10
  jae .Lset_zero4
  mov rax, [rdx + NODE_LENGTH]
  mov rsi, r10
  sub rsi, rcx
  cmp rax, rsi
  cmova rax, rsi
  lea r8, [r9 + rcx*4]
  xor rcx, rcx
  test rax, rax
  je .Lstore4
.Lword_loop4:
  cmp rax, 4
  jb .Lword_tail4
  .p2align 4
.Lword_loop4_body:
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
  jae .Lword_loop4_body
.Lword_tail4:
  test rax, rax
  je .Lstore4
.Lword_tail_loop4:
  mov edi, dword ptr [r8]
  imul rcx, rcx, 31
  add rcx, rdi
  add r8, 4
  dec rax
  jne .Lword_tail_loop4
.Lstore4:
  mov qword ptr [rdx + NODE_BLOCK_ID], rcx
  jmp .Lnext4
.Lset_zero4:
  mov qword ptr [rdx + NODE_BLOCK_ID], 0
.Lnext4:
  inc r11
.Lscalar_check_outer4:
  cmp r11, r12
  jb .Lscalar_outer4
.Ldone4:
  xor eax, eax
  pop r15
  pop r14
  pop r13
  pop r12
  pop rbx
  ret
.section .note.GNU-stack,"",@progbits
.att_syntax prefix
#endif
#endif
