; ==============================================================================
; W-Sort - Highly Optimized AMD64 Assembly Implementation
; Target: AMD64 (x86_64) with AVX2 support
; Enhancements:
;   - AVX2 Vectorized Block Swaps (block_swap_sr) Unrolled x2
;   - AVX2 Partitioning Unrolled x2 with Correct Mask Logic
;   - Cache Prefetching (partition)
;   - 16-byte Loop Alignment
; ==============================================================================

section .text
global wave_sort

; ==============================================================================
; Helper Macros and Constants
; ==============================================================================

%define SIZEOF_INT 4

; ==============================================================================
; Function: swap
; RDI = int32_t *a
; RSI = int32_t *b
; ==============================================================================
align 16
swap:
    mov     eax, [rdi]
    mov     ecx, [rsi]
    mov     [rdi], ecx
    mov     [rsi], eax
    ret

; ==============================================================================
; Function: block_swap_sl
; Signature: void block_swap_sl(int32_t *arr, size_t m, size_t p, size_t ll)
; Params: RDI=arr, RSI=m, RDX=p, RCX=ll
; Note: This implements a "Juggling Algorithm" logic which is inherently scalar.
; ==============================================================================
align 16
block_swap_sl:
    push    r12
    push    r13
    push    rbx

    ; RDI=arr, RSI=m, RDX=p, RCX=ll

    mov     r8d, [rdi + rsi*4]  ; tmp = arr[m]
    mov     r9, rsi             ; init = m
    mov     r10, rsi            ; j = m
    
    ; nm = p - ll + 1
    mov     r11, rdx
    sub     r11, rcx
    inc     r11

    ; total_len = p - m + 1
    mov     r13, rdx
    sub     r13, rsi
    inc     r13

    xor     r12, r12            ; count = 0

    align 16
.loop_body:
    cmp     r12, r13
    jge     .exit_sl

    cmp     r10, r11            ; if (j >= nm)
    jb      .sl_else

    ; k = j - nm + m
    mov     rbx, r10
    sub     rbx, r11
    add     rbx, rsi

    cmp     rbx, r9             ; if (k == init)
    jne     .sl_cycle_cont

    ; init++; arr[j] = tmp; j = init; tmp = arr[j];
    inc     r9
    mov     [rdi + r10*4], r8d
    mov     r10, r9
    mov     r8d, [rdi + r10*4]
    jmp     .sl_next

.sl_cycle_cont:
    ; arr[j] = arr[k]; j = k;
    mov     eax, [rdi + rbx*4]
    mov     [rdi + r10*4], eax
    mov     r10, rbx
    jmp     .sl_next

.sl_else:
    ; size_t k = j + ll;
    mov     rbx, r10
    add     rbx, rcx
    
    ; arr[j] = arr[k]; j = k;
    mov     eax, [rdi + rbx*4]
    mov     [rdi + r10*4], eax
    mov     r10, rbx

.sl_next:
    inc     r12
    jmp     .loop_body

.exit_sl:
    pop     rbx
    pop     r13
    pop     r12
    ret

; ==============================================================================
; Function: block_swap_sr
; Signature: void block_swap_sr(int32_t *restrict arr, size_t m, size_t r, size_t p)
; Params: RDI=arr, RSI=m, RDX=r, RCX=p
; Optimized: Uses AVX2 Unrolled (16 elements/iter)
; Logic:
;   while (j < p) {
;       arr[i] = arr[j];
;       i++;
;       arr[j] = arr[i]; // effectively arr[old_i + 1]
;       j++;
;   }
; ==============================================================================
align 16
block_swap_sr:
    ; i = m  (RSI)
    ; tmp = arr[i]
    mov     r8d, [rdi + rsi*4] ; r8d = tmp
    ; j = r  (RDX)

    ; Check if we can use AVX2 Unrolled (need at least 16 elements)
    ; condition: j + 16 <= p
    lea     rax, [rdx + 16]
    cmp     rax, rcx
    ja      .sr_check_single_vec

    align 16
.sr_avx_unrolled_loop:
    ; Loop Guard: check if j + 16 <= p
    lea     rax, [rdx + 16]
    cmp     rax, rcx
    ja      .sr_check_single_vec

    ; UNROLLED BLOCK 1 (First 8 elements)
    ; 1. Load 8 elements from arr[j] -> YMM0
    vmovdqu ymm0, [rdi + rdx*4]
    ; 2. Load 8 elements from arr[i+1] -> YMM1 (Unaligned)
    vmovdqu ymm1, [rdi + rsi*4 + 4]
    
    ; UNROLLED BLOCK 2 (Next 8 elements)
    ; 3. Load 8 elements from arr[j+8] -> YMM2
    vmovdqu ymm2, [rdi + rdx*4 + 32]
    ; 4. Load 8 elements from arr[i+9] -> YMM3 (Unaligned)
    vmovdqu ymm3, [rdi + rsi*4 + 36] ; (i+8)+1 = i+9. Offset 32+4=36.

    ; STORE BLOCK 1
    ; 5. Store YMM0 to arr[i]
    vmovdqu [rdi + rsi*4], ymm0
    ; 6. Store YMM1 to arr[j]
    vmovdqu [rdi + rdx*4], ymm1

    ; STORE BLOCK 2
    ; 7. Store YMM2 to arr[i+8]
    vmovdqu [rdi + rsi*4 + 32], ymm2
    ; 8. Store YMM3 to arr[j+8]
    vmovdqu [rdi + rdx*4 + 32], ymm3

    ; Increment indices by 16
    add     rsi, 16
    add     rdx, 16
    jmp     .sr_avx_unrolled_loop

.sr_check_single_vec:
    ; Check if we can process remaining 8 elements
    lea     rax, [rdx + 8]
    cmp     rax, rcx
    ja      .sr_scalar_loop

    ; Single Vector Block (8 elements)
    vmovdqu ymm0, [rdi + rdx*4]
    vmovdqu ymm1, [rdi + rsi*4 + 4]
    vmovdqu [rdi + rsi*4], ymm0
    vmovdqu [rdi + rdx*4], ymm1
    add     rsi, 8
    add     rdx, 8

    align 16
.sr_scalar_loop:
    cmp     rdx, rcx ; while (j < p)
    jge     .sr_done
    
    ; arr[i] = arr[j]
    mov     r9d, [rdi + rdx*4]
    mov     [rdi + rsi*4], r9d
    
    inc     rsi      ; i++
    
    ; arr[j] = arr[i]
    mov     r9d, [rdi + rsi*4]
    mov     [rdi + rdx*4], r9d
    
    inc     rdx      ; j++
    jmp     .sr_scalar_loop

.sr_done:
    ; arr[i] = arr[j]
    mov     r9d, [rdi + rdx*4]
    mov     [rdi + rsi*4], r9d
    
    ; arr[j] = tmp;
    mov     [rdi + rdx*4], r8d
    ret

; ==============================================================================
; Function: block_swap
; Signature: void block_swap(int32_t *arr, size_t m, size_t r, size_t p)
; Params: RDI=arr, RSI=m, RDX=r, RCX=p
; ==============================================================================
align 16
block_swap:
    ; size_t ll = r - m;
    mov     r8, rdx
    sub     r8, rsi  ; r8 = ll
    jz      .bs_ret  ; if (ll == 0) return

    ; size_t lr = p - r + 1;
    mov     r9, rcx
    sub     r9, rdx
    inc     r9       ; r9 = lr

    cmp     r9, 1
    jne     .bs_check_size
    
    ; if (lr == 1) swap(&arr[m], &arr[p]);
    lea     rax, [rdi + rsi*4]
    lea     rdx, [rdi + rcx*4]
    mov     r8d, [rax]
    mov     r9d, [rdx]
    mov     [rax], r9d
    mov     [rdx], r8d
    ret

.bs_check_size:
    ; if (lr <= ll) block_swap_sr(arr, m, r, p);
    cmp     r9, r8
    ja      .bs_call_sl
    
    call    block_swap_sr
    ret

.bs_call_sl:
    ; block_swap_sl(arr, m, p, ll)
    mov     rdx, rcx ; p
    mov     rcx, r8  ; ll
    call    block_swap_sl
.bs_ret:
    ret

; ==============================================================================
; Function: partition
; Signature: size_t partition(int32_t *arr, size_t l, size_t r, size_t p_idx)
; Returns: i (RAX)
; Optimized: AVX2 Unrolled Scanning (2 vectors/iter) + Prefetch
; ==============================================================================
align 16
partition:
    ; pivot_val = arr[p_idx]
    mov     r10d, [rdi + rcx*4]

    ; i = l - 1
    mov     rax, rsi
    dec     rax      ; rax = i

    ; j = r
    mov     r8, rdx  ; r8 = j

    ; Prepare AVX2 Pivot
    vmovd   xmm0, r10d
    vpbroadcastd ymm0, xmm0

    align 16
.part_loop:
    ; --- Inner Loop i ---

.scan_i:
    inc     rax         ; i++
    cmp     rax, r8     ; if (i == j)
    je      .part_done

    ; Check if at least 16 elements (2 vectors) can be scanned
    mov     r9, r8
    sub     r9, rax
    cmp     r9, 16
    jl      .scan_i_single_check

    ; Prefetch cache lines (128 bytes ahead)
    prefetcht0 [rdi + rax*4 + 128]

    ; Load 2 vectors (16 elements)
    vmovdqu ymm1, [rdi + rax*4]       ; i ... i+7
    vmovdqu ymm2, [rdi + rax*4 + 32]  ; i+8 ... i+15

    ; Compare both against pivot
    ; VPCMPGTD dest, src1, src2 -> dest = (src1 > src2) -> (Pivot > Val)
    ; We want to stop if (Val >= Pivot) -> NOT (Pivot > Val)
    ; So we need mask bits to be 1. If any is 0, we stop.
    
    vpcmpgtd ymm3, ymm0, ymm1
    vpcmpgtd ymm4, ymm0, ymm2

    ; Extract masks individually. Do NOT vpor.
    vpmovmskb r9d, ymm3
    vpmovmskb r11d, ymm4

    ; Check if both are "all 1s" (0xFFFFFFFF for dwords, but vpmovmskb on ymm gives 32 bits)
    ; If (r9d == -1) AND (r11d == -1), then we continue.
    ; Use RCX as temp (it was p_idx, now free).
    mov     ecx, r9d
    and     ecx, r11d
    not     ecx         ; If result is 0, then both were -1 (all 1s).
    
    test    ecx, ecx
    jnz     .found_i_unrolled

    ; Both vectors clean, advance 16
    add      rax, 15    ; +16 total (inc rax was +1)
    jmp      .scan_i

.found_i_unrolled:
    ; One of the elements is >= Pivot. Find it.
    ; Check first vector (r9d)
    not      r9d
    test     r9d, r9d
    jnz      .resolve_i_vec1

    ; Must be in second vector (r11d)
    not      r11d
    tzcnt    r11d, r11d
    shr      r11d, 2
    add      rax, 8     ; Offset for second vector
    add      rax, r11
    jmp      .break_i

.resolve_i_vec1:
    tzcnt    r9d, r9d
    shr      r9d, 2
    add      rax, r9
    jmp      .break_i

.scan_i_single_check:
    ; Fallback for < 16 elements
    cmp     r9, 8
    jl      .scalar_i_check
    
    vmovdqu ymm1, [rdi + rax*4]
    vpcmpgtd ymm2, ymm0, ymm1
    vpmovmskb r9d, ymm2
    not     r9d
    test    r9d, r9d
    jz      .advance_i_single

    tzcnt   r9d, r9d
    shr     r9d, 2
    add     rax, r9
    jmp     .break_i

.advance_i_single:
    add     rax, 7
    jmp     .scan_i

.scalar_i_check:
    mov     r11d, [rdi + rax*4]
    cmp     r11d, r10d
    jge     .break_i
    jmp     .scan_i

.break_i:
    ; --- Inner Loop j ---

.scan_j:
    dec     r8          ; j--
    cmp     r8, rax
    je      .part_done

    ; Check if at least 16 elements
    mov     r9, r8
    sub     r9, rax
    cmp     r9, 16
    jl      .scan_j_single_check

    prefetcht0 [rdi + r8*4 - 128]

    ; Load 2 vectors ending at j
    ; Vec1: [j-7 ... j]     -> addr: (j-7)*4
    ; Vec2: [j-15 ... j-8]  -> addr: (j-15)*4 = (j-7)*4 - 32
    
    mov     r9, r8
    sub     r9, 7
    
    vmovdqu ymm1, [rdi + r9*4]      ; Vec1 (High indices)
    vmovdqu ymm2, [rdi + r9*4 - 32] ; Vec2 (Low indices)

    ; We want to stop if (Val <= Pivot).
    ; VPCMPGTD mask, Val, Pivot -> 1s if Val > Pivot (Keep going)
    ; 0s if Val <= Pivot (STOP)

    vpcmpgtd ymm3, ymm1, ymm0
    vpcmpgtd ymm4, ymm2, ymm0

    vpmovmskb r11d, ymm3    ; Vec1 mask (High)
    vpmovmskb r9d, ymm4     ; Vec2 mask (Low)

    mov     ecx, r11d
    and     ecx, r9d
    not     ecx
    test    ecx, ecx
    jnz     .found_j_unrolled

    sub     r8, 15      ; -16 total
    jmp     .scan_j

.found_j_unrolled:
    ; Check Vec1 (High indices) first because we scan backwards (j--)
    not      r11d
    test     r11d, r11d
    jnz      .resolve_j_vec1

    ; Must be in Vec2 (Low indices)
    not      r9d
    bsr      r9d, r9d
    shr      r9d, 2
    
    ; Adjust j. Base for Vec2 is (j-15).
    sub      r8, 15
    add      r8, r9
    jmp      .break_j

.resolve_j_vec1:
    bsr      r11d, r11d
    shr      r11d, 2
    ; Base for Vec1 is (j-7).
    sub      r8, 7
    add      r8, r11
    jmp      .break_j

.scan_j_single_check:
    cmp     r9, 8
    jl      .scalar_j_check

    mov     r9, r8
    sub     r9, 7
    vmovdqu ymm1, [rdi + r9*4]
    
    vpcmpgtd ymm2, ymm1, ymm0
    vpmovmskb r11d, ymm2
    not     r11d
    test    r11d, r11d
    jz      .advance_j_single
    
    bsr     r11d, r11d
    shr     r11d, 2
    sub     r8, 7
    add     r8, r11
    jmp     .break_j

.advance_j_single:
    sub     r8, 7
    jmp     .scan_j

.scalar_j_check:
    mov     r11d, [rdi + r8*4]
    cmp     r11d, r10d
    jle     .break_j
    jmp     .scan_j

.break_j:
    ; swap(&arr[i], &arr[j])
    mov     r9d, [rdi + rax*4]
    mov     r11d, [rdi + r8*4]
    mov     [rdi + rax*4], r11d
    mov     [rdi + r8*4], r9d
    
    jmp     .part_loop

.part_done:
    ret

; ==============================================================================
; Function: downwave (Recursion)
; ==============================================================================
align 16
downwave:
    push    rbp
    push    rbx
    push    r12
    push    r13
    push    r14
    push    r15
    sub     rsp, 8

    cmp     rdx, rsi
    je      .dw_return

    mov     rbx, rdi ; arr
    mov     r12, rsi ; start
    mov     r13, rdx ; sorted_start
    mov     r14, rcx ; end

    ; p = sorted_start + (end - sorted_start) / 2
    mov     rax, r14
    sub     rax, r13
    shr     rax, 1
    add     rax, r13
    mov     r15, rax ; p

    ; partition
    mov     rdi, rbx
    mov     rsi, r12
    mov     rdx, r13
    mov     rcx, r15
    call    partition
    
    ; m is in RAX
    cmp     rax, r13
    jne     .dw_not_sorted_start

    ; m == sorted_start
    cmp     r15, r13
    jne     .dw_check_p_gt_0

    test    r13, r13
    jz      .dw_return
    mov     rdi, rbx
    mov     rsi, r12
    mov     rdx, r13
    dec     rdx
    call    upwave
    jmp     .dw_return

.dw_check_p_gt_0:
    test    r15, r15
    jz      .dw_return
    mov     rdi, rbx
    mov     rsi, r12
    mov     rdx, r13
    mov     rcx, r15
    dec     rcx
    call    downwave
    jmp     .dw_return

.dw_not_sorted_start:
    mov     rbp, rax ; m

    ; block_swap
    mov     rdi, rbx
    mov     rsi, rbp
    mov     rdx, r13
    mov     rcx, r15
    call    block_swap

    cmp     rbp, r12
    jne     .dw_check_p_sorted

    ; m == start
    cmp     r15, r13
    jne     .dw_m_start_next

    mov     rdi, rbx
    mov     rsi, rbp
    inc     rsi
    mov     rdx, r14
    call    upwave
    jmp     .dw_return

.dw_m_start_next:
    lea     rax, [r15 + 1]
    mov     rsi, rbp
    add     rsi, rax
    sub     rsi, r13
    
    mov     rdi, rbx
    mov     rdx, rax
    mov     rcx, r14
    call    downwave
    jmp     .dw_return

.dw_check_p_sorted:
    cmp     r15, r13
    jne     .dw_final_split

    test    rbp, rbp
    jz      .dw_do_second_up
    mov     rdi, rbx
    mov     rsi, r12
    mov     rdx, rbp
    dec     rdx
    call    upwave

.dw_do_second_up:
    mov     rdi, rbx
    mov     rsi, rbp
    inc     rsi
    mov     rdx, r14
    call    upwave
    jmp     .dw_return

.dw_final_split:
    mov     rax, r15
    sub     rax, r13
    mov     r8, rbp
    add     r8, rax ; split_point

    test    r8, r8
    jz      .dw_second_rec
    
    push    r8
    mov     rdi, rbx
    mov     rsi, r12
    mov     rdx, rbp
    mov     rcx, r8
    dec     rcx
    call    downwave
    pop     r8

.dw_second_rec:
    mov     rdi, rbx
    mov     rsi, r8
    inc     rsi
    mov     rdx, r15
    inc     rdx
    mov     rcx, r14
    call    downwave

.dw_return:
    add     rsp, 8
    pop     r15
    pop     r14
    pop     r13
    pop     r12
    pop     rbx
    pop     rbp
    ret

; ==============================================================================
; Function: upwave (Recursion)
; ==============================================================================
align 16
upwave:
    push    rbp
    push    rbx
    push    r12
    push    r13
    push    r14
    
    cmp     rsi, rdx
    je      .uw_exit

    mov     rbx, rdi
    mov     r12, rsi
    mov     r13, rdx

    test    r13, r13
    jz      .uw_exit

    mov     r14, r13
    mov     rbp, 1
    
    push    r15
    mov     r15, r13
    dec     r15

    align 16
.uw_loop:
    mov     rdi, rbx
    mov     rsi, r15
    mov     rdx, r14
    mov     rcx, r13
    call    downwave

    mov     r14, r15
    mov     rbp, r13
    sub     rbp, r14
    inc     rbp

    mov     rax, r13
    sub     rax, r12
    inc     rax

    mov     rcx, rbp
    shl     rcx, 2
    cmp     rax, rcx
    jl      .uw_break

    mov     rcx, rbp
    shl     rcx, 1
    inc     rcx

    cmp     r13, rcx
    jb      .uw_set_start

    mov     rax, r13
    sub     rax, rcx
    cmp     rax, r12
    jb      .uw_set_start

    mov     r15, rax
    jmp     .uw_check_lb

.uw_set_start:
    mov     r15, r12

.uw_check_lb:
    cmp     r15, r12
    jae     .uw_check_ss
    mov     r15, r12

.uw_check_ss:
    cmp     r14, r12
    je      .uw_break
    
    jmp     .uw_loop

.uw_break:
    mov     rdi, rbx
    mov     rsi, r12
    mov     rdx, r14
    mov     rcx, r13
    call    downwave

    pop     r15
.uw_exit:
    pop     r14
    pop     r13
    pop     r12
    pop     rbx
    pop     rbp
    ret

; ==============================================================================
; Function: wave_sort
; Entry Point
; ==============================================================================
align 16
wave_sort:
    test    rdi, rdi
    jz      .ws_done
    cmp     rsi, 2
    jb      .ws_done

    dec     rsi      ; end = n - 1
    mov     rdx, rsi
    xor     rsi, rsi ; start = 0
    call    upwave

.ws_done:
    ret
