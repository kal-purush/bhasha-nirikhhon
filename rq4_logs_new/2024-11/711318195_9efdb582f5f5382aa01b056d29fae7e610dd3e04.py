import torch
import time
import triton
import triton.language as tl
import matplotlib
import pandas as pd
import numpy as np

# We can fuse `leaky_relu` by providing it as an `ACTIVATION` meta-parameter in `_matmul`.
@triton.jit
def leaky_relu(x):
    return tl.where(x >= 0, x, 0.01 * x)

@triton.jit
def d_leacky_relu_inv_backward(x):
    return tl.where(x >= 0, 1.0, 0.01)

@triton.jit
def _mlp_wide_kernel_bwd_dx(
    pid_h, pid_b,
    x_ptr, w1_ptr, w2_ptr, o_ptr, dx_ptr, dw1_ptr, dw2_ptr, do_ptr,
    H, B, D: tl.constexpr, E,
    stride_xb, stride_xd,
    stride_w1d, stride_w1e,
    stride_w2e, stride_w2d,
    stride_ob, stride_od,
    stride_dxb, stride_dxd,
    stride_dw1d, stride_dw1e,
    stride_dw2e, stride_dw2d,
    stride_dob, stride_dod,
    BLOCK_SIZE_B: tl.constexpr, BLOCK_SIZE_E: tl.constexpr,
    ACTIVATION: tl.constexpr
):
    """Kernel for computing the mlp_bwd_dx
    Z = X @ W1, H = f(Z), O = H @ W2
    - X has shape (B, D)
    - W1 has shape (D, E)
    - W2 has shape (E, D)
    - O has shape (B, D)
    - dX has shape (B, D)
    - dW1 has shape (D, E)
    - dW2 has shape (E, D)
    - dO has shape (B, D)
    """
    # -----------------------------------------------------------
    # Map program ids `pid` to the block of C it should compute.
    # pid_b = pid // H
    # pid_h = pid % H
    TARGET_TYPE = x_ptr.type.element_ty

    offs_b = tl.arange(0, BLOCK_SIZE_B)
    offs_d = tl.arange(0, D)
    offs_e = tl.arange(0, BLOCK_SIZE_E)

    x_ptrs = x_ptr + ((pid_h * B + pid_b * BLOCK_SIZE_B + offs_b[:, None]) * stride_xb + offs_d[None, :] * stride_xd)
    x_mask = (offs_b[:, None] < B - pid_b * BLOCK_SIZE_B) & (offs_d[None, :] < D)

    do_ptrs = do_ptr + ((pid_h * B + pid_b * BLOCK_SIZE_B + offs_b[:, None]) * stride_dob + offs_d[None, :] * stride_dod)
    do_mask = (offs_b[:, None] < B - pid_b * BLOCK_SIZE_B) & (offs_d[None, :] < D)

    w1_ptrs = w1_ptr + ((pid_h * D + offs_d[:, None]) * stride_w1d + offs_e[None, :] * stride_w1e)
    w2_ptrs = w2_ptr + ((pid_h * E + offs_e[:, None]) * stride_w2e + offs_d[None, :] * stride_w2d)

    dw1_ptrs = dw1_ptr + ((pid_h * D + offs_d[:, None]) * stride_dw1d + offs_e[None, :] * stride_dw1e)
    dw2_ptrs = dw2_ptr + ((pid_h * E + offs_e[:, None]) * stride_dw2e + offs_d[None, :] * stride_dw2d)

    x = tl.load(x_ptrs, mask=x_mask, other=0.0) # BLOCK_SIZE_B, D
    do = tl.load(do_ptrs, mask=do_mask, other=0.0) # BLOCK_SIZE_B, D
    dx = tl.zeros((BLOCK_SIZE_B, D), dtype=tl.float32)  # BLOCK_SIZE_B, D

    for e in range(0, tl.cdiv(E, BLOCK_SIZE_E)):

        w1_mask = (offs_d[:, None] < D) & (offs_e[None, :] < E - e * BLOCK_SIZE_E)
        w2_mask = (offs_e[:, None] < E - e * BLOCK_SIZE_E) & (offs_d[None, :] < D)

        w1 = tl.load(w1_ptrs, mask=w1_mask, other=0.0) # D, BLOCK_SIZE_E
        w2 = tl.load(w2_ptrs, mask=w2_mask, other=0.0) # BLOCK_SIZE_E, D

        z = tl.dot(x, w1, out_dtype=tl.float32)                         # BLOCK_SIZE_B, BLOCK_SIZE_E
        # activation
        if ACTIVATION == "leaky_relu":
            h = leaky_relu(z).to(TARGET_TYPE)                           # BLOCK_SIZE_B, BLOCK_SIZE_E
        else:
            h = z.to(TARGET_TYPE)                                       # BLOCK_SIZE_B, BLOCK_SIZE_E

        dh = tl.dot(do, tl.trans(w2), out_dtype=tl.float32)             # BLOCK_SIZE_B, BLOCK_SIZE_E

        if ACTIVATION == "leaky_relu":
            dz = (dh * d_leacky_relu_inv_backward(z)).to(TARGET_TYPE)   # BLOCK_SIZE_B, BLOCK_SIZE_E
        else:
            dz = dh.to(TARGET_TYPE)

        dx += tl.dot(dz, tl.trans(w1), out_dtype=tl.float32)             # BLOCK_SIZE_B, D

        w1_ptrs += BLOCK_SIZE_E * stride_w1e
        w2_ptrs += BLOCK_SIZE_E * stride_w2e
        dw1_ptrs += BLOCK_SIZE_E * stride_dw1e
        dw2_ptrs += BLOCK_SIZE_E * stride_dw2e

    return dx

@triton.jit
def _mlp_wide_kernel_bwd_dw1w2(
    pid_h, pid_e,
    x_ptr, w1_ptr, w2_ptr, o_ptr, dx_ptr, dw1_ptr, dw2_ptr, do_ptr,
    H, B, D: tl.constexpr, E,
    stride_xb, stride_xd,
    stride_w1d, stride_w1e,
    stride_w2e, stride_w2d,
    stride_ob, stride_od,
    stride_dxb, stride_dxd,
    stride_dw1d, stride_dw1e,
    stride_dw2e, stride_dw2d,
    stride_dob, stride_dod,
    BLOCK_SIZE_B: tl.constexpr, BLOCK_SIZE_E: tl.constexpr,
    ACTIVATION: tl.constexpr
):
    """Kernel for computing the mlp_bwd_dw1w2
    Z = X @ W1, H = f(Z), O = H @ W2
    - X has shape (B, D)
    - W1 has shape (D, E)
    - W2 has shape (E, D)
    - O has shape (B, D)
    - dX has shape (B, D)
    - dW1 has shape (D, E)
    - dW2 has shape (E, D)
    - dO has shape (B, D)
    """
    # -----------------------------------------------------------
    # Map program ids `pid` to the block of C it should compute.
    # pid_b = pid // H
    # pid_h = pid % H
    TARGET_TYPE = x_ptr.type.element_ty

    offs_b = tl.arange(0, BLOCK_SIZE_B)
    offs_d = tl.arange(0, D)
    offs_e = tl.arange(0, BLOCK_SIZE_E)

    x_ptrs = x_ptr + ((pid_h * B + offs_b[:, None]) * stride_xb + offs_d[None, :] * stride_xd)

    do_ptrs = do_ptr + ((pid_h * B + offs_b[:, None]) * stride_dob + offs_d[None, :] * stride_dod)
    do_mask = (offs_b[:, None] < B) & (offs_d[None, :] < D)

    w1_ptrs = w1_ptr + ((pid_h * D + offs_d[:, None]) * stride_w1d + (pid_e * BLOCK_SIZE_E + offs_e[None, :]) * stride_w1e)
    w1_mask = (offs_d[:, None] < D) & (offs_e[None, :] < E - pid_e * BLOCK_SIZE_E)
    w2_ptrs = w2_ptr + ((pid_h * E + pid_e * BLOCK_SIZE_E + offs_e[:, None]) * stride_w2e + offs_d[None, :] * stride_w2d)
    w2_mask = (offs_e[:, None] < E - pid_e * BLOCK_SIZE_E) & (offs_d[None, :] < D)

    w1 = tl.load(w1_ptrs, mask=w1_mask, other=0.0)                      # D, BLOCK_SIZE_E
    w2 = tl.load(w2_ptrs, mask=w2_mask, other=0.0)                      # BLOCK_SIZE_E, D
    do = tl.load(do_ptrs, mask=do_mask, other=0.0)                      # BLOCK_SIZE_B, D

    dw1 = tl.zeros((D, BLOCK_SIZE_E), dtype=tl.float32)                 # D, BLOCK_SIZE_E
    dw2 = tl.zeros((BLOCK_SIZE_E, D), dtype=tl.float32)                 # BLOCK_SIZE_E, D
    for b in range(0, tl.cdiv(B, BLOCK_SIZE_B)):

        x_mask = (offs_b[:, None] < B - b * BLOCK_SIZE_B) & (offs_d[None, :] < D)
        do_mask = (offs_b[:, None] < B - b * BLOCK_SIZE_B) & (offs_d[None, :] < D)

        x = tl.load(x_ptrs, mask=x_mask, other=0.0)                     # BLOCK_SIZE_B, D
        do = tl.load(do_ptrs, mask=do_mask, other=0.0)                  # BLOCK_SIZE_B, D

        z = tl.dot(x, w1, out_dtype=tl.float32)                         # BLOCK_SIZE_B, BLOCK_SIZE_E
        # activation
        if ACTIVATION == "leaky_relu":
            h = leaky_relu(z).to(TARGET_TYPE)                           # BLOCK_SIZE_B, BLOCK_SIZE_E
        else:
            h = z.to(TARGET_TYPE)                                       # BLOCK_SIZE_B, BLOCK_SIZE_E

        dh = tl.dot(do, tl.trans(w2), out_dtype=tl.float32)             # BLOCK_SIZE_B, BLOCK_SIZE_E

        dw2 += tl.dot(tl.trans(h), do, out_dtype=tl.float32)            # BLOCK_SIZE_E, D
        # tl.store(dw2_ptrs, dw2, mask=dw2_mask, eviction_policy="evict_last")

        if ACTIVATION == "leaky_relu":
            dz = (dh * d_leacky_relu_inv_backward(z)).to(TARGET_TYPE)   # BLOCK_SIZE_B, BLOCK_SIZE_E
        else:
            dz = dh.to(TARGET_TYPE)

        dw1 += tl.dot(tl.trans(x), dz, out_dtype=tl.float32)             # D, BLOCK_SIZE_E

        x_ptrs += BLOCK_SIZE_B * stride_xb
        do_ptrs += BLOCK_SIZE_B * stride_dob

    return dw1, dw2

@triton.autotune(
    configs=[
        # triton.Config({'BLOCK_SIZE_B': 16, 'BLOCK_SIZE_E': 16}, num_stages=4, num_warps=4),
        # triton.Config({'BLOCK_SIZE_B': 16, 'BLOCK_SIZE_E': 32}, num_stages=4, num_warps=4),
        # triton.Config({'BLOCK_SIZE_B': 32, 'BLOCK_SIZE_E': 16}, num_stages=4, num_warps=4),
        # triton.Config({'BLOCK_SIZE_B': 16, 'BLOCK_SIZE_E': 64}, num_stages=2, num_warps=4),
        triton.Config({'BLOCK_SIZE_B': 32, 'BLOCK_SIZE_E': 32}, num_stages=3, num_warps=4),
        triton.Config({'BLOCK_SIZE_B': 64, 'BLOCK_SIZE_E': 32}, num_stages=2, num_warps=4),
        triton.Config({'BLOCK_SIZE_B': 32, 'BLOCK_SIZE_E': 64}, num_stages=2, num_warps=4),
        triton.Config({'BLOCK_SIZE_B': 64, 'BLOCK_SIZE_E': 64}, num_stages=2, num_warps=4),
        # triton.Config({'BLOCK_SIZE_B': 128, 'BLOCK_SIZE_E': 64, 'GROUP_SIZE_B': 1}, num_stages=2, num_warps=4),
        # triton.Config({'BLOCK_SIZE_B': 64, 'BLOCK_SIZE_E': 128, 'GROUP_SIZE_B': 1}, num_stages=2, num_warps=4),
        # triton.Config({'BLOCK_SIZE_B': 128, 'BLOCK_SIZE_E': 128}, num_stages=2, num_warps=4),
    ],
    key=['H', 'B', 'D', 'E'],
)
@triton.jit
def mlp_wide_kernel_bwd2(
    x_ptr, w1_ptr, w2_ptr, o_ptr, dx_ptr, dw1_ptr, dw2_ptr, do_ptr,
    H, B, D: tl.constexpr, E,
    stride_xb, stride_xd,
    stride_w1d, stride_w1e,
    stride_w2e, stride_w2d,
    stride_ob, stride_od,
    stride_dxb, stride_dxd,
    stride_dw1d, stride_dw1e,
    stride_dw2e, stride_dw2d,
    stride_dob, stride_dod,
    BLOCK_SIZE_B: tl.constexpr, BLOCK_SIZE_E: tl.constexpr,
    ACTIVATION: tl.constexpr
):
    """Kernel for computing the mlp
    Z = X @ W1, H = f(Z), O = H @ W2
    - X has shape (B, D)
    - W1 has shape (D, E)
    - W2 has shape (E, D)
    - O has shape (B, D)
    - dX has shape (B, D)
    - dW1 has shape (D, E)
    - dW2 has shape (E, D)
    - dO has shape (B, D)
    """
    # -----------------------------------------------------------
    # Map program ids `pid` to the block of C it should compute.
    pid = tl.program_id(axis=0)
    pid_x_w = 0

    # batch_groups = tl.cdiv(B, BLOCK_SIZE_B)
    batch_groups_e = tl.cdiv(E, BLOCK_SIZE_E)
    batch_groups_b = tl.cdiv(B, BLOCK_SIZE_B)
    idx = pid % (batch_groups_e + batch_groups_b)
    pid_h = pid // (batch_groups_e + batch_groups_b)
    # pid_b = pid // H
    # pid_h = pid % H
    TARGET_TYPE = x_ptr.type.element_ty

    offs_b = tl.arange(0, BLOCK_SIZE_B)
    offs_d = tl.arange(0, D)
    offs_e = tl.arange(0, BLOCK_SIZE_E)

    if idx >= batch_groups_e:

        pid_b = idx - batch_groups_e

        dx_ptrs = dx_ptr + ((pid_h * B + pid_b * BLOCK_SIZE_B + offs_b[:, None]) * stride_dxb + offs_d[None, :] * stride_dxd)
        dx_mask = (offs_b[:, None] < B - pid_b * BLOCK_SIZE_B) & (offs_d[None, :] < D)

        dx = tl.zeros((BLOCK_SIZE_B, D), dtype=tl.float32)  # BLOCK_SIZE_B, D
        dx = _mlp_wide_kernel_bwd_dx(
            pid_h, pid_b,
            x_ptr, w1_ptr, w2_ptr, o_ptr, dx_ptr, dw1_ptr, dw2_ptr, do_ptr,
            H, B, D, E,
            stride_xb, stride_xd,
            stride_w1d, stride_w1e,
            stride_w2e, stride_w2d,
            stride_ob, stride_od,
            stride_dxb, stride_dxd,
            stride_dw1d, stride_dw1e,
            stride_dw2e, stride_dw2d,
            stride_dob, stride_dod,
            BLOCK_SIZE_B, BLOCK_SIZE_E,
            ACTIVATION
        )

        tl.store(dx_ptrs, dx.to(TARGET_TYPE), mask=dx_mask)

    else:

        pid_e = idx

        dw1_ptrs = dw1_ptr + ((pid_h * D + offs_d[:, None]) * stride_dw1d + (pid_e * BLOCK_SIZE_E + offs_e[None, :]) * stride_dw1e)
        dw1_mask = (offs_d[:, None] < D) & (offs_e[None, :] < E - pid_e * BLOCK_SIZE_E)
        dw2_ptrs = dw2_ptr + ((pid_h * E + pid_e * BLOCK_SIZE_E + offs_e[:, None]) * stride_dw2e + offs_d[None, :] * stride_dw2d)
        dw2_mask = (offs_e[:, None] < E - pid_e * BLOCK_SIZE_E) & (offs_d[None, :] < D)

        dw1, dw2 = _mlp_wide_kernel_bwd_dw1w2(
            pid_h, pid_e,
            x_ptr, w1_ptr, w2_ptr, o_ptr, dx_ptr, dw1_ptr, dw2_ptr, do_ptr,
            H, B, D, E,
            stride_xb, stride_xd,
            stride_w1d, stride_w1e,
            stride_w2e, stride_w2d,
            stride_ob, stride_od,
            stride_dxb, stride_dxd,
            stride_dw1d, stride_dw1e,
            stride_dw2e, stride_dw2d,
            stride_dob, stride_dod,
            BLOCK_SIZE_B, BLOCK_SIZE_E,
            ACTIVATION
        )

        tl.store(dw1_ptrs, dw1.to(TARGET_TYPE), mask=dw1_mask)
        tl.store(dw2_ptrs, dw2.to(TARGET_TYPE), mask=dw2_mask)

def mlp_wide_triton_bwd2(x, w1, w2, o, do, activation=""):
    # Check constraints.
    assert x.shape[-1] == w1.shape[-2], "Incompatible dimensions"
    assert w1.shape[-1] == w2.shape[-2], "Incompatible dimensions"
    assert x.shape[-1] == w2.shape[-1], "Incompatible dimensions"
    assert x.shape == o.shape, "Incompatible dimensions"
    assert x.shape == do.shape, "Incompatible dimensions"

    H, B, D = x.shape
    E = w1.shape[-1]

    x = x.view(H * B, D)
    w1 = w1.view(D * H, E)
    w2 = w2.view(E * H, D)
    o = o.view(H * B, D)
    do = do.view(H * B, D)

    assert x.is_contiguous(), "Matrix X must be contiguous"
    assert w1.is_contiguous(), "Matrix W1 must be contiguous"
    assert w2.is_contiguous(), "Matrix W2 must be contiguous"
    assert o.is_contiguous(), "Matrix O must be contiguous"
    assert do.is_contiguous(), "Matrix dO must be contiguous"


    # Allocates output.
    dx = torch.zeros_like(x)
    dw1 = torch.zeros_like(w1)
    dw2 = torch.zeros_like(w2)
    # print(dx.shape, dw1.shape, dw2.shape, do.shape)

    # 1D launch kernel where each block gets its own program.
    grid = lambda META: (
        # triton.cdiv(B, META['BLOCK_SIZE_B']) * H,
        (triton.cdiv(B, META['BLOCK_SIZE_B']) + triton.cdiv(E, META['BLOCK_SIZE_E'])) * H,
    )
    mlp_wide_kernel_bwd2[grid](
        x, w1, w2, o, dx, dw1, dw2, do,
        H, B, D, E,
        x.stride(0), x.stride(1),
        w1.stride(0), w1.stride(1),
        w2.stride(0), w2.stride(1),
        o.stride(0), o.stride(1),
        dx.stride(0), dx.stride(1),
        dw1.stride(0), dw1.stride(1),
        dw2.stride(0), dw2.stride(1),
        do.stride(0), do.stride(1),
        ACTIVATION=activation
    )

    # print(dx.shape, dw1.shape, dw2.shape)
    return dx.view(H, B, D), dw1.view(H, D, E), dw2.view(H, E, D)

def mlp_torch_bwd(x, w1, w2, o, do, activation=""):
    # x: H, B, D
    # w1: H, D, E
    # w2: H, E, D
    # o: H, B, D
    # do: H, B, D
    z = torch.bmm(x, w1) # H, B, E
    if activation == "leaky_relu":
        h = torch.nn.functional.leaky_relu(z, negative_slope=0.01)
    else:
        h = z

    dh = torch.bmm(do, torch.transpose(w2, -1, -2)) # H, B, E
    dw2 = torch.bmm(torch.transpose(h, -1, -2), do) # H, E, D

    if activation == "leaky_relu":
        dz = dh * torch.where(z >= 0, 1.0, 0.01).to(dh.dtype)  # H, B, E
    else:
        dz = dh

    dx = torch.bmm(dz, torch.transpose(w1, -1, -2)) # H, B, D
    dw1 = torch.bmm(torch.transpose(x, -1, -2), dz) # H, D, E

    return dx, dw1, dw2

def unit_test_bwd2():
    torch.manual_seed(time.time() % 1000)
    DTYPE = torch.bfloat16
    B = 1024 * 4
    D = 64
    E = 768
    H = E // D

    x = torch.randn((H, B, D), device='cuda', dtype=DTYPE) / np.sqrt(D)
    w1 = torch.randn((H, D, E), device='cuda', dtype=DTYPE) / np.sqrt(E)
    w2 = torch.randn((H, E, D), device='cuda', dtype=DTYPE) / np.sqrt(D)
    o = torch.randn((H, B, D), device='cuda', dtype=DTYPE) / np.sqrt(D)
    do = torch.randn((H, B, D), device='cuda', dtype=DTYPE) / np.sqrt(D)

    triton_output = mlp_wide_triton_bwd2(x, w1, w2, o, do, activation="leaky_relu")
    torch_output = mlp_torch_bwd(x, w1, w2, o, do, activation="leaky_relu")

    print(f"triton_output={triton_output[1].shape, triton_output[1]}")
    print(f"torch_output={torch_output[1].shape, torch_output[1]}")

    eplison = 3e-2
    if torch.allclose(triton_output[1], torch_output[1], atol=eplison, rtol=eplison):
        print("✅ Triton and Torch match")
    else:
        print("❌ Triton and Torch differ")

    dx_diff = np.abs(triton_output[0].to(torch.float32).cpu().numpy() - torch_output[0].to(torch.float32).cpu().numpy())
    dx_rel_diff = dx_diff / np.abs(triton_output[0].to(torch.float32).cpu().numpy() + torch_output[0].to(torch.float32).cpu().numpy() + eplison)
    print(f"[dx: {triton_output[0].shape}, {torch_output[0].shape}] max diff: {np.max(dx_diff):.2e}, mean diff: {np.mean(dx_diff):.2e}, rel max diff: {np.max(dx_rel_diff)*100:.2f}%, rel mean diff: {np.mean(dx_rel_diff)*100:.2f}%")

    dw1_diff = np.abs(triton_output[1].to(torch.float32).cpu().numpy() - torch_output[1].to(torch.float32).cpu().numpy())
    dw1_rel_diff = dw1_diff / np.abs(triton_output[1].to(torch.float32).cpu().numpy() + torch_output[1].to(torch.float32).cpu().numpy() + eplison)
    print(f"[dw1: {triton_output[1].shape}, {torch_output[1].shape}] max diff: {np.max(dw1_diff):.2e}, mean diff: {np.mean(dw1_diff):.2e}, rel max diff: {np.max(dw1_rel_diff)*100:.2f}%, rel mean diff: {np.mean(dw1_rel_diff)*100:.2f}%")

    dw2_diff = np.abs(triton_output[2].to(torch.float32).cpu().numpy() - torch_output[2].to(torch.float32).cpu().numpy())
    dw2_rel_diff = dw2_diff / np.abs(triton_output[2].to(torch.float32).cpu().numpy() + torch_output[2].to(torch.float32).cpu().numpy() + eplison)
    print(f"[dw2: {triton_output[2].shape}, {torch_output[2].shape}] max diff: {np.max(dw2_diff):.2e}, mean diff: {np.mean(dw2_diff):.2e}, rel max diff: {np.max(dw2_rel_diff)*100:.2f}%, rel mean diff: {np.mean(dw2_rel_diff)*100:.2f}%")


if __name__ == '__main__':
    unit_test_bwd2()