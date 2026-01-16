"""
An adaptation of Andrej Karpathy's nanoGPT implementation in PyTorch.
Original source: https://github.com/karpathy/nanoGPT

Original License:
MIT License

Copyright (c) 2022 Andrej Karpathy

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Original comments:
Full definition of a GPT Language Model, all of it in this single file.
References:
1) the official GPT-2 TensorFlow implementation released by OpenAI:
https://github.com/openai/gpt-2/blob/master/src/model.py
2) huggingface/transformers PyTorch implementation:
https://github.com/huggingface/transformers/blob/main/src/transformers/models/gpt2/modeling_gpt2.py
"""

import math
from dataclasses import dataclass

import torch
import torch.nn as nn
from torch.nn import functional as F


# @torch.jit.script # good to enable when not using torch.compile, disable when using (our default)
def new_gelu(x):
    """
    Implementation of the GELU activation function currently in Google BERT repo (identical to OpenAI GPT).
    Reference: Gaussian Error Linear Units (GELU) paper: https://arxiv.org/abs/1606.08415
    """
    return (
        0.5
        * x
        * (
            1.0
            + torch.tanh(math.sqrt(2.0 / math.pi) * (x + 0.044715 * torch.pow(x, 3.0)))
        )
    )


class CrossAttention(nn.Module):
    def __init__(self, repr_dim, nhead=4, nlayers=4, use_buffer_token=False):
        super().__init__()
        self.tf_decoder = nn.TransformerDecoder(
            nn.TransformerDecoderLayer(
                d_model=repr_dim,
                nhead=nhead,
                dim_feedforward=repr_dim * 4,
                batch_first=True,
            ),
            num_layers=nlayers,
        )
        if use_buffer_token:
            self.buffer_token = nn.Parameter(torch.randn(1, 1, repr_dim))
        self.use_buffer_token = use_buffer_token

    def forward(self, feat, cond):
        # add buffer token to the beginning of the sequence
        if self.use_buffer_token:
            batch_size = feat.size(0)
            buffer_token = self.buffer_token.expand(batch_size, 1, -1)
            cond_with_buffer = torch.cat([buffer_token, cond], dim=1)
            return self.tf_decoder(feat, cond_with_buffer)
        else:
            return self.tf_decoder(feat, cond)

class CausalSelfAttention(nn.Module):
    def __init__(self, config):
        super().__init__()
        assert config.n_embd % config.n_head == 0

        # Key, query, value projections for all heads
        self.c_attn = nn.Linear(config.n_embd, 3 * config.n_embd)
        # Output projection
        self.c_proj = nn.Linear(config.n_embd, config.n_embd)

        # Regularization
        self.attn_dropout = nn.Dropout(config.dropout)
        self.resid_dropout = nn.Dropout(config.dropout)

        self.n_head = config.n_head
        self.n_embd = config.n_embd
        self.head_dim = self.n_embd // self.n_head  # Head size

        # Initialize cache for keys and values
        self.max_cache_length = config.max_cache_len  # Maximum number of time steps to cache
        self.cache_initialized = False  # Flag to check if cache is initialized

        # Compression modules
        self.compress_k = nn.Conv1d(
            in_channels=self.head_dim,
            out_channels=self.head_dim,
            kernel_size=config.compress_kernel_size,
            stride=config.compress_stride,
            padding=config.compress_padding,
            groups=self.head_dim,  # Depthwise convolution
            bias=False,
        )
        self.compress_v = nn.Conv1d(
            in_channels=self.head_dim,
            out_channels=self.head_dim,
            kernel_size=config.compress_kernel_size,
            stride=config.compress_stride,
            padding=config.compress_padding,
            groups=self.head_dim,
            bias=False,
        )
        self.norm_compress_k = nn.LayerNorm(self.head_dim)
        self.norm_compress_v = nn.LayerNorm(self.head_dim)

    def reset_cache(self, batch_size):
        # Initialize fixed-size cache tensors
        device = next(self.parameters()).device
        max_cache_tokens = self.max_cache_length * self.num_tokens_per_time_step  # Total tokens in cache
        self.cached_k = torch.zeros(
            batch_size, self.n_head, max_cache_tokens, self.head_dim, device=device
        )
        self.cached_v = torch.zeros(
            batch_size, self.n_head, max_cache_tokens, self.head_dim, device=device
        )
        self.cache_lengths = torch.zeros(batch_size, dtype=torch.long, device=device)
        self.cache_initialized = True

    def forward(self, x, attn_mask=None):
        B, T_tokens, C = x.size()  # x: (B, T_tokens, C)
        device = x.device
        self.num_tokens_per_time_step = T_tokens  # Number of tokens per time step

        # if not self.cache_initialized or (self.cached_k is not None and self.cached_k.size(0) != B):
        if not self.cache_initialized:
            # Initialize fixed-size cache tensors
            max_cache_tokens = self.max_cache_length * T_tokens
            self.cached_k = torch.zeros(
                B, self.n_head, max_cache_tokens, self.head_dim, device=device
            )
            self.cached_v = torch.zeros(
                B, self.n_head, max_cache_tokens, self.head_dim, device=device
            )
            self.cache_lengths = torch.zeros(B, dtype=torch.long, device=device)
            self.cache_initialized = True

        # Compute query, key, and value projections
        q, k, v = self.c_attn(x).split(self.n_embd, dim=2)
        q = q.view(B, T_tokens, self.n_head, self.head_dim).transpose(1, 2)  # (B, nh, T_tokens, hs)
        k = k.view(B, T_tokens, self.n_head, self.head_dim).transpose(1, 2)
        v = v.view(B, T_tokens, self.n_head, self.head_dim).transpose(1, 2)

        # Concatenate cached keys and values
        if self.cache_lengths[0] > 0:
            # Previous uncompressed keys and values
            prev_k = self.cached_k[:, :, : self.cache_lengths[0], :]  # (B, nh, L_cache, hs)
            prev_v = self.cached_v[:, :, : self.cache_lengths[0], :]

            # Compress the previous keys and values
            # Reshape for convolution: (B * nh, hs, L_cache)
            prev_k_reshaped = prev_k.permute(0, 1, 3, 2).reshape(B * self.n_head, self.head_dim, -1)
            prev_v_reshaped = prev_v.permute(0, 1, 3, 2).reshape(B * self.n_head, self.head_dim, -1)

            # Compress
            compressed_prev_k = self.compress_k(prev_k_reshaped)  # (B * nh, hs, L_comp)
            compressed_prev_v = self.compress_v(prev_v_reshaped)

            # Reshape back to (B, nh, L_comp, hs)
            compressed_prev_k = compressed_prev_k.reshape(B, self.n_head, self.head_dim, -1).permute(0, 1, 3, 2)
            compressed_prev_v = compressed_prev_v.reshape(B, self.n_head, self.head_dim, -1).permute(0, 1, 3, 2)

            # Apply layer normalization
            compressed_prev_k = self.norm_compress_k(compressed_prev_k)
            compressed_prev_v = self.norm_compress_v(compressed_prev_v)

            # Detach compressed keys and values to prevent gradients
            compressed_prev_k = compressed_prev_k.detach()
            compressed_prev_v = compressed_prev_v.detach()

            # Concatenate compressed cached keys and values with current k and v
            k = torch.cat([compressed_prev_k, k], dim=2)  # (B, nh, L_total, hs)
            v = torch.cat([compressed_prev_v, v], dim=2)
        else:
            # No cached keys and values
            pass  # k and v remain as they are

        # Update attention mask
        T_q = q.size(2)
        T_k = k.size(2)
        causal_mask = torch.tril(torch.ones(T_q, T_k, device=device)).view(1, 1, T_q, T_k)
        if attn_mask is not None:
            attn_mask = attn_mask[:, :, :, :T_k] * causal_mask
        else:
            attn_mask = causal_mask

        # Compute attention scores
        att = (q @ k.transpose(-2, -1)) * (1.0 / math.sqrt(self.head_dim))
        att = att.masked_fill(attn_mask == 0, float("-inf"))
        att = F.softmax(att, dim=-1)
        att = self.attn_dropout(att)

        # Attention output
        y = att @ v  # (B, nh, T_q, hs)
        y = y.transpose(1, 2).contiguous().view(B, T_q, self.n_embd)  # (B, T_q, C)

        # Output projection
        y = self.resid_dropout(self.c_proj(y))

        # Update cache with current uncompressed keys and values
        current_kv_len = k.size(2) - (compressed_prev_k.size(2) if self.cache_lengths[0] > 0 else 0)
        current_k = k[:, :, -current_kv_len:, :]  # (B, nh, T_tokens, hs)
        current_v = v[:, :, -current_kv_len:, :]  # (B, nh, T_tokens, hs)

        for i in range(B):
            required_capacity = self.cache_lengths[i] + current_kv_len
            cache_capacity = self.cached_k.size(2)
            if required_capacity > cache_capacity:
                # Remove oldest entries to make space
                overflow = required_capacity - cache_capacity
                if overflow < self.cache_lengths[i]:
                    # Shift cache to remove oldest entries
                    self.cached_k[i, :, : self.cache_lengths[i] - overflow, :] = self.cached_k[
                        i, :, overflow : self.cache_lengths[i], :
                    ]
                    self.cached_v[i, :, : self.cache_lengths[i] - overflow, :] = self.cached_v[
                        i, :, overflow : self.cache_lengths[i], :
                    ]
                    self.cache_lengths[i] -= overflow
                else:
                    # Cache is too small, reset cache
                    self.cache_lengths[i] = 0

            # Update cache
            start = self.cache_lengths[i].item()
            end = start + current_kv_len
            self.cached_k[i, :, start:end, :] = current_k[i]
            self.cached_v[i, :, start:end, :] = current_v[i]
            self.cache_lengths[i] += current_kv_len

        return y
    




class MLP(nn.Module):
    def __init__(self, config):
        super().__init__()
        self.c_fc = nn.Linear(config.n_embd, 4 * config.n_embd)
        self.c_proj = nn.Linear(4 * config.n_embd, config.n_embd)
        self.dropout = nn.Dropout(config.dropout)

    def forward(self, x):
        x = self.c_fc(x)
        x = new_gelu(x)
        x = self.c_proj(x)
        x = self.dropout(x)
        return x


class Block(nn.Module):
    def __init__(self, config):
        super().__init__()
        self.ln_1 = nn.LayerNorm(config.n_embd)
        self.attn = CausalSelfAttention(config)
        self.ln_2 = nn.LayerNorm(config.n_embd)
        self.mlp = MLP(config)

    def forward(self, x, mask=None):
        x = x + self.attn(self.ln_1(x), attn_mask=mask)
        x = x + self.mlp(self.ln_2(x))
        return x


@dataclass
# class GPTConfig:
#     block_size: int = 1024
#     input_dim: int = 256
#     output_dim: int = 256
#     n_layer: int = 12
#     n_head: int = 12
#     n_embd: int = 768
#     dropout: float = 0.1

@dataclass
class GPTConfig:
    block_size: int = 1024
    input_dim: int = 256
    output_dim: int = 256
    n_layer: int = 12
    n_head: int = 12
    n_embd: int = 768
    dropout: float = 0.1
    max_cache_len: int = 4  # Maximum cache length (number of previous frames)
    compress_kernel_size: int = 2
    compress_stride: int = 2
    compress_padding: int = 0



class GPT(nn.Module):
    def __init__(self, config):
        super().__init__()
        assert config.input_dim is not None
        assert config.output_dim is not None
        assert config.block_size is not None
        self.config = config

        self.transformer = nn.ModuleDict(
            dict(
                wte=nn.Linear(config.input_dim, config.n_embd),
                wpe=nn.Embedding(config.block_size, config.n_embd),
                drop=nn.Dropout(config.dropout),
                h=nn.ModuleList([Block(config) for _ in range(config.n_layer)]),
                ln_f=nn.LayerNorm(config.n_embd),
            )
        )
        self.lm_head = nn.Linear(config.n_embd, config.output_dim, bias=False)
        # init all weights, and apply a special scaled init to the residual projections, per GPT-2 paper
        self.apply(self._init_weights)
        for pn, p in self.named_parameters():
            if pn.endswith("c_proj.weight"):
                torch.nn.init.normal_(
                    p, mean=0.0, std=0.02 / math.sqrt(2 * config.n_layer)
                )

        # report number of parameters
        n_params = sum(p.numel() for p in self.parameters())
        print("number of parameters: %.2fM" % (n_params / 1e6,))

    def forward(self, input, targets=None, mask=None):
        device = input.device
        b, t, d = input.size()
        assert (
            t <= self.config.block_size
        ), f"Cannot forward sequence of length {t}, block size is only {self.config.block_size}"
        pos = torch.arange(0, t, dtype=torch.long, device=device).unsqueeze(
            0
        )  # shape (1, t)

        # forward the GPT model itself
        tok_emb = self.transformer.wte(
            input
        )  # token embeddings of shape (b, t, n_embd)
        pos_emb = self.transformer.wpe(
            pos
        )  # position embeddings of shape (1, t, n_embd)
        x = self.transformer.drop(tok_emb + pos_emb)
        for block in self.transformer.h:
            x = block(x, mask=mask)
        x = self.transformer.ln_f(x)
        logits = self.lm_head(x)
        return logits

    def _init_weights(self, module):
        if isinstance(module, nn.Linear):
            torch.nn.init.normal_(module.weight, mean=0.0, std=0.02)
            if module.bias is not None:
                torch.nn.init.zeros_(module.bias)
        elif isinstance(module, nn.Embedding):
            torch.nn.init.normal_(module.weight, mean=0.0, std=0.02)
        elif isinstance(module, nn.LayerNorm):
            torch.nn.init.zeros_(module.bias)
            torch.nn.init.ones_(module.weight)

    def crop_block_size(self, block_size):
        assert block_size <= self.config.block_size
        self.config.block_size = block_size
        self.transformer.wpe.weight = nn.Parameter(
            self.transformer.wpe.weight[:block_size]
        )
        for block in self.transformer.h:
            block.attn.bias = block.attn.bias[:, :, :block_size, :block_size]

    def configure_optimizers(self, weight_decay, learning_rate, betas):
        """
        This long function is unfortunately doing something very simple and is being very defensive:
        We are separating out all parameters of the model into two buckets: those that will experience
        weight decay for regularization and those that won't (biases, and layernorm/embedding weights).
        We are then returning the PyTorch optimizer object.
        """

        # separate out all parameters to those that will and won't experience regularizing weight decay
        decay = set()
        no_decay = set()
        whitelist_weight_modules = (torch.nn.Linear,)
        blacklist_weight_modules = (torch.nn.LayerNorm, torch.nn.Embedding)
        for mn, m in self.named_modules():
            for pn, p in m.named_parameters():
                fpn = "%s.%s" % (mn, pn) if mn else pn  # full param name
                if pn.endswith("bias"):
                    # all biases will not be decayed
                    no_decay.add(fpn)
                elif pn.endswith("weight") and isinstance(m, whitelist_weight_modules):
                    # weights of whitelist modules will be weight decayed
                    decay.add(fpn)
                elif pn.endswith("weight") and isinstance(m, blacklist_weight_modules):
                    # weights of blacklist modules will NOT be weight decayed
                    no_decay.add(fpn)

        # validate that we considered every parameter
        param_dict = {pn: p for pn, p in self.named_parameters()}
        inter_params = decay & no_decay
        union_params = decay | no_decay
        assert (
            len(inter_params) == 0
        ), "parameters %s made it into both decay/no_decay sets!" % (str(inter_params),)
        assert (
            len(param_dict.keys() - union_params) == 0
        ), "parameters %s were not separated into either decay/no_decay set!" % (
            str(param_dict.keys() - union_params),
        )

        # create the pytorch optimizer object
        optim_groups = [
            {
                "params": [param_dict[pn] for pn in sorted(list(decay))],
                "weight_decay": weight_decay,
            },
            {
                "params": [param_dict[pn] for pn in sorted(list(no_decay))],
                "weight_decay": 0.0,
            },
        ]
        # optimizer = torch.optim.AdamW(optim_groups, lr=learning_rate, betas=betas)
        optimizer = torch.optim.Adam(optim_groups, lr=learning_rate, betas=betas)
        return optimizer
    
    def reset_cache(self):
        # Iterate over all transformer blocks and reset their caches
        for block in self.transformer.h:
            block.attn.reset_cache()