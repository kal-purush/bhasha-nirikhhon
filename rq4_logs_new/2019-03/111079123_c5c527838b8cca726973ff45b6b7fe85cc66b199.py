import torch.nn as nn


class HA(nn.Module):
    def __init__(self, embedding, horizon):
        super().__init__()
        self.embedding = embedding
        self.horizon = horizon