import torch
import torch.nn as nn


class InputEmbeddings(nn.Module):
    """
    Construct the input embeddings.

    Args:
        d_model: the hidden dimension of the model in the paper
        vocab_size: the vocabulary size

    Returns:
        a nn.Embedding layer
    """

    def __init__(self, d_model: int, vocab_size: int):
        super().__init__()
        self.d_model = d_model
        self.vocab_size = vocab_size
        self.embedding = nn.Embedding(vocab_size, d_model)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """
        Embeds the input token ids.

        Args:
            x: the input token ids

        Returns:
            the embedded input tokens
        """
        return self.embedding(x)