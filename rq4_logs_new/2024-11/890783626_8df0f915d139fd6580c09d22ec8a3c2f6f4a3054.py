import torch
import torch.nn as nn

class PatchEmbedding(nn.Module):
    """
    Module block to process a batch of images of 
    shape [batch_size, num_channels, height, width]
    -> [batch_size, num_patches, patch_size ** 2 * num_channels]
    """
    def __init__(self, color_channel=3, patch_size=16, embedding_dim=768):
        super().__init__()
        self.patch_size = patch_size
        self.patcher = nn.Conv2d(in_channels=color_channel,
                              out_channels=embedding_dim,
                              kernel_size=patch_size,
                              stride=patch_size)
        self.flatten = nn.Flatten(start_dim=2, end_dim=3)
    
    def forward(self, x):
        # Input is a batch of images in shape -> [batch_size, num_channels, height, width]
        image_resolution = x.shape[-1]
        assert image_resolution % self.patch_size == 0, f"Image resolution ({image_resolution}) should be divisible by patch size ({self.patch_size})"
        return self.flatten(self.patcher(x)).permute(0, 2, 1) # Shape [batch_size, num_patches, patch_size ** 2 * num_channels]


class ViT(nn.Module):
    def __init__(self,
                 num_layers=12,
                 patch_size=16,
                 embedding_dim=768,
                 MLP_size=3072,
                 num_heads=12,
                 num_classes=3
                 ):
        super().__init__()
        self.patch_embed = PatchEmbedding(patch_size=patch_size,
                                          embedding_dim=embedding_dim)
        self.encoder_block = torch.nn.TransformerEncoderLayer(d_model=embedding_dim,
                                                              nhead=num_heads,
                                                              dim_feedforward=MLP_size,
                                                              batch_first=True,
                                                              activation="gelu",
                                                              dropout=0.1,
                                                              norm_first=True)
        self.encoder = torch.nn.TransformerEncoder(self.encoder_block,
                                                   num_layers=num_layers)
        self.classifier = nn.Sequential(
            nn.LayerNorm(normalized_shape=embedding_dim),
            nn.Linear(in_features=embedding_dim,
                      out_features=num_classes)
        )
    
    def forward(self, x):
        x = self.patch_embed(x)
        x = self.encoder(x)
        x = self.classifier(x[:, 0])
        return x