import os

import torch
from torch.utils.data import Dataset
from sklearn.decomposition import PCA


def get_latents_dataset(root):
    makeup_latents = torch.load(root)
    latents, labels = [], []
    for name, latent in makeup_latents.items():
        latent = latent.cpu().numpy()
        latents.append(latent.reshape(-1))
        labels.append(int(name.split('_')[0][6:]) - 1)
    return latents, labels


def get_direction_latents(makeup_root, nonmakeup_root, save_dir, label):
    makeup_latents = torch.load(makeup_root)
    nonmakeup_latents = torch.load(nonmakeup_root)
    makeup_latents_names = list(makeup_latents.keys())
    nonmakeup_latents_names = list(nonmakeup_latents.keys())
    latents = []
    for i, makeup_latents_name in enumerate(makeup_latents_names):
        if int(makeup_latents_name.split('_')[0][6:]) == label:
            latents.append(makeup_latents[makeup_latents_name] - nonmakeup_latents[nonmakeup_latents_names[i]])
    latents = torch.cat([latent.unsqueeze(0) for latent in latents], dim=0)
    result = torch.mean(latents, dim=0)
    os.makedirs(f'{save_dir}/align', exist_ok=True)
    torch.save(result, f'{save_dir}/align/makeup{label}.pt')


if __name__ == '__main__':
    for lable in range(1, 16):
        get_direction_latents('./result/PMT_align_lr1e-4/makeup/inversion/latent.pt',
                              './result/PMT_align_lr1e-4/nonmakeup/inversion/latent.pt',
                              './editings/interfacegan_directions', lable)