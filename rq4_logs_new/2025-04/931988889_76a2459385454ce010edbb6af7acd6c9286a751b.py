import argparse
import os
import random

import matplotlib.pyplot as plt
import torch
from pathlib import Path

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from core import get_fft
from data.preprocessing import AugmentedVibrationalSignalDataset
from diffusion import config_registry
from diffusion.sampling import DDIM
from diffusion.util import load_model, plot_comparison_samples, plot_edit_results
from classify import get_classifier_confidence


def main(args):
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    config = config_registry[args.version]
    class_names = config.class_names
    
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    model = load_model(model_version=args.version, device=device)
    model.eval()

    dataset = AugmentedVibrationalSignalDataset(
        data_dir='data/processed/train',
        segment_length=1024,
        overlap_ratios=[0],
        class_names=class_names,
        augment=False,
    )

    ddim = DDIM(model, config)

    # Find all indices of normal samples (label == 0)
    normal_indices = [i for i in range(len(dataset)) if dataset[i][1] == 0]
    # choose n random samples
    random_normal_idx = random.sample(normal_indices, args.num_samples)
    
    samples, labels = dataset.get_X_y_tensor()
    real_samples = samples[random_normal_idx]

    latent = ddim.invert(real_samples, num_steps=100)
    reconstructed = ddim.reconstruct(real_samples, condition=0, num_steps=100, guidance_scale=args.cfg)
    
    # Helpers from util.py for signal formatting
    def to_np(x):
        if isinstance(x, torch.Tensor):
            return x.detach().cpu().numpy()
        return x
    def squeeze_signal(x):
        if len(x.shape) == 3:
            return x[0, 0]
        elif len(x.shape) == 2:
            return x[0]
        return x

    # Plot Original, Latent, and Reconstructed using similar style as plot_edit_results
    colors = ['tab:blue', 'tab:orange', 'tab:green']
    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
    real_np = to_np(real_samples)[0]
    latent_np = to_np(latent)[0]
    recon_np = to_np(reconstructed)[0]
    axes[0].plot(squeeze_signal(real_np), color=colors[0])
    axes[0].set_title(f'Original Signal (Class: {class_names[0]})')
    axes[0].set_ylabel('Amplitude')
    axes[0].grid(True, linestyle='--', alpha=0.5)
    axes[1].plot(squeeze_signal(latent_np), color=colors[1])
    axes[1].set_title('Latent Signal')
    axes[1].set_ylabel('Amplitude')
    axes[1].grid(True, linestyle='--', alpha=0.5)
    axes[2].plot(squeeze_signal(recon_np), color=colors[2])
    axes[2].set_title(f'Reconstructed Signal (Class: {class_names[0]})')
    axes[2].set_xlabel('Time step')
    axes[2].set_ylabel('Amplitude')
    axes[2].grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'original_latent_reconstructed.png'), bbox_inches='tight')
    plt.close(fig)

    # Edit into all faulty classes (1–10) and plot results
    edited_samples = {}
    confidence = {}
    for faulty_idx in range(1, len(class_names)):
        edited = ddim.edit(
            real_samples,
            target_condition=faulty_idx,
            source_condition=None if not args.conditional else 0,
            num_steps=args.ddim_steps,
            guidance_scale=args.cfg
        )
        conf, _, _ = get_classifier_confidence(edited, faulty_idx, class_names, verbose=False, device=device, always_tensor=True)
        edited_samples[class_names[faulty_idx]] = edited
        confidence[class_names[faulty_idx]] = conf.mean().item()
        print(f"Edited normal sample into class '{class_names[faulty_idx]}'")
        
        plot_edit_results(
            real_samples[0],
            reconstructed[0],
            edited[0],
            0,
            faulty_idx,
            class_names,
            save_path=os.path.join(output_dir, f'edit_normal_to_{class_names[faulty_idx]}.png'),
        )
        
        
    
    for class_name, edited_sample in edited_samples.items():
        class_idx = config.class_names.index(class_name)
        real_samples = [signal for signal, label in zip(samples, labels) 
                            if label == class_idx]
        real_samples_tensor = torch.stack(real_samples)
        plot_comparison_samples(
            real_samples_tensor,
            edited_sample,
            class_name,
            output_dir,
            num_to_plot=args.num_samples,
            title=f"Comparison of Real and Editing Generated Samples for {class_name}. Classification confidence: {confidence[class_name]:.2f}"
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", type=str, default="best", help="Model version")
    parser.add_argument("--output-dir", type=str, default="results/edit", help="Directory to save results")
    parser.add_argument("--num-samples", type=int, default=10, help="Number of samples to generate per class")
    parser.add_argument("--cfg", type=float, default=None, help="Classifier-free guidance scale")
    parser.add_argument("--conditional", type=bool, default=False, help="Use conditional editing")
    parser.add_argument("--ddim-steps", type=int, default=100, help="Number of DDIM steps")
    args = parser.parse_args()
    
    main(args)