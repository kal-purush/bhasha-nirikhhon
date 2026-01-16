from diffusers import StableDiffusionPipeline
import torch
from tqdm import tqdm
import os

model_name= "stabilityai/stable-diffusion-2-1-base"
model_path = "../models/sd-cxr-healthy-2024_02_17"
pipe = StableDiffusionPipeline.from_pretrained(model_name, torch_dtype=torch.float16)
pipe.unet.load_attn_procs(model_path)
pipe.to("cuda")

# number of images to generate
num_images = 10000

# prompts
prompts = []
prompts.append("view_position = PA. modality = CR. No findings.")
# prompts.append("view_position = PA. modality = CR. Small right pleural effusion with atelectasis versus developing airspace disease in the bilateral lung bases.")
# prompts.append("view_position = PA. modality = CR. ET tube in good position.")
# prompts.append("view_position = LATERAL. modality = CR. No findings.")

# inference parameters
# prompt = "cxr no finding"
guidance_scale = 4
num_steps = 75

# output directory
out_dir = "../output/results_2024_02_17/"
if not os.path.exists(out_dir): os.mkdir(out_dir)

# for i in tqdm(range(num_images)):
#     image = pipe(prompt, num_inference_steps=num_steps, guidance_scale=guidance_scale).images[0]
#     image.save(os.path.join(out_dir, f"synth_{i:04d}.png"))

for i, prompt in enumerate(prompts):
    for j in tqdm(range(num_images)):
        image = pipe(prompt, num_inference_steps=num_steps, guidance_scale=guidance_scale).images[0]
        # image.save(os.path.join(out_dir, f"prompt{i}_{j:03d}.png"))
        image.save(os.path.join(out_dir, f"{j:04d}.png"))