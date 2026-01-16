import torch
from PIL import Image
from diffusers import DiffusionPipeline, StableDiffusionImg2ImgPipeline

# Set device - Note: FLUX.schnell works better on GPU, but can run on CPU
device = "cuda" if torch.cuda.is_available() else "cpu"
dtype = torch.float16 if torch.cuda.is_available() else torch.float32  # Use float16 on GPU, float32 on CPU

# Load the FLUX.schnell pipeline
pipe = StableDiffusionImg2ImgPipeline.from_pretrained(
    "black-forest-labs/FLUX.schnell", 
    torch_dtype=dtype,
    safety_checker=None
)
pipe = pipe.to(device)

# Specify the input image path
image_path = '/Users/dinul/Desktop/proj/outfit-transformer/PXL_20250209_185841907 (1).jpg'

# Load and preprocess the image
image_initial = Image.open(image_path).convert("RGB").resize((1024, 1024))

# Define prompt for wrinkle removal without altering the garment
prompt = (
    "Keep the clothing item exactly the same—same shape, color, texture, and design. "
    "Only enhance the image to remove wrinkles from the fabric, making it look freshly ironed. "
    "Improve the lighting and clarity to resemble a Zara product photo, but do not change the garment's appearance. "
    "Keep the result realistic and wearable, like a professionally photographed version of the same item."
)

# Process the image - adjusted parameters that work well with FLUX.schnell
# Use a lower strength (0.1) for subtle enhancements that won't change the garment identity
result = pipe(
    prompt=prompt,
    image=image_initial,
    num_inference_steps=50,
    strength=0.1,  # Lower strength for subtle changes
    guidance_scale=1.0,  # Lower guidance scale for less aggressive prompt following
)

# Get and save the result
generated_image = result.images[0]
generated_image.save('/Users/dinul/Desktop/proj/outfit-transformer/upd.jpg')