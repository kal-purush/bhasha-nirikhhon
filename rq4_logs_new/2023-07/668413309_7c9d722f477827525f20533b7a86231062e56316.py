from config import DEVICE, PATH_TO_MODEL, TRANSFORM_TRAIN
import segmentation_models_pytorch as smp
import torch
from PIL import Image
import numpy as np
import matplotlib.pyplot as plt

model = smp.Unet(
    encoder_name="resnet34",  # choose encoder, e.g. mobilenet_v2 or efficientnet-b7
    encoder_weights="imagenet",  # use `imagenet` pre-trained weights for encoder initialization
    in_channels=3,  # model input channels (1 for gray-scale images, 3 for RGB, etc.)
    classes=1,  # model output channels (number of classes in your dataset)
    )
model.load_state_dict(torch.load(PATH_TO_MODEL))
model.to(DEVICE)
model.eval()

pil_img = Image.open('./airbus-ship-detection/test_v2/00c3db267.jpg')
img = TRANSFORM_TRAIN(image=torch.tensor(np.array(pil_img), dtype=torch.float32).permute(2, 0, 1).unsqueeze(0).to(DEVICE))['image']
pred = model(img)
# Convert the prediction tensor to a NumPy array and squeeze the batch dimension (if present)
pred_np = pred.cpu().detach().numpy().squeeze()

# Assuming your model outputs the segmentation mask, you can use a threshold to convert it to binary (0 or 1)
threshold = -255
mask_binary = (pred_np > threshold).astype(np.uint8)

# Convert the mask to a PIL image
mask_pil = Image.fromarray(mask_binary * 255)

# Visualize the image and mask side by side
fig, axes = plt.subplots(1, 2, figsize=(10, 5))
axes[0].imshow(img)
axes[0].set_title("Input Image")
axes[0].axis("off")

axes[1].imshow(mask_pil, cmap='gray')
axes[1].set_title("Segmentation Mask")
axes[1].axis("off")

plt.show()