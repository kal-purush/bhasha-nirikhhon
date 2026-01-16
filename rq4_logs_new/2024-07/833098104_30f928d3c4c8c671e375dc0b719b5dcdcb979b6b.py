import torch
from torch.utils.data import Dataset
from skimage import color, transform
import os
import matplotlib.pyplot as plt
from torchvision import transforms
from transformers import SegformerFeatureExtractor

class XRayDataset(Dataset):
    """
    Load the lung X-ray images with their corresponding masks and set this as a group
    """
    def __init__(self, image_dir, mask_dir, feature_extractor, transform=None):
        self.image_dir = image_dir
        self.mask_dir = mask_dir
        self.feature_extractor = feature_extractor
        self.transform = transform
        self.images = sorted(os.listdir(image_dir))  # load files in a sorted manner

    def __len__(self):
        return len(self.images)

    def __getitem__(self, idx):
        """
        Gets the images and masks in the dataset according to the index.

        :return: processed images and masks
        """
        img_file = self.images[idx]
        # use 'split' to get the specific part in file name that we want
        img_number = img_file.split('_')[-1].split('.')[0]

        img_path = os.path.join(self.image_dir, img_file)
        mask_path = os.path.join(self.mask_dir, f'cxrmask_{img_number}.jpeg')

        image = plt.imread(img_path)
        mask = plt.imread(mask_path)
        new_size = (256, 256)  # resize
        image = transform.resize(image, new_size)
        mask = transform.resize(mask, new_size)

        # Check the number of image channels, if 4, convert to 3
        if image.shape[2] == 4:
            image = image[:, :, :3]

        # if RGB, convert to gray
        if image.ndim == 3:
            image = color.rgb2gray(image)
        if mask.ndim == 3:
            mask = color.rgb2gray(mask)
        # convert mask to binary images
        mask = (mask > 0.5).astype(float)

        # apply feature_extractor
        if self.transform:
            image = self.transform(image)
            mask = self.transform(mask)

        # Convert to required input format for SegFormer
        inputs = self.feature_extractor(images=image, return_tensors="pt")
        return inputs['pixel_values'].squeeze(), torch.tensor(mask, dtype=torch.float32)

# Convert images to PyTorch Tensor format and do normalization
def get_transform():
    return transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.5], std=[0.5])
    ])

# set file path
image_dir = 'bob/Downloads/PKU_summerschool/data/1000_external_data/image'
mask_dir = 'bob/Downloads/PKU_summerschool/data/1000_external_data/mask'

feature_extractor = SegformerFeatureExtractor.from_pretrained("nvidia/segformer-b0-finetuned-ade-512-512")

dataset = XRayDataset(image_dir=image_dir, mask_dir=mask_dir, feature_extractor=feature_extractor, transform=get_transform())