from graphdiffusion.pipeline import *
import seaborn as sns
from graphdiffusion.plotting import plot_data_as_normal_pdf
from graphdiffusion import utils

with open("../graphdiffusion/imports.py", "r") as file:
    exec(file.read())
import os

if not os.path.exists("pokemon"):
    os.system("git clone https://github.com/gerritgr/pokemon_diffusion && cp -r pokemon_diffusion/pokemon pokemon/ && rm -rf pokemon_diffusion")
from torchvision import transforms
from torch.utils.data import Dataset, DataLoader, IterableDataset, TensorDataset
from PIL import Image

IMG_SIZE = 32




pipeline = PipelineImage(img_height=IMG_SIZE, img_width=IMG_SIZE, img_channels=3)
pipeline.visualize_foward(
    data=(torch.rand(1, 3, IMG_SIZE, IMG_SIZE)-0.5)*2.0,
    outfile="images/example6_noise_forward.jpg",
    plot_data_func=plot_image_on_axis,
    num=25,
)

pipeline.visualize_foward(
    data=(torch.rand(1, 3, IMG_SIZE, IMG_SIZE)-0.5)*2.0,
    outfile="images/example6_noise_forward2.jpg",
    plot_data_func=plot_data_as_normal_pdf,
    num=25,
)





import torch
from torchvision import datasets, transforms
from torch.utils.data import DataLoader

# Define a transform to normalize and convert images to 3 channels
transform = transforms.Compose([
    transforms.Resize(32),  # Resize images to 32x32 pixels
    transforms.Grayscale(num_output_channels=3),  # Convert to 3-channel grayscale images
    transforms.ToTensor(),  # Convert images to tensor
    transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))  # Normalize pixel values to [-1, 1]
])

# Load the FashionMNIST dataset
fashion_mnist_train = datasets.FashionMNIST(
    root='./data',  # Specify the path to store the data
    train=True,  # Load the training data
    download=True,  # Download the data if it's not already downloaded
    transform=transform  # Apply the defined transformations
)

# Create a DataLoader
train_loader = DataLoader(
    fashion_mnist_train,  # Dataset to load
    batch_size=10,  # Batch size
    shuffle=True,  # Shuffle the dataset
)

# Example: Iterate over the DataLoader
for images, labels in train_loader:
    print(images.shape)  # Should print [100, 3, 28, 28] indicating batch_size, channels, height, width
    break  # Break after the first batch to demonstrate



pipeline.visualize_foward(
    data=train_loader,
    outfile="images/example6_fmnist_forward.jpg",
    plot_data_func=plot_image_on_axis,
    num=25,
)


pipeline.visualize_foward(
    data=train_loader,
    outfile="images/example6_fmnist_forward2.jpg",
    plot_data_func=plot_data_as_normal_pdf,
    num=25,
)



pipeline.train(data=train_loader, epochs=10)