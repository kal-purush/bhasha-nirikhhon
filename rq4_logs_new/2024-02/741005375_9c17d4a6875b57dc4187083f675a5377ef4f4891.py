from graphdiffusion.pipeline import *
import seaborn as sns
from graphdiffusion.plotting import plot_data_as_normal_pdf
with open("../graphdiffusion/imports.py", "r") as file:
    exec(file.read())
import os
if not os.path.exists("pokemon"):
    os.system("git clone https://github.com/gerritgr/pokemon_diffusion && cp -r pokemon_diffusion/pokemon pokemon/ && rm -rf pokemon_diffusion" )
from torchvision import transforms 
from torch.utils.data import Dataset, DataLoader, IterableDataset, TensorDataset
from PIL import Image

IMG_SIZE = 16


def image_to_tensor(image):
    """Convert a PIL image to a PyTorch tensor.

    Args:
        image (PIL.Image): The image to be converted.

    Returns:
        torch.Tensor: The converted PyTorch tensor.
    """
    # Define the image transformation pipeline
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Resize((IMG_SIZE, IMG_SIZE), antialias=True),
        transforms.Lambda(lambda t: (t - 0.5) * 2.)
    ])

    # Apply the transformation pipeline to the image
    return transform(image)


def tensor_to_img(tensor):
    """Convert a PyTorch tensor to a PIL image.

    Args:
        tensor (torch.Tensor): The PyTorch tensor to be converted.

    Returns:
        PIL.Image: The converted PIL image.
    """
    # Define the tensor transformation pipeline
    transform = transforms.Compose([
        transforms.Lambda(lambda t: (t + 1.) / 2.),
        transforms.Lambda(lambda t: torch.clamp(t, min=0., max=1.)),
        transforms.ToPILImage()
    ])

    # Apply the transformation pipeline to the tensor
    return transform(tensor)



class ImageDataset(Dataset):
  """A dataset for images that supports on-the-fly transformations (i.e., augmentations).

  Args:
      imgpaths (str, optional): The path pattern for the images. Default is "pokemon/*png".
  """

  def __init__(self, img_paths="pokemon/*png"):

    # You can/should play around with these.
    self.on_the_fly_transform = transforms.Compose([
    transforms.RandomHorizontalFlip(0.5),
    transforms.RandomApply(torch.nn.ModuleList([transforms.RandomCrop(int(IMG_SIZE*0.8)),]), p=0.1),
    transforms.RandomAutocontrast(p=0.1),
    transforms.Resize((IMG_SIZE, IMG_SIZE), antialias=True),
    transforms.Lambda(lambda t: torch.clamp(t, min=-1., max=1.)),
    ])

    self.img_list = list()
    for img_path in glob.glob(img_paths):
      # Turn the transparent part in the image to white following 
      # https://stackoverflow.com/questions/50898034/how-replace-transparent-with-a-color-in-pillow
      image = Image.open(img_path)
      rgba_image = Image.new("RGBA", image.size, "WHITE")
      rgba_image.paste(image, (0, 0), image)   
      rgb_image = rgba_image.convert('RGB')

      # Convert the PIL image to a tensor, where each value is in [-1,1].
      img_as_tensor = image_to_tensor(rgb_image)
      self.img_list.append(img_as_tensor)

  def __getitem__(self, index):
    """Get an image tensor from the dataset with on-the-fly transformation.

    Args:
        index (int): The index of the image tensor in the dataset.

    Returns:
        torch.Tensor: The image tensor with on-the-fly transformation.
    """
    img = self.img_list[index]
    img = self.on_the_fly_transform(img)

    return img.flatten()
  
  def get_pil_image(self, index, with_random_augmentation=True):
    """Get a PIL image from the dataset with or without on-the-fly transformation.

    Args:
        index (int): The index of the PIL image in the dataset.
        with_random_augmentation (bool, optional): Whether to apply on-the-fly transformation. Default is True.

    Returns:
        PIL.Image: The PIL image with or without on-the-fly transformation.
    """
    if with_random_augmentation:
      return tensor_to_img(self.__getitem__(index))
    return tensor_to_img(self.img_list[index].reshape(3, IMG_SIZE, IMG_SIZE))

  def __len__(self):
    return len(self.img_list)


dataset = ImageDataset()
dataloader = DataLoader(dataset, batch_size=100, shuffle=True)


for data in dataloader:
    print(data.shape)
    break


def plot_image_on_axis(array, axis, arrays):
    array = array[0,:]
    array = array.reshape(3, IMG_SIZE, IMG_SIZE)
    array = tensor_to_img(torch.tensor(array))
    #array = to_numpy_array(array)
    axis.imshow(array)

#pre_trained_path="../pre_trained/vectordenoiser_pokemon_weights.pt"
degradation_obj = VectorDegradationDDPM()
pipeline = PipelineVector(node_feature_dim=3*IMG_SIZE*IMG_SIZE, hidden_dim=1024, num_layers=12, dropout_rate=0.3, degradation_obj=degradation_obj)
pipeline.visualize_foward(
    data=dataloader,
    outfile="pokemon_forward.jpg",
    plot_data_func=plot_image_on_axis,
    num=25,
)



pipeline.train(data=dataloader, epochs=10000)
pipeline.reconstruction_obj.save_model(pre_trained_path="../pre_trained/vectordenoiser_pokemon_weights.pt")

#data0 = pipeline.inference(dataloader, noise_to_start=None, steps=1000)
#print(data0)

pipeline.config["vectorbridge_magnitude_scale"] = 1.0
pipeline.visualize_reconstruction(
    data=dataloader,
    plot_data_func=plot_image_on_axis,
    outfile="pokemon_backward_normal.jpg",
    num=25,
    steps=100,
)


pipeline.bridge_obj = VectorBridgeNaive()
pipeline.visualize_reconstruction(
    data=dataloader,
    plot_data_func=plot_image_on_axis,
    outfile="pokemon_backward_naive.jpg",
    num=25,
    steps=100,
)


pipeline.bridge_obj = VectorBridgeColdDiffusion()
pipeline.visualize_reconstruction(
    data=dataloader,
    plot_data_func=plot_image_on_axis,
    outfile="pokemon_backward_cold.jpg",
    num=25,
    steps=100,
)


pipeline.bridge_obj = VectorBridgeDDPM()
pipeline.visualize_reconstruction(
    data=dataloader,
    plot_data_func=plot_image_on_axis,
    outfile="pokemon_backward_ddpm.jpg",
    num=25,
    steps=100,
)

pipeline.bridge_obj = VectorBridgeAlt()
pipeline.visualize_reconstruction(
    data=dataloader,
    plot_data_func=plot_image_on_axis,
    outfile="pokemon_backward_alt.jpg",
    num=25,
    steps=100,
)