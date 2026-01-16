"""
Classes and functions to handle data
"""
import torch
import torchvision.transforms as transforms
from torch.utils.data import DataLoader
from torch.utils.data.dataset import Dataset
import torchvision
import torchvision.transforms as transforms
import random  
from tifffile import TiffFile
import numpy as np
import cv2

import tensorflow as tf
# import tensorflow_io as tfio

#Classe Dataset
class CustomDataset(Dataset):
    def __init__(self, image_paths, mask_paths, train=True):   # initial logic happens like transform

        self.image_paths = image_paths
        self.mask_paths = mask_paths
        self.transforms = transforms.ToTensor()
       
    def __getitem__(self, idx):
        #tmask = np.empty((0,65536))
        if torch.is_tensor(idx):
            idx = idx.tolist()
        with TiffFile(self.image_paths[idx]) as tif :
            img = tif.asarray()
        if img.dtype == np.uint16:
            scale = 255/2200
            byte_im = (img)*scale
            byte_im = (byte_im.clip(0, 255) + 0.5).astype(np.uint8)
        image = byte_im
        with TiffFile(self.mask_paths[idx]) as tif :
            mask = tif.asarray()
        t_image = self.transforms(image)
        t_mask = torch.tensor(mask,dtype = torch.long)
        ID = str(self.image_paths[idx])
        #retourne un dictionnaire avec comme paramètre image, mask et ID image. 
        return {"image": t_image, "masque" : t_mask, "id" : ID} ### rajouté

    def __len__(self):  # return count of sample we have
        return len(self.image_paths)
    
    
class CustomTestDataset(Dataset):
    def __init__(self, image_paths, train=True):   # initial logic happens like transform

        self.image_paths = image_paths
        self.transforms = transforms.ToTensor()
       
    def __getitem__(self, idx):
        if torch.is_tensor(idx):
            idx = idx.tolist()
        with TiffFile(self.image_paths[idx]) as tif :
            img = tif.asarray()
        if img.dtype == np.uint16:
            scale = 255/2200
            byte_im = (img)*scale
            byte_im = (byte_im.clip(0, 255) + 0.5).astype(np.uint8)
        image = byte_im
        t_image = self.transforms(image)
        ID = str(self.image_paths[idx])
        #retourne un dictionnaire avec comme paramètre image, mask et ID image. 
        return {"image": t_image,  "id" : ID} ### rajouté

    def __len__(self):  # return count of sample we have
        return len(self.image_paths)

class CustomDatasetSnow(Dataset):
    def __init__(self, image_paths, mask_paths, train=True):   # initial logic happens like transform

        self.image_paths = image_paths
        self.mask_paths = mask_paths
        self.transforms = transforms.ToTensor()
       
    def __getitem__(self, idx):
        #tmask = np.empty((0,65536))
        if torch.is_tensor(idx):
            idx = idx.tolist()
        with TiffFile(self.image_paths[idx]) as tif :
            img = tif.asarray()
        if img.dtype == np.uint16:
            scale = 255/2200
            byte_im = (img)*scale
            byte_im = (byte_im.clip(0, 255) + 0.5).astype(np.uint8)
        image = byte_im
        with TiffFile(self.mask_paths[idx]) as tif :
            mask = tif.asarray()
        mask[((mask>0) & (mask<8))|(mask==9)]=0
        mask[mask==8]=1
        t_image = self.transforms(image)
        t_mask = torch.tensor(mask,dtype = torch.long)
        ID = str(self.image_paths[idx])
        #retourne un dictionnaire avec comme paramètre image, mask et ID image. 
        return {"image": t_image, "masque" : t_mask, "id" : ID} ### rajouté

    def __len__(self):  # return count of sample we have
        return len(self.image_paths)
    

class LandCoverData():
    """Class to represent the S2GLC Land Cover Dataset for the challenge,
    with useful metadata and statistics.
    """
    # image size of the images and label masks
    IMG_SIZE = 256
    # the images are RGB+NIR (4 channels)
    N_CHANNELS = 4
    # we have 9 classes + a 'no_data' class for pixels with no labels (absent in the dataset)
    N_CLASSES = 10
    CLASSES = [
        'no_data',
        'clouds',
        'artificial',
        'cultivated',
        'broadleaf',
        'coniferous',
        'herbaceous',
        'natural',
        'snow',
        'water'
    ]
    # classes to ignore because they are not relevant. "no_data" refers to pixels without
    # a proper class, but it is absent in the dataset; "clouds" class is not relevant, it
    # is not a proper land cover type and images and masks do not exactly match in time.
    IGNORED_CLASSES_IDX = [0, 1]

    # The training dataset contains 18491 images and masks
    # The test dataset contains 5043 images and masks
    TRAINSET_SIZE = 18491
    TESTSET_SIZE = 5043

    # for visualization of the masks: classes indices and RGB colors
    CLASSES_COLORPALETTE = {
        0: [0,0,0],
        1: [255,25,236],
        2: [215,25,28],
        3: [211,154,92],
        4: [33,115,55],
        5: [21,75,35],
        6: [118,209,93],
        7: [130,130,130],
        8: [255,255,255],
        9: [43,61,255]
        }
    CLASSES_COLORPALETTE = {c: np.asarray(color) for (c, color) in CLASSES_COLORPALETTE.items()}

    # statistics
    # the pixel class counts in the training set
    TRAIN_CLASS_COUNTS = np.array(
        [0, 20643, 60971025, 404760981, 277012377, 96473046, 333407133, 9775295, 1071, 29404605]
    )
    MEAN_CHANNEL = [ 339.42029674, 570.98497474,  539.11161384, 2634.49868179] 
    STD_CHANNEL = [ 339.79895785, 404.86935149,  549.41877854, 1071.38939764]
    WEIGHT_CLASS = np.array([0.0, 10.0, 5.0, 1.0, 2, 8.0, 2.0, 8.0, 10.0, 5])
    WEIGHT_CLASS_TEST_CLOUDS_SNOW= np.array([1.0, 9900.0])
    # the minimum and maximum value of image pixels in the training set
    TRAIN_PIXELS_MIN = 1
    TRAIN_PIXELS_MAX = 24356


def numpy_parse_image(image_path):
    """Load an image and its segmentation mask as numpy arrays and returning a tuple
    Args:
        image_path (bytes): path to image
    Returns:
        (numpy.array[uint16], numpy.array[uint8]): the image and mask arrays
    """
    image_path = Path(bytes.decode(image_path))
    # get mask path from image path:
    # image should be in a images/<image_id>.tif subfolder, while the mask is at masks/<image_id>.tif
    mask_path = image_path.parent.parent/'masks'/image_path.name
    with TiffFile(image_path) as tifi, TiffFile(mask_path) as tifm:
        image = tifi.asarray()
        mask = tifm.asarray()
        # add channel dimension to mask: (256, 256, 1)
        mask = mask[..., None]
    return image, mask


@tf.function(input_signature=[tf.TensorSpec(None, tf.string)])
def parse_image(image_path):
    """Wraps the parse_image function as a TF function"""
    image, mask = tf.numpy_function(numpy_parse_image, (image_path,), (tf.uint16, tf.uint8))
    image.set_shape([LandCoverData.IMG_SIZE, LandCoverData.IMG_SIZE, LandCoverData.N_CHANNELS])
    mask.set_shape([LandCoverData.IMG_SIZE, LandCoverData.IMG_SIZE, 1])
    return image, mask


@tf.function
def normalize(input_image, input_mask):
    """Rescale the pixel values of the images between 0.0 and 1.0"""
    image = tf.cast(input_image, tf.float32) / LandCoverData.TRAIN_PIXELS_MAX
    return image, input_mask


@tf.function
def load_image_train(input_image, input_mask):
    """Apply optional augmentations and normalize a train image and its label mask."""

    image, mask = input_image, input_mask
    if tf.random.uniform(()) > 0.5:
        image = tf.image.flip_left_right(image)
        mask = tf.image.flip_left_right(mask)
    if tf.random.uniform(()) > 0.5:
        image = tf.image.flip_up_down(image)
        mask = tf.image.flip_up_down(mask)
    if tf.random.uniform(()) > 0.5:
        image = tf.image.rot90(image)
        mask = tf.image.rot90(mask)
    elif tf.random.uniform(()) > 0.5:
        image = tf.image.rot90(image, k=3)
        mask = tf.image.rot90(mask, k=3)

    image, mask = normalize(image, mask)
    return image, mask


@tf.function
def load_image_test(input_image, input_mask):
    """Normalize test image and its label mask."""
    image, mask = normalize(input_image, input_mask)
    return image, mask



def Data_augmentation_image(mon_image, a):
    if a ==0: image = mon_image
    if a ==1: image = cv2.rotate(mon_image, cv2.ROTATE_90_CLOCKWISE)
    if a ==2: image = cv2.rotate(mon_image, cv2.ROTATE_180)
    if a ==3: image = cv2.rotate(mon_image, cv2.ROTATE_90_COUNTERCLOCKWISE)
    return image
def Data_augmentation_mask(mon_masque, a):   
    if a==0: masques=mon_masque
    if a ==1: masques = cv2.rotate(mon_masque, cv2.ROTATE_90_CLOCKWISE)
    if a ==2 : masques = cv2.rotate(mon_masque, cv2.ROTATE_180)
    if a ==3 : masques = cv2.rotate(mon_masque, cv2.ROTATE_90_COUNTERCLOCKWISE)
    return masques
#Classe Dataset
class CustomDatasetaugmentation(Dataset):
    def __init__(self, image_paths, mask_paths, train=True):   # initial logic happens like transform

        self.image_paths = image_paths
        self.mask_paths = mask_paths
        self.transforms = transforms.ToTensor()
        self.train = train
       
    def __getitem__(self, idx):
        #tmask = np.empty((0,65536))
        if torch.is_tensor(idx):
            idx = idx.tolist()
        a = random.randint(0, 3)
        if self.train==False : 
            a=0
        with TiffFile(self.image_paths[idx]) as tif :
            imag = tif.asarray()
            images = Data_augmentation_image(imag, a)
        if images.dtype == np.uint16:
            scale = 255/2200
            byte_im = (images)*scale
            byte_im = (byte_im.clip(0, 255) + 0.5).astype(np.uint8)
        image = byte_im
        with TiffFile(self.mask_paths[idx]) as tif :
            mask = tif.asarray()
            masks = Data_augmentation_mask(mask, a)
        t_image = self.transforms(image)
        t_mask = torch.tensor(masks,dtype = torch.long)
        ID = str(self.image_paths[idx])
        #retourne un dictionnaire avec comme paramètre image, mask et ID image. 
        return {"image": t_image, "masque" : t_mask, "id" : ID} ### rajouté

    def __len__(self):  # return count of sample we have
        return len(self.image_paths)