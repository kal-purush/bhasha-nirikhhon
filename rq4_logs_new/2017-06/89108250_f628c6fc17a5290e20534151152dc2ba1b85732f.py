from tqdm import tqdm
import numpy as np

from .dataset import Dataset
from constants import LABELS, WEATHER_IDX, TRAIN_DATA_DIR
from utils import get_resized_image


class WeatherInInputDataset(Dataset):
  """
  A dataset with only the weathers labels in input
  """
  
  input_length = 2

  def _xyData(self, image_data_fmt, isTraining, input_shape):
    dataset = self.training_files if isTraining else self.validation_files
    Y = []
    X = []
    X2 = []
    print("Reading inputs...")
    with tqdm(total=len(dataset)) as pbar:
      for f in dataset:
        file = f.split('/')[-1].split('.')[0]
        img, weather_tags = self.get_input(f, TRAIN_DATA_DIR, image_data_fmt, input_shape)
        X.append(img)
        X2.append(weather_tags)
        Y.append(self.outputs[file])
        pbar.update(1)
    return ([np.array(X), np.array(X2)], np.array(Y))

  def get_input(self, image_name, data_dir, image_data_fmt, input_shape):
    """
    Return the input corresponding to one image file
    """
    return get_resized_image(image_name, data_dir, image_data_fmt, input_shape), [self.outputs[image_name][i] for i in WEATHER_IDX]