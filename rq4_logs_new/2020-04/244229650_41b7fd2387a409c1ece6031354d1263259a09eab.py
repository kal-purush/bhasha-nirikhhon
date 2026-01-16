import sys
import pickle
import os
import numpy as np


class DataReader:
  def __init__(self, path):
    patient_file_paths = list(map(lambda f : os.path.join(path, f), os.listdir(path)))
    self.x = []
    self.y = []
    for patient_file_path in patient_file_paths:
      with (open(patient_file_path, "rb")) as patient_file:
        while True:
          try:
            patient_data = pickle.load(patient_file)
            multi_channel_picture = np.expand_dims(patient_data.contour_diff_matricies[0], axis=0)
            multi_channel_picture = np.append(multi_channel_picture, np.expand_dims(patient_data.contour_diff_matricies[1], axis=0), axis=0)
            multi_channel_picture = np.append(multi_channel_picture, np.expand_dims(patient_data.contour_diff_matricies[2], axis=0), axis=0)
            self.x.append(multi_channel_picture)
            self.y.append(int(patient_data.pathology == 'HCM'))
          except EOFError:
            break