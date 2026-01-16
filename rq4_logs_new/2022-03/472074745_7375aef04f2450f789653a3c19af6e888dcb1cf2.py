import os
import glob
from sklearn.model_selection import train_test_split

def split_data(path_data, train_folder, val_folder, split_size=0.15):

    folders = os.listdir(path_data) #gives a list of all the folders in the directory

    for folder in folders:
        full_path = os.path.join(path_data,folder)
        images_paths = glob.glob(os.path.join(full_path, '*.png'))

        x_train, x_val = train_test_split(images_paths, test_size=split_size)

        for x in x_train:

            basename = os.path.basename(x)
            path_to_folder = os.path.join()