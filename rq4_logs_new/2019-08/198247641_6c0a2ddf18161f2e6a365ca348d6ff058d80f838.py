import sys
import os
import numpy as np
import pandas as pd
from utils import save_predictions, save_probabilities
from utils import preprocess_image
from utils import kappa_loss, ordinal_loss, cauchy_loss, correntropy_loss
# from sklearn.model_selection import train_test_split
# from sklearn.metrics import cohen_kappa_score
from keras.models import load_model

if len(sys.argv) > 1:
    file_path = sys.argv[1]
else:
    file_path = 'data'
if len(sys.argv) > 2:
    output_csv = sys.argv[2]
else:
    output_csv = 'output.csv'


model_path = '.'
model_name = 'DCNet.h5'  # Name of the model file to load
model = load_model(os.path.join(model_path, model_name), custom_objects = {'kappa_loss': kappa_loss, 'ordinal_loss': ordinal_loss, 'cauchy_loss': cauchy_loss, 'correntropy_loss': correntropy_loss})

fill_type = 'mix'  # Fill type for image proprocessing. 'crop, 'pad', or 'mix'
softmax_activation = False  # If True, softmax activation was used for the output layer, otherwise sigmoid was used
im_size = 299  # 299 for inception style networks, 244 for densenet style networks

### Load data
try:
    files = pd.read_csv(output_csv).iloc[:,0].values
except:
    files = []
    for f in sorted(os.listdir(file_path)):
        if f.endswith('.tif') or f.endswith('.jpeg') or f.endswith('.jpg'):
            files.append(f)

N = len(files)
print("Files:", N)
x_test = np.empty((N, im_size, im_size, 3), dtype = np.uint8)

for i in range(N):
    filename = files[i]
    filename = os.path.join(file_path, filename)
    if not (filename.endswith('.tif') or filename.endswith('.jpeg') or filename.endswith('.jpg') or filename.endswith('.png')):
        if os.path.exists(filename + '.tif'):
            filename += '.tif'
        elif os.path.exists(filename  + '.jpeg'):
            filename += '.jpeg'
        elif os.path.exists(filename + '.jpg'):
            filename += '.jpg'
        elif os.path.exists(filename + '.png'):
            filename += '.png'
        else:
            print("Check file extension")
    x_test[i, :, :, :] = preprocess_image(filename, fill_type = fill_type, desired_size = im_size)


def predict_test(x_test):
    if softmax_activation:
        y_test_raw = model.predict(x_test, verbose = 1)
        y_test = np.argmax(y_test_raw, axis = 1)
    else:
        y_test_raw = model.predict(x_test, verbose = 1)
        y_test = (y_test_raw > 0.5).astype(int).sum(axis=1) - 1
    
    save_predictions(y_test, files, save_name = output_csv)


if __name__ == "__main__":
    predict_test(x_test)