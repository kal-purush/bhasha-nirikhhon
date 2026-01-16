import numpy as np
from keras.models import load_model
from keras.utils import to_categorical
from keras.utils import plot_model
import os
import PIL

model = load_model('model12.h5')

im = PIL.Image.open('2.jpeg')
im = im.convert('1') # convert image to black and white
im = np.array(im.resize((28, 28), PIL.Image.ANTIALIAS))
im = im[..., np.newaxis]
output = model.predict(np.array([im]), batch_size=32, verbose=2)
print(str(np.argmax(output)))