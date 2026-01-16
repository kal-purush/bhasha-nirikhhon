from keras.engine import  Model
from keras.layers import Input
from keras_vggface.vggface import VGGFace


from PIL import Image
import numpy as np

# Convolution Features
model = VGGFace()


if __name__ == "__main__":

    im = Image.open('A.J._Buckley.jpg')

    im = im.resize((224,224))

    im = np.array(im).astype(np.float32)

    im = np.expand_dims(im, axis=0)

    # Test pretrained model

    out = model.predict(im)

    print(out[0][0])