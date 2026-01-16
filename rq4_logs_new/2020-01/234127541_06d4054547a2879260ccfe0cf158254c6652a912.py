from flask import Flask , request , jsonify
from flask_restful import Resource , Api
from io import BytesIO
import tensorflow as tf
import cloudinary
import cloudinary.api
import cloudinary.uploader
import numpy as np
import requests
import os


app = Flask(__name__)
api = Api(app)

cloudinary.config(
    cloud_name = os.environ['cloud_name'],
    api_key = os.environ['api_key'],
    api_secret = os.environ['api_secret']
)



def Conv2D_Block(input_tensor , n_filters):
  x = tf.keras.layers.Conv2D(filters = n_filters , kernel_size = (3 , 3) , kernel_initializer = 'he_normal' , padding = 'same')(input_tensor)
  x = tf.keras.layers.BatchNormalization()(x)
  x = tf.keras.layers.Activation('relu')(x)
  
  x = tf.keras.layers.Conv2D(filters = n_filters , kernel_size = (3 , 3) , kernel_initializer = 'he_normal' , padding = 'same')(x)
  x = tf.keras.layers.BatchNormalization()(x)
  x = tf.keras.layers.Activation('relu')(x)
  
  return x


def U_Net(img_tensor , n_filters = 16):
  conv1 = Conv2D_Block(img_tensor , n_filters * 1)
  pool1 = tf.keras.layers.MaxPooling2D((2 , 2))(conv1)
  pool1 = tf.keras.layers.Dropout(0.05)(pool1)
  
  conv2 = Conv2D_Block(pool1 , n_filters * 2)
  pool2 = tf.keras.layers.MaxPooling2D((2 , 2))(conv2)
  pool2 = tf.keras.layers.Dropout(0.05)(pool2)
  
  conv3 = Conv2D_Block(pool2 , n_filters * 4)
  pool3 = tf.keras.layers.MaxPooling2D((2 , 2))(conv3)
  pool3 = tf.keras.layers.Dropout(0.05)(pool3)
  
  conv4 = Conv2D_Block(pool3 , n_filters * 8)
  pool4 = tf.keras.layers.MaxPooling2D((2 , 2))(conv4)
  pool4 = tf.keras.layers.Dropout(0.05)(pool4)
  
  conv5 = Conv2D_Block(pool4 , n_filters * 16)
  
  pool6 = tf.keras.layers.Conv2DTranspose(n_filters * 8 , (3 , 3) , (2, 2) , padding = 'same')(conv5)
  pool6 = tf.keras.layers.concatenate([pool6 , conv4])
  pool6 = tf.keras.layers.Dropout(0.05)(pool6)
  conv6 = Conv2D_Block(pool6 , n_filters * 8)
  
  pool7 = tf.keras.layers.Conv2DTranspose(n_filters * 4 , (3 , 3) , (2 , 2) , padding = 'same')(conv6)
  pool7 = tf.keras.layers.concatenate([pool7 , conv3])
  pool7 = tf.keras.layers.Dropout(0.05)(pool7)
  conv7 = Conv2D_Block(pool7 , n_filters * 4)
  
  pool8 = tf.keras.layers.Conv2DTranspose(n_filters * 2 , (3 , 3) , (2 , 2) , padding = 'same')(conv7)
  pool8 = tf.keras.layers.concatenate([pool8 , conv2])
  pool8 = tf.keras.layers.Dropout(0.05)(pool8)
  conv8 = Conv2D_Block(pool8 , n_filters * 2)
  
  pool9 = tf.keras.layers.Conv2DTranspose(n_filters * 1 , (3 , 3) , (2 , 2) , padding = 'same')(conv8)
  pool9 = tf.keras.layers.concatenate([pool9 , conv1])
  pool9 = tf.keras.layers.Dropout(0.05)(pool9)
  conv9 = Conv2D_Block(pool9 , n_filters * 1)
  
  output = tf.keras.layers.Conv2D(1 , (1 , 1) , activation = 'sigmoid')(conv9)
  
  u_net = tf.keras.Model(inputs = [img_tensor] , outputs = [output])
  
  return u_net


img_tensor = tf.keras.layers.Input((128 , 128 , 3) , name = 'img')
model = U_Net(img_tensor)
model.compile(optimizer = tf.keras.optimizers.Adam(),
            loss = 'binary_crossentropy',
            metrics = ['accuracy'])

model.load_weights('./weights/sky_seg_weights.hdf5')

class SegmentationApi(Resource):
    def post(self):
        data = request.get_json()
        image_url = data['url']
        image_file = requests.get(image_url)
        image = tf.keras.preprocessing.image.load_img(BytesIO(image_file.content) , target_size=(128 , 128))
        image = tf.keras.preprocessing.image.img_to_array(image , dtype='float32')
        image = image / 255.0

        print(image.shape)

        pred = model.predict(tf.expand_dims(image , axis=0))
        pred = np.ma.masked_where(pred == 0 , pred)
        mask = tf.keras.preprocessing.image.array_to_img(np.expand_dims(np.squeeze(pred) , axis=-1))
        mask.save('./masks/mask.png')

        cloudinary.api.delete_all_resources()
        result = cloudinary.uploader.upload_image('./masks/mask.png')
        image_url = result.url

        return {'mask' : image_url} , 201


api.add_resource(SegmentationApi , '/')



if __name__ == '__main__':
    app.run(host='192.168.1.201' , debug=True)

    

