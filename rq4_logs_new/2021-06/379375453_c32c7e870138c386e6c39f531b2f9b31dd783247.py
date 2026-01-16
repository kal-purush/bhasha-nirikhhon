import streamlit as st
from PIL import Image
import matplotlib.pyplot as plt
import tensorflow as tf
import tensorflow_hub as hub
from tensorflow.keras import preprocessing
import numpy as np


st.header("Traffic sign Classifier")

def main():
    file_uploaded = st.file_uploader("Choose the file", type =['jpg','png','jpeg'])
    if file_uploaded is not None:
        image = Image.open(file_uploaded)
        figure = plt.figure()
        plt.imshow(image)
        plt.axis('off')
        st.pyplot(figure)
        result = predict_class(image)
        st.success('Identified Sign board is {}'.format(result))

def predict_class(image):
    classifier_model = tf.keras.models.load_model('traffic_classifier.h5')
    shape = ((30,30,3))
    model = tf.keras.Sequential([hub.KerasLayer(classifier_model, input_shape=shape)])
    test_image = image.resize((30,30))
    test_image = preprocessing.image.img_to_array(test_image)
    test_image = np.expand_dims(test_image, axis =0)
    class_names = ['Speed limit (20km/h)', 'Speed limit (30km/h)','Speed limit (50km/h)','Speed limit (60km/h)','Speed limit (70km/h)',
    'Speed limit (80km/h)','End of speed limit (80km/h)','Speed limit (100km/h)','Speed limit (120km/h)','No passing',
    'No passing veh over 3.5 tons','Right-of-way at intersection','Priority road','Yield','Stop','No vehicles',
    'Veh > 3.5 tons prohibited','No entry','General caution','Dangerous curve left','Dangerous curve right','Double curve',
    'Bumpy road','Slippery road','Road narrows on the right','Road work','Traffic signals','Pedestrians','Children crossing',
    'Bicycles crossing','Beware of ice/snow','Wild animals crossing','End speed + passing limits','Turn right ahead',
    'Turn left ahead','Ahead only','Go straight or right','Go straight or left','Keep right','Keep left','Roundabout mandatory',
    'End of no passing','End no passing veh > 3.5 tons' ]
        
    predictions = model.predict(test_image)
    scores = tf.nn.softmax(predictions[0])
    scores = scores.numpy()
    image_class = class_names[np.argmax(scores)]
    return image_class

if __name__ == '__main__':
    main()
   