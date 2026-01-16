'''
This script is used to classify any unknown animal with the use of a pretrained model (and its
labels). The code used is nearly identical to the one found in the Jupyter notebook "Tensorboard"
and was created mainly for testing purposes on a Raspberry Pi 3. Due to hardware constraints,
running a Jupyter notebook was not possible.
'''
import tensorflow as tf
import time
import os
import cv2
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-p", "--path", help="path to the folder of images that should be classified")
ap.add_argument("-m", "--model", help="path to the model for the classification")
ap.add_argument("-l", "--labels", help="path to the labels used in the model")
args = vars(ap.parse_args())

root_path = args.get("path", None)
frozen_graph_path = args.get("model", None)
labels_path = args.get("labels", None)

if(root_path == None or frozen_graph_path == None or labels_path == None):
    print("\n\nPlease provide all the information necessary. See help for more details.\n\n")

def load_session_with_graph():
    '''
        Load the given frozden model graph "frozen_graph_path" for the classification
    '''
    with tf.gfile.FastGFile(frozen_graph_path, 'rb') as f:
        graph_def = tf.GraphDef()
        graph_def.ParseFromString(f.read())
        _ = tf.import_graph_def(graph_def, name='')

    sess = tf.Session()
    return sess

def load_images():
    '''
        Load all the images in the given picture folder "root_path"
    '''
    image_data = []
    image_labels = []
    corresponding_paths = []

    root_sub_dirs = os.listdir(root_path)
    for root_sub_dir in root_sub_dirs:
        root_sub_path = os.path.join(root_path, root_sub_dir)
        if os.path.isdir(root_sub_path):
            picture_names = os.listdir(root_sub_path)
            for picture_name in picture_names:
                if ".DS_Store" not in picture_name:
                    picture_path = os.path.join(root_sub_path, picture_name)
                    corresponding_paths.append(picture_path)
                    image_data.append(cv2.imread(picture_path))
                    image_labels.append(root_sub_dir)

    return image_data, image_labels, corresponding_paths

def predict(sess, image_data):
    #feed the image_data as input to the graph and get first prediction
    softmax_tensor = sess.graph.get_tensor_by_name('final_result:0')
    return sess.run(softmax_tensor, {'DecodeJpeg:0': image_data})

def predict_unknown_animal():
    '''
        Predict the animal on an image using the given labels "labels_path"
    '''
    sess = load_session_with_graph()
    image_data, images_label, corresponding_paths = load_images()
    label_lines = [line.rstrip().lstrip() for line in tf.gfile.GFile(labels_path)]
    results = []
    
    for image in image_data:
        predictions = predict(sess, image)
        predicted_class = predictions[0].argmax()
        results.append(label_lines[predicted_class])
    return results

'''
    Print the time the classification took for benchmarking reasons
'''
time_start = time.time()
listOfUnknownAnimals = predict_unknown_animal()
print listOfUnknownAnimals
time_end = time.time()
print("I worked for: "+str(time_end - time_start))