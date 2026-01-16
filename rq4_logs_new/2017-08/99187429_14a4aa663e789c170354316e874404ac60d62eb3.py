import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix
from tensorflow.examples.tutorials.mnist import input_data

'''initial steps'''
#tensorflow loading mnist data
#loading data as one hot vector
'''one hot vector-it means data labels (classes) is converted into columns representing binary representation of labels.same number
of rows but but number of columns is equal to number of classes.(0 or 1 representation)'''

data=input_data.read_data_sets("data/MNIST/", one_hot=True)

print("size od data loaded:")
print("Training set size:\t {}".format(len(data.train.labels)))
print("Test set size:\t {}".format(len(data.test.labels)))
print("Validation set size:\t {}".format(len(data.validation.labels)))
#index in one hot vector represents actual class whether its number 1,2,...8,9.
data.test.cls=np.array([label.argmax() for label in data.test.labels])

#default mnist image size is 28 by 28:
img_size=28

#storing images in one dimensional array like a onr dimensional vector represntation.length of one dim array[28*28]
img_flat=img_size*img_size

#shape of image with heigth,width
img_shape=(img_size,img_size)

#number of classses
no_of_classes=10

def plot_images(images, cls_true, cls_pred=None):
    assert len(images) == len(cls_true) == 9
    
    # Create figure with 3x3 sub-plots.
    fig, axes = plt.subplots(3, 3)
    fig.subplots_adjust(hspace=0.3, wspace=0.3)

    for i, ax in enumerate(axes.flat):
        # Plot image.
        ax.imshow(images[i].reshape(img_shape), cmap='binary')

        # Show true and predicted classes.
        if cls_pred is None:
            xlabel = "True: {0}".format(cls_true[i])
        else:
            xlabel = "True: {0}, Pred: {1}".format(cls_true[i], cls_pred[i])

        ax.set_xlabel(xlabel)
        
        # Remove ticks from the plot.
        ax.set_xticks([])
        ax.set_yticks([])
    plt.show()
		
# Get the first images from the test-set.
images = data.test.images[0:9]

# Get the true classes for those images.
cls_true = data.test.cls[0:9]

# Plot the images and labels using our helper-function above.
plot_images(images=images, cls_true=cls_true)

#none means it will accept any number of input images with each vector representing size of 28 by 28.
x=tf.placeholder(tf.float32,[None,img_flat])


y_true=tf.placeholder(tf.int64,[None,no_of_classes])

#2-D tensor representing weights
weights=tf.Variable(tf.zeros([img_flat,no_of_classes]))

#1-D tensor representing biases
biases=tf.Variable(tf.zeros([no_of_classes]))

#now trying to build a  model
#the element of the ith row and jth column is an estimate of how likely the ith input image is to be of the jth class.
logits = tf.matmul(x, weights) + biases

y_pred = tf.nn.softmax(logits)

#predicting the label
y_pred_cls = tf.argmax(y_pred, dimension=1)

#cost function calculation.i.e. comparing true labels with predicted labels.
cross_entropy = tf.nn.softmax_cross_entropy_with_logits(logits=logits,labels=y_true)


#in earlier step cross_entropy was calculated for each image.Now use all cross_entropy values and calculate its means to know
#how our model well our model is evaluating.
cost = tf.reduce_mean(cross_entropy)

optimizer = tf.train.GradientDescentOptimizer(learning_rate=0.5).minimize(cost)

correct_prediction = tf.equal(y_pred_cls, y_true_cls)
accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

#########################################################################################

'''Here tensorflow graph generation code ends '''
#########################################################################################
session = tf.Session()









