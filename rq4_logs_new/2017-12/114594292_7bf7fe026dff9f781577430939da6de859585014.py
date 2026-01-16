##### THIS LEARNING ALGORITHM DECIDES WHETHER AN IMAGE CONTAINS A PERSON OR NOT
##### INPUT: IMAGE 
##### OUTPUT: Y/N AS TO WHETHER IT CONTAINS A PERSON

import time
import numpy as np
import h5py
import matplotlib.pyplot as plt
import scipy
from PIL import Image
from scipy import ndimage
from pnp_app_utils import *

#get_ipython().magic('matplotlib inline')
plt.rcParams['figure.figsize'] = (5.0, 4.0) # set default size of plots
plt.rcParams['image.interpolation'] = 'nearest'
plt.rcParams['image.cmap'] = 'gray'

# np.random.seed(1)

# Load the training and test sets
train_x_orig, train_y, val_x_orig, val_y, test_x_orig, test_y = load_data()

# Load a sample image and show it
print("Loading a sample image from training set...")

# This index value can be set to anything between 1 and 915
index = 598
plt.imshow(train_x_orig[index].astype(np.uint8))
plt.show()

if train_y[index] == 0:
    print ("y = 0, so this picture does not contain a person.")
elif train_y[index] == 1:
    print ("y = 1, so this picture does contain a person.")

input("\nPress Enter to continue.")


# Explore your dataset 
m_train = train_x_orig.shape[0]
m_test = test_x_orig.shape[0]
m_val = val_x_orig.shape[0]

num_px_x = train_x_orig.shape[1]
num_px_y = train_x_orig.shape[2]

print("Some information about this dataset: ")
print ("Number of training examples: " + str(m_train))
print ("Number of validation examples: " + str(m_val))
print ("Number of testing examples: " + str(m_test))
print ("Each image is of size: (" + str(num_px_x) + ", " + str(num_px_y) + ", 3)")
print ("train_x_orig shape: " + str(train_x_orig.shape))
print ("train_y shape: " + str(train_y.shape))
print ("val_x_orig shape: " + str(val_x_orig.shape))
print ("val_y shape: " + str(val_y.shape))
print ("test_x_orig shape: " + str(test_x_orig.shape))
print ("test_y shape: " + str(test_y.shape))

input("\nPress Enter to continue.")


print ("Now we will flatten the shapes of the train, test, and val data set.")

# Reshape the training, val, and test examples 
train_x_flatten = train_x_orig.reshape(train_x_orig.shape[0], -1).T  
val_x_flatten = val_x_orig.reshape(val_x_orig.shape[0], -1).T
test_x_flatten = test_x_orig.reshape(test_x_orig.shape[0], -1).T


# Standardize data to have feature values between 0 and 1.
train_x = train_x_flatten/255.
val_x = val_x_flatten/255.
test_x = test_x_flatten/255.

print ("train_x's shape: " + str(train_x.shape))
print ("val_x's shape: " + str(val_x.shape))
print ("test_x's shape: " + str(test_x.shape))

    
input("\nNow we will train a two-layer neural network to identify people in photos. Press Enter to continue.")

# Constants for the two-layer model
n_x = num_px_x * num_px_y * 3 # number of input units 
n_h = 7                       # number of hidden units
n_y = 1                       # number of output units
layer_dims = (n_x, n_h, n_y)

# Two layer model

def two_layer_model(X, Y, layer_dims, learning_rate, num_iterations, print_cost=False):

    ###  Implements a two-layer neural network: LINEAR->RELU->LINEAR->SIGMOID.
    
    ###  Arguments:
    ###  X -- input data, of shape (n_x, number of examples)
    ###  Y -- true "label" vector (containing 0 if cat, 1 if non-cat), of shape (1, number of examples)
    ###  layer_dims -- dimensions of the layers (n_x, n_h, n_y)
    ###  num_iterations -- number of iterations of the optimization loop
    ###  learning_rate -- learning rate of the gradient descent update rule
    ###  print_cost -- If set to True, this will print the cost every 100 iterations 
    
    ###  Returns:
    ###  parameters -- a dictionary containing W1, W2, b1, and b2

    
    np.random.seed(1)
    grads = {}
    costs = []                              # to keep track of the cost
    m = X.shape[1]                           # number of examples
    (n_x, n_h, n_y) = layer_dims
    
    parameters = initialize_parameters(n_x, n_h, n_y)

    
    # Get W1, b1, W2 and b2 from the dictionary parameters.
    W1 = parameters["W1"]
    b1 = parameters["b1"]
    W2 = parameters["W2"]
    b2 = parameters["b2"]
    
    # Loop (gradient descent)

    for i in range(0, num_iterations):

        # Forward propagation: LINEAR -> RELU -> LINEAR -> SIGMOID

        A1, cache1 = linear_activation_forward(X, W1, b1, "relu")
        A2, cache2 = linear_activation_forward(A1, W2, b2, "sigmoid")

        
        # Compute cost
        cost = compute_cost(A2, Y)

        
        # Initializing backward propagation
        dA2 = - (np.divide(Y, A2) - np.divide(1 - Y, 1 - A2))
        
        # Backward propagation

        dA1, dW2, db2 = linear_activation_backward(dA2, cache2, "sigmoid")
        dA0, dW1, db1 = linear_activation_backward(dA1, cache1, "relu")

        
        # Set grads
        grads['dW1'] = dW1
        grads['db1'] = db1
        grads['dW2'] = dW2
        grads['db2'] = db2
        
        # Update parameters
        parameters = update_parameters(parameters, grads, learning_rate)


        # Retrieve W1, b1, W2, b2 from parameters
        W1 = parameters["W1"]
        b1 = parameters["b1"]
        W2 = parameters["W2"]
        b2 = parameters["b2"]
        
        # Print the cost every 100 training example
        if print_cost and i % 100 == 0:
            print("Cost after iteration {}: {}".format(i, np.squeeze(cost)))
        if print_cost and i % 100 == 0:
            costs.append(cost)
       
    # plot the cost

    plt.plot(np.squeeze(costs))
    plt.ylabel('cost')
    plt.xlabel('iterations (per tens)')
    plt.title("Learning rate =" + str(learning_rate))
    plt.show()
    
    return parameters


input("\nModel is set up. Now we shall train the model. Press Enter to continue.")

parameters = two_layer_model(train_x, train_y, layer_dims = (n_x, n_h, n_y), learning_rate = 0.0075, num_iterations = 1000, print_cost=True)


input("\nModel is set up. Now we will run the two-layer model to predict on the training and validation sets. Press Enter to continue.")

predictions_train = predict(train_x, train_y, parameters)
predictions_val = predict(val_x, val_y, parameters)
predictions_test = predict(test_x, test_y, parameters)


input("\nTime to define a five-layer model. Press Enter to continue.")

# L-layer model (5 layers in this case)

# Define constants
layers_dims = [num_px_x * num_px_y * 3, 30, 6, 4, 1] 


def L_layer_model(X, Y, layers_dims, learning_rate, lambd, num_iterations, print_cost=False):

    ###  Implements a L-layer neural network: [LINEAR->RELU]*(L-1)->LINEAR->SIGMOID.
    
    ###  Arguments:
    ###  X -- data, numpy array of shape (number of examples, num_px * num_px * 3)
    ###  Y -- true "label" vector (containing 0 if cat, 1 if non-cat), of shape (1, number of examples)
    ###  layers_dims -- list containing the input size and each layer size, of length (number of layers + 1).
    ###  learning_rate -- learning rate of the gradient descent update rule
    ###  num_iterations -- number of iterations of the optimization loop
    ###  print_cost -- if True, it prints the cost every 100 steps
    
    ###  Returns:
    ###  parameters -- parameters learnt by the model. They can then be used to predict.

    costs = []                       
    
    # Parameters initialization.
    parameters = initialize_parameters_deep(layers_dims)
    
    # Loop (gradient descent)
    for i in range(0, num_iterations):

        # Forward propagation: [LINEAR -> RELU]*(L-1) -> LINEAR -> SIGMOID.
        AL, caches = L_model_forward(X, parameters)
        
        # Compute cost.
        cost = compute_cost_with_regularization(AL, Y, parameters, lambd)
    
        # Backward propagation.
        grads = L_model_backward_with_reg(AL, Y, caches, lambd)
 
        # Update parameters.
        parameters = update_parameters(parameters, grads, learning_rate)
                
        # Print the cost every 100 training example
        if print_cost and i % 100 == 0:
            print ("Cost after iteration %i: %f" %(i, cost))
        if print_cost and i % 100 == 0:
            costs.append(cost)
            
    # plot the cost
    plt.plot(np.squeeze(costs))
    plt.ylabel('cost')
    plt.xlabel('iterations (per tens)')
    plt.title("Learning rate =" + str(learning_rate))
    plt.show()
    
    return parameters

def L_layer_model_with_batches(X, Y, layers_dims, learning_rate, mini_batch_size, lambd, num_iterations, print_cost=False):

    ###  Implements a L-layer neural network: [LINEAR->RELU]*(L-1)->LINEAR->SIGMOID.
    
    ###  Arguments:
    ###  X -- data, numpy array of shape (number of examples, num_px * num_px * 3)
    ###  Y -- true "label" vector (containing 0 if cat, 1 if non-cat), of shape (1, number of examples)
    ###  layers_dims -- list containing the input size and each layer size, of length (number of layers + 1).
    ###  learning_rate -- learning rate of the gradient descent update rule
    ###  num_iterations -- number of iterations of the optimization loop
    ###  print_cost -- if True, it prints the cost every 100 steps
    
    ###  Returns:
    ###  parameters -- parameters learnt by the model. They can then be used to predict.

    costs = []
    seed = 0                         
    
    # Parameters initialization.
    parameters = initialize_parameters_deep(layers_dims)
    
    # Loop (gradient descent)
    for i in range(0, num_iterations):

        # Define the random minibatches. (Seed is included so that it reshuffles different for every iteration)
        seed = seed + 1
        minibatches = random_mini_batches(X, Y, mini_batch_size, seed)

        for minibatch in minibatches:

            # Forward propagation: [LINEAR -> RELU]*(L-1) -> LINEAR -> SIGMOID.
            AL, caches = L_model_forward(X, parameters)
            
            # Compute cost.
            cost = compute_cost_with_regularization(AL, Y, parameters, lambd)
        
            # Backward propagation.
            grads = L_model_backward_with_reg(AL, Y, caches, lambd)
     
            # Update parameters.
            parameters = update_parameters(parameters, grads, learning_rate)
                
        # Print the cost every 100 training example
        if print_cost and i % 100 == 0:
            print ("Cost after iteration %i: %f" %(i, cost))
        if print_cost and i % 100 == 0:
            costs.append(cost)
            
    # plot the cost
    plt.plot(np.squeeze(costs))
    plt.ylabel('cost')
    plt.xlabel('iterations (per tens)')
    plt.title("Learning rate =" + str(learning_rate))
    plt.show()
    
    return parameters

input("\nNow we will train a five-layer neural network to identify people in photos. Press Enter to continue.")

# Train the model 
parameters = L_layer_model(train_x, train_y, layers_dims, learning_rate = 0.009, lambd = 0.8, num_iterations = 1000, print_cost = True)
#parameters = L_layer_model_with_batches(train_x, train_y, layers_dims, learning_rate = 0.009, mini_batch_size = 64, lambd = 0.8, num_iterations = 1000, print_cost = True)

input("\nModel is trained. Now we will use it to predict images. Press Enter to continue.")

# Output the predictions
print("Accuracy on training set: ")
pred_train = predict(train_x, train_y, parameters)
print("Accuracy on validation set: ")
pred_val = predict(val_x, val_y, parameters)
print("Accuracy on test set: ")
pred_test = predict(test_x, test_y, parameters)