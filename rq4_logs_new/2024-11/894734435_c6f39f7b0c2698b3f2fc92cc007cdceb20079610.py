import numpy as np
from network import Network

# Example on AND gate
input_dim = 2
output_dim = 1

# AND Gate.
train_data = np.array([[0, 0], [0, 1], [1, 0], [1, 1]])
Y_true = np.array([[0], [0], [0], [1]])

# Initialize the model and add layers
my_model = Network()
my_model.add(4)   # First hidden layer with 4 neurons
my_model.add('relu')
my_model.add(1)   # Output layer with 1 neuron
my_model.add('sigmoid') 

# Fit the model with the specified input dimension
my_model.fit(input_dim)

# Train the model
my_model.train(train_data, Y_true, error='mse', epochs=5000, lr=0.01)

print(my_model.predict(train_data).round())