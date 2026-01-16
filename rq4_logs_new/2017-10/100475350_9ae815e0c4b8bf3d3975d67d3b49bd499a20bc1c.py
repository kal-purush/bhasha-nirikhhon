import numpy as np


# Number if iterations for training.
# EPOCHS = 50000

def sigmoid(num):
  return 1 / (1 + np.exp(-num))


def sigmoid_derivative(num):
  return num * (1 - num)


# Inputs with a bias, always being 1.
inputs = np.array([[0, 0, 1], [0, 1, 1], [1, 0, 1], [1, 1, 1]])

outputs = np.array([[1], [0], [0], [1]])


# Seed for deterministic behaviour, mainly for debugging.
np.random.seed(1)


def main(num_layers=2, neurons=(3, 4, 1)):
  """The main function, with customizable network.

  Args:
    num_layers: Number of layers, excluding the o/p layer for now.
    neurons: Array of number of neurons in each layer.
  """
  epochs = 50000
  # Assert last layer has 1 neuron and len(neurons) == num_layers + 1.
  # This is gonna change later as the o/p doesn't stay 1 bit later.
  assert neurons[num_layers] == 1
  # Weights for each layer. Random (-1, 1) for now.
  weights = []
  # nth weight means weights fron nth to (n+1)th row in network.
  # for index, layer_num in enumerate(range(num_layers)):
  for index in range(num_layers):
    weights.append(2 * np.random.random(neurons[index:index + 2]))

  for training_round in range(epochs):
    # Make this into a multilaer network later.
    # O/p of layer 1.
    predicted_1 = sigmoid(np.dot(inputs, weights[0]))
    predicted_2 = sigmoid(np.dot(predicted_1, weights[1]))

    # Print predictions.
    if not training_round % 10000:
      print 'Predictions after {} training runs:'.format(training_round)
      print predicted_2

    # Error computation.
    error_2 = outputs - predicted_2
    delta_2 = error_2 * sigmoid_derivative(predicted_2)
    error_1 = delta_2.dot(weights[1].T)
    delta_1 = error_1 * sigmoid_derivative(predicted_1)

    # Update weights.
    weights[1] += predicted_1.T.dot(delta_2)
    weights[0] += inputs.T.dot(delta_1)

  print 'Output after training:'
  print predicted_2


if __name__ == '__main__':
  main()