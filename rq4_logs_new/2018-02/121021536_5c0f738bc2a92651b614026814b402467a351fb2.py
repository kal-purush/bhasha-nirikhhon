import numpy



trainData = numpy.genfromtxt('MNIST_training.csv', delimiter=',', skip_header=1)
testData = numpy.genfromtxt('MNIST_test.csv', delimiter=',', skip_header=1)