import numpy
import scipy.special
import sys


def load_data(f1, f2, f3):
    train_file = open(f1, 'r')
    train_images = train_file.readlines()
    train_file.close()
    train_label_file = open(f2, 'r')
    train_labels = train_label_file.readlines()
    train_label_file.close()
    test_file = open(f3, 'r')
    test_images = test_file.readlines()
    test_file.close()
    return train_images,train_labels,test_images

def output_csv(result):
    numpy.savetxt('test_predictions.csv',result,delimiter=',', fmt='%d')

class neuralNetwork:
    def __init__(self, input_nodes, hidden_layer1_nodes, hidden_layer2_nodes, output_nodes, learning_rate):
        self.input_nodes = input_nodes
        self.hidden_layer1_nodes = hidden_layer1_nodes
        self.hidden_layer2_nodes = hidden_layer2_nodes
        self.output_nodes = output_nodes
        self.learning_rate = learning_rate

        self.weight_input_to_hid1 = (numpy.random.normal(0.0, pow(self.hidden_layer1_nodes, -0.5), (self.hidden_layer1_nodes, self.input_nodes)))
        self.weight_hid1_to_hid2 = (numpy.random.normal(0.0, pow(self.hidden_layer2_nodes, -0.5), (self.hidden_layer2_nodes, self.hidden_layer1_nodes)))
        self.weight_hid2_to_output = (numpy.random.normal(0.0, pow(self.output_nodes, -0.5), (self.output_nodes, self.hidden_layer2_nodes)))

        self.bias_hid1 = numpy.random.randn(self.hidden_layer1_nodes, 1)
        self.bias_hid2 = numpy.random.randn(self.hidden_layer2_nodes, 1)
        self.bias_out = numpy.random.randn(self.output_nodes, 1)

        self.sigmoid_activation_function = lambda x: scipy.special.expit(x)

    def feed_forward(self, inputs):
        hidden1_inputs = numpy.dot(self.weight_input_to_hid1, inputs) + self.bias_hid1
        hidden1_outputs = self.sigmoid_activation_function(hidden1_inputs)

        hidden2_inputs = numpy.dot(self.weight_hid1_to_hid2, hidden1_outputs) + self.bias_hid2
        hidden2_outputs = self.sigmoid_activation_function(hidden2_inputs)

        final_inputs = numpy.dot(self.weight_hid2_to_output, hidden2_outputs) + self.bias_out
        final_outputs = self.sigmoid_activation_function(final_inputs)

        return hidden1_outputs, hidden2_outputs, final_outputs

    def cross_entropy(self,label, final_outputs):
        output_errors = label - final_outputs
        hidden2_errors = numpy.dot(numpy.transpose(self.weight_hid2_to_output), output_errors)
        hidden1_errors = numpy.dot(numpy.transpose(self.weight_hid1_to_hid2), hidden2_errors)
        return output_errors, hidden2_errors, hidden1_errors

    def backward_propagation(self, output_errors, final_outputs, hidden2_outputs,
                             hidden2_errors, hidden1_outputs, hidden1_errors, train_list):
        self.weight_hid2_to_output += self.learning_rate * numpy.dot((output_errors *
                                                                      final_outputs * (1.0 - final_outputs)),
                                                                     numpy.transpose(hidden2_outputs))
        self.bias_out += self.learning_rate * (output_errors * final_outputs * (1.0 - final_outputs))

        self.weight_hid1_to_hid2 += self.learning_rate * numpy.dot((hidden2_errors *
                                                                    hidden2_outputs * (1.0 - hidden2_outputs)),
                                                                   numpy.transpose(hidden1_outputs))
        self.bias_hid2 += self.learning_rate * (hidden2_errors * hidden2_outputs * (1.0 - hidden2_outputs))

        self.weight_input_to_hid1 += self.learning_rate * numpy.dot((hidden1_errors *
                                                                     hidden1_outputs * (1.0 - hidden1_outputs)),
                                                                    numpy.transpose(train_list))
        self.bias_hid1 += self.learning_rate * (hidden1_errors * hidden1_outputs * (1.0 - hidden1_outputs))

    def train(self, train_list, label):
        train_list = numpy.transpose(numpy.array(train_list, ndmin=2))
        label = numpy.transpose(numpy.array(label, ndmin=2))
        hidden1_outputs, hidden2_outputs, final_outputs = self.feed_forward(train_list)
        output_errors, hidden2_errors, hidden1_errors = self.cross_entropy(label, final_outputs)
        self.backward_propagation(output_errors, final_outputs, hidden2_outputs,
                                 hidden2_errors, hidden1_outputs, hidden1_errors, train_list)

    def query(self, test_list):
        inputs = numpy.transpose(numpy.array(test_list, ndmin=2))
        hidden1_outputs, hidden2_outputs, final_outputs = self.feed_forward(inputs)
        return final_outputs


if __name__ == '__main__':
    input_nodes = 784
    hidden_layer1_nodes = 256
    hidden_layer2_nodes = 128
    output_nodes = 10
    learning_rate = 0.05
    epochs = 10
    train_images, train_labels, test_images = load_data(sys.argv[1], sys.argv[2], sys.argv[3])
    n = neuralNetwork(input_nodes, hidden_layer1_nodes, hidden_layer2_nodes, output_nodes, learning_rate)

    for e in range(epochs):
        for i in range(len(train_images)):
            train_image = train_images[i].split(',')
            inputs = (numpy.asarray(train_image,dtype=numpy.float64) / 255.0 * 0.99) + 0.01
            targets = numpy.zeros(output_nodes) + 0.01
            targets[int(train_labels[i][0])] = 0.99
            n.train(inputs, targets)

    test_label_answer = numpy.zeros((len(test_images),1))
    for i in range(len(test_images)):
        test_image = test_images[i].split(',')
        inputs = (numpy.asarray(test_image,dtype=numpy.float64) / 255.0 * 0.99) + 0.01
        outputs = n.query(inputs)
        predict_label = numpy.argmax(outputs)
        test_label_answer[i][0] = predict_label

    output_csv(test_label_answer)

   




