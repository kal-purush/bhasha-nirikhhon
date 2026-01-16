import numpy as np
import matplotlib.pyplot as plt
import math

from sklearn.model_selection import train_test_split

act_num = 35
gen_fac = 4
inputs = 100

act_index_end = act_num - gen_fac

iteration = 1000

weights = np.ones(35)
error_th = 0.05


def activation_matrix(num):
    idx = ((num + 1) * act_index_end) / inputs
    idx = math.floor(idx)

    arr = np.zeros(35)
    for i in range(idx, idx + gen_fac):
        arr[i] = 1

    return arr, idx


def generate_data(func):
    max_value = 6
    min_value = -6

    num_of_datapoints = 100

    # Resolution = step size
    resolution = float((max_value - min_value) / num_of_datapoints)

    # convert degrees to radians
    ip_dataset = [min_value + (resolution * i) for i in range(0, num_of_datapoints)]
    op_dataset = [func(ip_dataset[i]) for i in range(0, num_of_datapoints)]

    # Split Dataset into training and testing sets. (70%training and 30%testing)
    input_train, input_test, output_train, output_test = train_test_split(ip_dataset, op_dataset, test_size=0.3)
    train_global_indices = [ip_dataset.index(i) for i in input_train]
    test_global_indices = [ip_dataset.index(i) for i in input_test]

    return [ip_dataset, op_dataset, input_train, input_test, output_train, output_test, train_global_indices,
            test_global_indices, resolution]


def sigmoid(x):
    sig = 1 / (1 + math.exp(-x))
    return sig


learning_rate = 0.2

data = generate_data(sigmoid)
ip_dataset = data[0]
op_dataset = data[1]
input_train = data[2]
input_test = data[3]
output_train = data[4]
output_test = data[5]
train_global_indices = data[6]
test_global_indices = data[7]
resolution = data[8]


def accuracy(real, pred):
    result = np.count_nonzero(abs(pred - real) < error_th)
    acc = result / len(real)
    return acc


def CMAC():
    for i in range(iteration):
        for train_idx in data[6]:
            act_mat, act_idx = activation_matrix(train_idx)
            act_sum = sum(np.multiply(act_mat, weights))

            error = op_dataset[train_idx] - act_sum

            correction = (learning_rate * error) / gen_fac

            weights[act_idx: (act_idx + gen_fac)] = [(weights[idx] + correction) for idx in
                                                     range(act_idx, (act_idx + gen_fac))]

    return weights


def CMAC_test():
    result = np.array([])

    for test_idx in test_global_indices:
        act_mat, act_idx = activation_matrix(test_idx)
        prediction_t = sum(np.multiply(act_mat, weights))

        result = np.append(result, prediction_t)

    accuracy_test_r = accuracy(output_test, result)

    return result, accuracy_test_r


if __name__ == "__main__":
    weights = CMAC()
    prediction, accuracy_test = CMAC_test()

    print(accuracy_test)

    plt.plot(input_train, output_train, '.', color='black')
    plt.plot(input_test, prediction, '.', color='red')

    plt.show()