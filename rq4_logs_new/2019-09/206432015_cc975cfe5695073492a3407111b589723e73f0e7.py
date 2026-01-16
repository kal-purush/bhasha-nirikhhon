import random
import math

import preprocessor as pr
import classify as cl

# Proportion of data that belongs to test set when running algorithm without x-validation.
TEST_SET_PROPORTION = 0.2


def run_breast_cancer_data():
    data = pr.open_breast_cancer_data()
    training, test = shuffle_and_split(data)
    result = cl.classify_breast_cancer_data(training, test)
    # TODO: use loss function on results.


# Segments the glass data into test and training and attempts to classify the test set using the training set.
def run_glass_data():
    data = pr.open_glass_data()
    training, test = shuffle_and_split(data)
    result = cl.classify_glass_data(training, test)
    # TODO: use loss function on results.


# Segments the house data into test and training and attempts to classify the test set using the training set.
def run_house_data():
    data = pr.open_house_votes_data()
    training, test = shuffle_and_split(data)
    result = cl.classify_house_data(training, test)
    # TODO: use loss function on results.


# Segments the iris data into test and training and attempts to classify the test set using the training set.
def run_iris_data():
    data = pr.open_iris_data()
    training, test = shuffle_and_split(data)
    result = cl.classify_house_data(training, test)
    # TODO: use loss function on results.


# Segments the soybean data into test and training and attempts to classify the test set using the training set.
def run_soybean_data():
    data = pr.open_soybean_small()
    training, test = shuffle_and_split(data)
    result = cl.classify_soybean_data(training, test)


# (1) shuffles the data, and (2) segments it into a training and test set.
def shuffle_and_split(data):
    # Shuffle data so classes aren't grouped.
    random.shuffle(data.copy())

    # Segment data into training and test, using TEST_SET_PROPORTION as the proportion of observations that should
    # belong to the test set.
    n = len(data)
    split_point = math.floor(n * TEST_SET_PROPORTION)
    test = data[:split_point]
    training = data[split_point:]

    return training, test