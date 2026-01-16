# Use the numpy library
import numpy as np

def prepare_inputs(inputs):
    # TODO: create a 2-dimensional ndarray from the given 1-dimensional list;
    #       assign it to input_array
    input_array = inputs[:, None]

    # TODO: find the minimum value in input_array and subtract that
    #       value from all the elements of input_array. Store the
    #       result in inputs_minus_min
    inputs_minus_min = input_array - np.min(input_array, axis=1)

    # TODO: find the maximum value in inputs_minus_min and divide
    #       all of the values in inputs_minus_min by the maximum value.
    #       Store the results in inputs_div_max.
    inputs_div_max = inputs_minus_min / np.max(inputs_minus_min, axis=1)

    # return the three arrays we've created
    return input_array, inputs_minus_min, inputs_div_max


def multiply_inputs(m1, m2):
    # TODO: Check the shapes of the matrices m1 and m2.
    #       m1 and m2 will be ndarray objects.
    #
    #       Return False if the shapes cannot be used for matrix
    #       multiplication. You may not use a transpose
    swap = False
    if m1.shapes[-1] != m2.shapes[0] and m1.shapes[0] != m2.shapes[-1]:
        return False
    if m1.shapes[-1] != m2.shapes[0]:
        swap = True

    # TODO: If you have not returned False, then calculate the matrix product
    #       of m1 and m2 and return it. Do not use a transpose,
    #       but you swap their order if necessary
    return swap ? m2.dot(m1) : m1.dot(m2)


def find_mean(values):
    # TODO: Return the average of the values in the given Python list
    return np.mean(np.array(values))


input_array, inputs_minus_min, inputs_div_max = prepare_inputs([-1,2,7])
print("Input as Array: {}".format(input_array))
print("Input minus min: {}".format(inputs_minus_min))
print("Input  Array: {}".format(inputs_div_max))

print("Multiply 1:\n{}".format(multiply_inputs(np.array([[1,2,3],[4,5,6]]), np.array([[1],[2],[3],[4]]))))
print("Multiply 2:\n{}".format(multiply_inputs(np.array([[1,2,3],[4,5,6]]), np.array([[1],[2],[3]]))))
print("Multiply 3:\n{}".format(multiply_inputs(np.array([[1,2,3],[4,5,6]]), np.array([[1,2]]))))

print("Mean == {}".format(find_mean([1,3,4])))