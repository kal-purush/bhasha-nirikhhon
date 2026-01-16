from numpy import *


def compute_error_for_given_points(b, m, points):
    #sum of squared errors gives error value (measure distance from each points to line drawn, square them, then sum all together and divide by total number of points) minimised by gradient descent
    totalError = 0
    for i in range(0, len(points)):
        x = points[i, 0]
        y = points[i, 1]
        totalError += (y - (m * x + b)) **2
    return totalError / float(len(points))

def step_gradient(current_b, current_m, points, learningRate):
    #gradient descent, calculating gradient by calculating partial derivative in respect to values b and m
    b_gradient = 0
    m_gradient = 0
    N = float(len(points))
    #iterate through all the points
    for i in range(0, len(points)):
        x = points[i, 0]
        y = points[i, 1]
        b_gradient += -(2/N) * (y -((current_m * x) + current_b))
        m_gradient += -(2/N) * x * (y - ((current_m * x) + current_b))
        new_b = current_b - (learningRate * b_gradient)
        new_m = current_m - (learningRate * m_gradient)
    return [new_b, new_m]



def gradient_descent_runner(points, starting_b, starting_m, learning_rate, num_iterations):
    b = starting_b
    m = starting_m
    for i in range(num_iterations):
        b, m = step_gradient(b, m, array(points), learning_rate)
    return [b, m]

def run():
    points = genfromtxt("data.csv", delimiter = ",")
    learning_rate = 0.0001
    #slope forumla y = mx + b (b = y intercept / m = slope)
    initial_b = 0
    initial_m = 0
    #how many iterations to run training step for
    num_iterations = 1000
    print ("Starting gradient descent at b = {0}, m = {1}, error = {2}".format(initial_b, initial_m, compute_error_for_given_points(initial_b, initial_m, points)))
    #get ideal b and m values using gradient descent runner step by feeding values we have defined
    [b, m] = gradient_descent_runner(points, initial_b, initial_m, learning_rate, num_iterations)
    print ("After {0} iterations b = {1}, m = {2}, error = {3}".format(num_iterations, b, m, compute_error_for_given_points(b, m, points)))

if __name__ == '__main__':
    run()