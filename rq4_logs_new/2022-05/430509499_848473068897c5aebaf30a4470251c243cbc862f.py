import numpy as np

def signed_dist(x,theta,theta_0):
    """
    Computes signed distance from x to hyperplane given by theta,theta_0
    
    :param x: Input data point
    :param theta: parameter theta of linear classifier
    :param theta_0: parameter theta_0 of linear classifier
    :return: signed perpendicular distance of x to hyperplane

    """

    return (theta.T@x + theta_0)/ np.linalg.norm(theta)

if __name__ == "__main__":
    x = np.array([4,-0.5])
    theta = np.array([3,4])
    theta_0 = 5
    print(signed_dist(x,theta,theta_0))