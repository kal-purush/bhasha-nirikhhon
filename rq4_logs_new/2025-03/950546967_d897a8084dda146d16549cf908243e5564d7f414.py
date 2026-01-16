# How the Algorithm Works
# Start Position: We begin at a specified starting point on the function landscape, where the goal is to find the lowest (minimum) point.
# Gradient Calculation: At each position, we calculate the gradient (a vector pointing in the direction of the steepest increase). Since we want to minimize the function, we move in the opposite direction of the gradient.
# Backtracking Line Search (Armijo Rule): Instead of using a fixed step size, the algorithm starts with a large step size and reduces it gradually (by multiplying with a factor) until it satisfies the Armijo condition. This condition ensures that the step size is chosen only if it decreases the function value significantly compared to the gradient's prediction. This step size adjustment helps make progress without overshooting or moving too cautiously.
# Update Position: We move to the new position based on the chosen step size. The difference between the old and new positions is checked, and if it's small enough (below a set tolerance), we assume the algorithm has converged and stop.

import numpy as np
import matplotlib.pyplot as plt

def rosenbrock(x, y, a=1, b=100):
    return (a - x)**2 + b * (y - x**2)**2

def rosenbrock_grad(x, y, a=1, b=100):
    dfdx = -2*(a - x) - 4*b*x*(y - x**2)
    dfdy = 2*b*(y - x**2)
    return np.array([dfdx, dfdy])

def armijo_line_search(f, grad_f, x, direction, alpha=1, beta=0.5, sigma=1e-4):
  
    fx = f(*x)
    grad_fx = grad_f(*x)
    
    while f(*(x + alpha * direction)) > fx + sigma * alpha * np.dot(grad_fx, direction):
        alpha *= beta  
    return alpha

def gradient_descent_armijo(f, grad_f, x0, max_iter=100, epsilon=1e-6):
    x = x0
    path = [x.copy()] 

    for k in range(max_iter):
        grad = grad_f(*x)
        if np.linalg.norm(grad) < epsilon:
            break
        
        direction = -grad
        
        alpha = armijo_line_search(f, grad_f, x, direction)
        
        x = x + alpha * direction
        path.append(x.copy()) 

    return np.array(path), x

x_range = np.linspace(-2.5, 2, 100)
y_range = np.linspace(1, 3, 100)
X, Y = np.meshgrid(x_range, y_range)
Z = rosenbrock(X, Y)

x0 = np.array([-2, 2])

gd_path, gd_min = gradient_descent_armijo(rosenbrock, rosenbrock_grad, x0, max_iter=1000)

plt.figure(figsize=(10, 8))
cp = plt.contour(X, Y, Z, levels=np.logspace(0, 3, 20), cmap='jet')
plt.colorbar(cp)

plt.plot(gd_path[:, 0], gd_path[:, 1], linestyle='-', color='r', marker='o', markersize=8, linewidth=1, label='GD with Armijo')

plt.scatter(gd_min[0], gd_min[1], color='r', zorder=1, s=100, label='GD Minimum', edgecolor='black')

plt.title('Gradient Descent with Armijo Line Search on Rosenbrock Function')
plt.xlabel('x')
plt.ylabel('y')
plt.legend()
plt.show()