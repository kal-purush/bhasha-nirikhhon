'''
Task 10

Implement gradient descent in python on the data:
x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
y = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

Plot the fit after every few iterations.

References:
matplotlib

'''
import numpy as np
import matplotlib.pyplot as plt

X = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
Y = np.array([2, 4, 6, 8, 10, 12, 14, 16, 18, 20])

# y = ax + b
# Function to optimize
f = lambda a,b,x : a*x + b

# Loss Function - Using SSE 
f_loss = lambda y,y_hat : (y**2 - y_hat**2)**(1/2)


# Randomly initialize a and b
a = 1
b = 1

learning_rate = 0.001

epochs = 1000

def plot_fit(x,y,a,b,iter):
    plt.scatter(x,y)
    plt.plot(x, f(a,b,x), color='r')
    plt.legend([f'y = {a}x + {b}'])
    plt.title(f'Epoch: {iter}')
    plt.show()



def gradient_descent(X,Y,a,b,learning_rate,epochs,f,f_loss):
    plot_fit(X,Y,a,b,0)
    # Repeat for number of epochs
    for epoch in range(epochs):
        # Iterate through the data
        for i,(x,y) in enumerate(zip(X,Y)):
            # Compute the prediction with current weights
            y_hat = f(a,b,x)
            # Computing the loss
            loss = f_loss(y,y_hat)
            # Updating the weights
            a -= learning_rate * (y_hat - y) * x
            b -= learning_rate * (y_hat - y)
            print(f"Epoch: {epoch}, Iteration: {i}, Loss: {loss}, a: {a}, b: {b}")
        if epoch % 100 == 0:
            plot_fit(X,Y,a,b,epoch)
    plot_fit(X,Y,a,b,epoch)

gradient_descent(X,Y,a,b,learning_rate,epochs,f,f_loss)




