import numpy as np
import pandas
import torch
from matplotlib import pyplot as plt

dataset_link = "https://gitlab.com/ntnu-tdat3025/regression/childgrowth-datasets/raw/master/day_head_circumference.csv"

dataset = pandas.read_csv(dataset_link,
                          usecols=['# day', 'head circumference'],
                          dtype={'# day': np.float32, 'head circumference': np.float32})
dataset.columns = ['day', 'head_circumference']

x_train = torch.tensor(dataset['day']).reshape(-1, 1)
y_train = torch.tensor(dataset['head_circumference']).reshape(-1, 1)


def sigmoid(x):
    res = 1.0 / (1.0 + np.e ** -x)
    return res


# This model uses Sigmoid function to create a non-linear regression model
class NonLinearRegressionModel:

    def __init__(self):
        self.W = torch.tensor([0.0], requires_grad=True)
        self.b = torch.tensor([0.0], requires_grad=True)

    def f(self, x):
        return 20.0 * sigmoid(x @ self.W + self.b) + 31.0

    def loss(self, x, y):
        return torch.nn.functional.mse_loss(self.f(x), y)


model = NonLinearRegressionModel()
optimizer = torch.optim.SGD([model.W, model.b], 0.00000035)

for epoch in range(25000):
    model.loss(x_train, y_train).backward()
    optimizer.step()

    optimizer.zero_grad()

print("W = %s, b = %s, loss = %s" % (model.W.data, model.b.data, model.loss(x_train, y_train).data))

# Visualize result
plt.plot(x_train, y_train, 'o', label='$(x^{(i)},y^{(i)})$')
plt.xlabel('x')
plt.ylabel('y')
x = torch.tensor(np.linspace(torch.min(x_train), torch.max(x_train), 150, dtype=np.float32)).reshape(-1, 1)

g = model.f(x).detach()
plt.plot(x, model.f(x).detach(), label='$\\hat y = f(x) = xW+b$')
plt.legend()
plt.show()