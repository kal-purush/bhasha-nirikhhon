from matplotlib import pyplot as plt
import neural_network as nn
from mpl_toolkits.mplot3d import Axes3D

def draw(y):
    fig = plt.figure()
    x = list()
    for i in range(len(y)):
        x.append(i)
    plt.plot(x,y)
    plt.show()
    plt.savefig("dnn/out/cost.png")

def tdchart(nn):
    fig = plt.figure()
    ax = Axes3D(fig)
    ax.set_xlabel('x0')
    ax.set_ylabel('x1')
    ax.set_zlabel('y')
    x = []
    y = []
    z = []
    dence = 30
    for i in range(dence):
        for j in range(dence):
            x.append(i/dence)
            y.append(j / dence)
            nn.forwordpropagation([i/dence,j/dence])
            z.append(nn.z[-1])
    ax.scatter3D(x, y, z)
    plt.show()
    plt.savefig("dnn/out/3d.png")   