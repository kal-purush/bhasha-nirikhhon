import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import tkinter as tk
from tkinter import simpledialog

def plot_graph(x_min, x_max, y_min, y_max):
    x = np.linspace(x_min, x_max, 100)
    y = np.linspace(y_min, y_max, 100)
    X, Y = np.meshgrid(x, y)
    Z = np.sin(np.sqrt(X**2 + Y**2))

    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.plot_surface(X, Y, Z, cmap='viridis')
    plt.show()

# Create the root window
root = tk.Tk()
root.withdraw()  # Hide the root window

# Ask the user for input
x_min = float(simpledialog.askstring("Input", "Enter x_min:", parent=root))
x_max = float(simpledialog.askstring("Input", "Enter x_max:", parent=root))
y_min = float(simpledialog.askstring("Input", "Enter y_min:", parent=root))
y_max = float(simpledialog.askstring("Input", "Enter y_max:", parent=root))
tk
# Plot the graph with the user input
plot_graph(x_min, x_max, y_min, y_max)