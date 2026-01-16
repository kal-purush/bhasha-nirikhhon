import pandas as pd

import matplotlib.pylab as plt

blocks = pd.read_csv('export.txt', names = ['x', 'y', 'z', 'type'], skiprows=[0])
blocks = blocks[blocks['type'] != 'air']
height_map = blocks.groupby(['x', 'z'])['y'].max().reset_index().pivot('x', 'z', 'y')
plt.xticks([])
plt.yticks([])
plt.xlim([150, 250])
plt.ylim([50, 250])
plt.contour(height_map.values)
plt.axes().set_aspect('equal', 'datalim')
plt.savefig("outline.png")