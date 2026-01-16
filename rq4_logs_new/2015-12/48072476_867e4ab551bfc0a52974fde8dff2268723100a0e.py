import matplotlib.pyplot as plt
import os.path
import numpy

directory = os.path.dirname(os.path.abspath(__file__));
filename = os.path.join(directory, 'woman.jpg')
img = plt.imread(filename)

fig, ax = plt.subplots(1, 2)
ax[0].imshow(img, interpolation='none')
ax[1].imshow(img)
ax[0].set_xlim(135, 165)
ax[0].set_ylim(470, 420)
ax[1].set_xlim(135, 165)
ax[1].set_ylim(470, 420)
#Pltw had 'fig.show()' but the window disapeared right away. 
#Stack Overflow said to change it to 'plt.show()'
plt.show()
