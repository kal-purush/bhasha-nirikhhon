
# coding: utf-8

# In[ ]:


import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas import DataFrame, Series  # for convenience
import pims
import trackpy as tp
import opencv as cv

# Imports the desired video
frames = pims.Video('01 - MRU_VAlta.mp4')


# In[ ]:


#frames = tp.format(3,2)
plt.imshow(frames[250])
plt.show()
image = frames[250]


grey = np.zeros((image.shape[0], image.shape[1]))
for rownum in range(len(image)):
    for colnum in range(len(image[rownum])):
        grey[rownum][colnum] = np.average(image[rownum][colnum])

print(grey)
#f = tp.locate(frames[120], 55)
#plt.figure()  # make a new figure
#tp.annotate(f, frames[120]);


# 