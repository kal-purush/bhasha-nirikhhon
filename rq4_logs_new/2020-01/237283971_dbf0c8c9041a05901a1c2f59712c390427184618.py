import numpy as np
from PIL import Image
import requests
import io

image1 = "images/cows_fullscreen.PNG"
image2 = "images/cow1.PNG"

img1 = np.asarray(Image.open(image1))
img2 = np.asarray(Image.open(image2))

# img2 is greyscale; make it 2D by taking mean of channel values.
img2 = np.mean(img2, axis=-1)

from scipy import signal

corr = signal.correlate2d(img1, img2, mode='same')

y, x = np.unravel_index(np.argmax(corr), corr.shape)

import matplotlib.pyplot as plt

x2, y2 = np.array(img2.shape) // 2

fig, (ax_img1, ax_img2, ax_corr) = plt.subplots(1, 3, figsize=(15, 5))
im = ax_img1.imshow(img1, cmap='gray')
ax_img1.set_title('img1')
ax_img2.imshow(img2, cmap='gray')
ax_img2.set_title('img2')
im = ax_corr.imshow(corr, cmap='viridis')
ax_corr.set_title('Cross-correlation')
ax_img1.plot(x, y, 'ro')
ax_img2.plot(x2, y2, 'go')
ax_corr.plot(x, y, 'ro')
fig.save('fig.png')