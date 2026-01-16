import pandas as pd
import numpy as np
from pyntcloud import PyntCloud
from PIL import Image

colourImg    = Image.open("plot.png")
colourPixels = colourImg.convert("RGB")

colourArray  = np.array(colourPixels.getdata()).reshape((colourImg.height, colourImg.width) + (3,))
indicesArray = np.moveaxis(np.indices((colourImg.height, colourImg.width)), 0, 2)
imageArray   = np.dstack((indicesArray, colourArray)).reshape((-1,5))
df = pd.DataFrame(imageArray, columns=["x", "y", "red","green","blue"])

depthImg = Image.open('plot.png').convert('L')
depthArray = np.array(depthImg.getdata())
df.insert(loc=2, column='z', value=depthArray)

df[['x','y','z']] = df[['x','y','z']].astype(float)
df[['red','green','blue']] = df[['red','green','blue']].astype(np.uint)
cloud = PyntCloud(df)

cloud.to_file("hand.ply", also_save=["mesh","points"],as_text=True)