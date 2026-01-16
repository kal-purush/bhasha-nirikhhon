from PIL import Image

def getSquare(im, threshold):
    return im.crop(getBox(im, threshold))
    
def getBox(im, threshold):
 width = im.width
 height = im.height
 pix = im.load()
	
 xmin = width
 xmax = 0
 ymin = height
 ymax = 0
    
 for w in range(0, width):
  for h in range(0, height):
   pixel = pix[w,h]
   if (pixel[0] < threshold or pixel[1] < threshold or pixel[2] < threshold):
    if (w > xmax): xmax = w
    if (w < xmin): xmin = w
    if (h > ymax): ymax = h
    if (h < ymin): ymin = h

 return getBoxDims(xmin, xmax, ymin, ymax)
 
def getBoxDims(xmin, xmax, ymin, ymax):
    dx = xmax - xmin
    dy = ymax - ymin
    
    if (dx > dy):
        difference = dx - dy
        half1 = difference / 2
        half2 = difference - half1
        ymin -= half1
        ymax += half2
    elif (dy > dx):
        difference = dy - dx
        half1 = difference / 2
        half2 = difference - half1
        xmin -= half1
        xmax += half2

    return xmin, ymin, xmax, ymax
    
def make1D(image_matrices):
    image_vectors = []
    for i in range(0, len(image_matrices)):
        vector = []
        for j in range(0, len(image_matrices[i])):
            for k in range(0, len(image_matrices[i][j])):
                vector.append(image_matrices[i][j][k])
        image_vectors.append(vector)
    
    return image_vectors