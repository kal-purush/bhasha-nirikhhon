
# coding: utf-8

# In[11]:


from PIL import Image
import random


# In[12]:


## add impluse noise if the source is pure
def addNoise(img,implusePercent=0.25):
    output = Image.new("L",(img.width,img.height))
    outputPixels = output.load()
    pixels = img.load()
    
    for j in range(img.height):
        for i in range(img.width):
            ran = random.random()
            if ran < implusePercent:
                outputPixels[i,j] = 0
            elif ran < 2*implusePercent:
                outputPixels[i,j] = 255
            else:
                outputPixels[i,j] = pixels[i,j]
            
    return output


# In[13]:


def getTags(img, x ,y,size = 3):
    pixels = []
    width = img.width
    height = img.height
    
    imgLoad = img.load()
    
    for j in range(size):
        pY = y - int(size/2) + j
        if  pY < 0 or pY >= height:
            continue
            
        for i in range(size):
            pX = x - int(size/2) + i
            if pX < 0 or pX >= width :
                continue
            #print(pX,pY,imgLoad[pX,pY])
            pixels.append(imgLoad[pX,pY])
    
    #print(pixels)
    pixels.sort()
    #print(pixels)
    
    Zmin = pixels[0]
    Zmax = pixels[-1]
    Zmed = pixels[int(len(pixels)/2)]
    Zxy = imgLoad[x,y]
    
    return Zmin,Zmed,Zmax,Zxy
            


# In[14]:


def adaptiveMedianFilter(img,x,y,size=3,maxSize = 7):
    Zmin,Zmed,Zmax,Zxy = getTags(img,x,y,size)
    #print( Zmin,Zmed,Zmax,Zxy,size)
    
    if Zmin < Zmed < Zmax:
        # level B
        if Zmin < Zxy < Zmax:
            return Zxy
        else:
            return Zmed
    else:
        size +=2
    
    if size <= maxSize:
        return adaptiveMedianFilter(img,x,y,size)
    else:
        ## med or xy?
        return Zmed
        #return Zxy


# In[15]:


def medianFilter(img,x,y,size=3,maxSize=7):
    Zmin,Zmed,Zmax,Zxy = getTags(img,x,y,size)
    return Zmed


# In[16]:


def filterNoise(img, method):
    output = Image.new("L",(img.width,img.height))
    pixels = output.load()
    
    for j in range(img.height):
        for i in range(img.width):
            pixels[i,j] = method(img,i,j)
    return output


# In[22]:


img = Image.open("source_color.jpg").convert("L")
img.save("source.jpg")
#img = Image.open("source.jpg")


# In[23]:


noisedImg = addNoise(img,0.25)
noisedImg.save("implused.jpg")


# In[24]:


filterImg = filterNoise(noisedImg,adaptiveMedianFilter)
filterImg.save("result-adaptive.jpg")


# In[25]:


badFilterImg = filterNoise(noisedImg,medianFilter)
badFilterImg.save("result-median.jpg")


# In[26]:


noisedImg.close()
filterImg.close()
badFilterImg.close()
img.close()
