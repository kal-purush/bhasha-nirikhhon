#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 15:25:45 2019

@author:

script of shadows generator

"""
import os
import random
from PIL import Image,  ImageDraw
from torchvision import transforms
import time


import numpy as np
import matplotlib.pyplot as plt

import requests
from io import BytesIO
#from stl import mesh

from mystl import mesh

import math as mt



def RAND_BACKGR_UNSPLASH (size):
#    DIR = 'img'
#    img_path = os.path.join(DIR, random.choice(os.listdir(DIR)))
    
    
#    im = Image.open(img_path)
    url = 'https://source.unsplash.com/random'
    response = requests.get(url)
    im = Image.open(BytesIO(response.content))
#    im.save("img/unsplash/{}.png".format(time.time()), "PNG")
    
#    im.transpose(Image.ROTATE_90)
    recize_fac=int(size*1.25)#np.random.randint(300,450)
    transform = transforms.Compose([
            transforms.Grayscale(num_output_channels=1),
#            transforms.RandomRotation(360),
            transforms.Resize([recize_fac,recize_fac]),
            transforms.RandomCrop([size, size])
            ])
    im = transform(im)
    pix = np.array(im)
    return pix

def RAND_BACKGR (size):
    DIR = 'img/unsplash'
    img_path = os.path.join(DIR, random.choice(os.listdir(DIR)))
    
    
    im = Image.open(img_path)
#    url = 'https://source.unsplash.com/random'
#    response = requests.get(url)
#    im = Image.open(BytesIO(response.content))
#    im.save("/img/unsplash/{}.png".format(time.time()), "PNG")
    
#    im.transpose(Image.ROTATE_90)
    recize_fac=size*1.25#np.random.randint(300,450)
    transform = transforms.Compose([
            transforms.Grayscale(num_output_channels=1),
#            transforms.RandomRotation(360),
            transforms.Resize([recize_fac,recize_fac]),
            transforms.RandomCrop([size, size])
            ])
    im = transform(im)
    pix = np.array(im)
    return pix


def SHOW(tmp):
    #создание градиентного фона

    plt.figure(figsize=(5,5))
    plt.imshow(tmp, interpolation='nearest',cmap='Greys')#отрисовка для проверки
    #plt.title(name)
    #          plt.savefig("tmp/{}_{}.png".format(cap_class,scale))
#    plt.axis('off')
    plt.show()  

def copy(coord):
    return {'x':coord['x'],'y':coord['y'],'z':coord['z']}



def TRIANGLE(x1,y1,x2,y2,x3,y3,z1,z2,z3,img, zbufer,imgblack,N,p2,p1):
    

    xs=[x1,x2,x3]
    ys=[y1,y2,y3]
    zs=[z1,z2,z3]
    
#    if xs==ys or xs==zs or ys == zs:
#        return img,zbufer
    
    i=np.argmin(ys)
    x0=xs[i]
    y0=ys[i]
    z0=zs[0]
    a = {'x': xs[i],'y': ys[i],'z': zs[i]}
    del xs[i],ys[i],zs[i]
    i=np.argmin(ys)
    x1=xs[i]
    y1=ys[i]
    z1=zs[i]
    b = {'x': xs[i],'y': ys[i],'z': zs[i]}
    del xs[i],ys[i],zs[i]
    x2=xs[0]
    y2=ys[0]
    z2=zs[0]
    c = {'x': xs[0],'y': ys[0],'z': zs[0]}
    
    
    
    A = {'x': (b['x']-a['x']),
             'y': (b['y']-a['y']),
             'z': (b['z']-a['z'])}
    B = {'x': (c['x']-a['x']),
             'y': (c['y']-a['y']),
             'z': (c['z']-a['z'])}
        
    nx= A['y']*B['z']-A['z']*B['y']
    ny= A['z']*B['x']-A['x']*B['z']
    nz= A['x']*B['y']-A['y']*B['x']
    
    lenN2 =(nx**2+ny**2+nz**2)**0.5 or 1
    N2 = {'a': nx/lenN2 ,
             'b': ny/lenN2  ,
             'c': nz/lenN2 }
    
    scalar = (( N['x']*N2['a']+N['y']*N2['b']+N['z']*N2['c'])**2)**0.5
    scalar = 1-scalar
#    (scalar*zscal+z0)/ztop
    
    
    zscal=p2
    z0=p1
    ztop=100
#    print(scalar)
    
    a00=True
    a11=True
    a22=True
    
    x0=int(x0)
    x1=int(x1)
    x2=int(x2)
    x3=int(x3)
    y0=int(y0)
    y1=int(y1)
    y2=int(y2)
    y3=int(y3)
    
#    img[y1,x1]=1
#    img[y2,x2]=1
#    img[y3,x3]=1
    if (zbufer[y1,x1]<z1):
        zbufer[y1,x1]= z1
        img[y1,x1]=(scalar*zscal+z0)/ztop
        imgblack[y1,x1]=1
    if (zbufer[y2,x2]<z2):
        zbufer[y2,x2]= z2
        img[y2,x2]=(scalar*zscal+z0)/ztop
        imgblack[y2,x2]=1
    if (zbufer[y3,x3]<z3):
        zbufer[y3,x3]= z3
        img[y3,x3]=(scalar*zscal+z0)/ztop
        imgblack[y3,x3]=1
    
    if x1-x0!=0:
        a0=(y1-y0)/(x1-x0)
        az0=(z1-z0)/(x1-x0)
    else:
        a00=False
    b0=y0
    
    if x2-x1!=0:
        a1=(y2-y1)/(x2-x1)
        az1=(z2-z1)/(x2-x1)
    else:
        a11=False
    b1=y1
    
    if x2-x0!=0:
        a2=(y2-y0)/(x2-x0)
    else:
        a22=False
    b2=y0
    
    for j in range (y0,y1+1):

        if a00:
            if a0!=0:
                xb=int((j-b0)/a0)+x0
            else:
                xb=x0
        else:
            xb=x0

        if a22:
            if a2!=0:
                xe=int((j-b2)/a2)+x0
            else:
                xe = x2
        else:
            xe = x2
        if xb<xe:
            xb,xe=xe,xb
#        print ("y:{} xb:{} xe:{}".format(j,xb,xe))
        for i in range (xe,xb+1):
#            print (i)
#            img[j,i]=1
            
            if N2['c']>0:
                z= (N2['a']*(a['x']-i)+N2['b']*(a['y']-j))/N2['c']+a['z']
            else:
                z= a['z']
                
            if (zbufer[j,i]<z):
                zbufer[j,i]= z
                img[j,i]=(scalar*zscal+z0)/ztop
                imgblack[j,i]=1

            
    for j in range (y1,y2+1):
        if a11:
            if a1!=0:
                xb=int((j-b1)/a1)+x1
            else:
                xb=x1
        else:
            xb = x1
        if a22:
            if a2!=0:
                xe=int((j-b2)/a2)+x0
            else:
                xe=x2
        else:
            xe=x2
            
        if xb<xe:
            xb,xe=xe,xb
#        print ("y:{} xb:{} xe:{}".format(j,xb,xe))
        for i in range (xe,xb+1):            
            if N2['c']>0:
                z= (N2['a']*(a['x']-i)+N2['b']*(a['y']-j))/N2['c']+a['z']
            else:
                z= a['z']
                
            if (zbufer[j,i]<z):
                zbufer[j,i]= z
                img[j,i]=(scalar*zscal+z0)/ztop
                imgblack[j,i]=1
    return img,zbufer,imgblack


#def MAKE_SHADOW(vec):

def RandomN():
    phi = mt.radians(np.random.randint(360))
    theta = mt.radians(np.random.randint(360))
    x = mt.sin(theta)*mt.cos(phi)
    y = mt.sin(theta)*mt.sin(phi)
    z = mt.cos(theta)
#    x=0
#    y=0
#    z=1
    return {'x':x, 'y':y, 'z':z}

def MAKE_SHADOW(mymesh,sh_width):
    vec=mymesh.vectors
    
    
    pi=mt.pi
#    
    ax=np.random.randint(-20,20)/360*2*pi#pi/6
    ay=np.random.randint(0,720)/720*2*pi
    az=np.random.randint(0,30)/720*2*pi
 
    
#    ax=-pi/2#pi/2+np.random.randint(-30,30)/360*2*pi
#    ay=0#np.random.randint(360)/360*2*pi
#    az=0#np.random.randint(-30,30)/360*2*pi
    
    #
    Mx=[[1,0,0],
        [0,mt.cos(ax),-mt.sin(ax)],
        [0,mt.sin(ax),mt.cos(ax)]]
    
    My=[[mt.cos(ay),0,mt.sin(ay)],
        [0,1,0],
        [-mt.sin(ay),0,mt.cos(ay)]]
        
    Mz=[[mt.cos(az),-mt.sin(az),0],
        [mt.sin(az),mt.cos(az),0],
        [0,0,1]]
    
    pov=np.matmul(Mx,np.matmul(My,Mz))
    
    vec_new=[]
    for v in vec:
        a=[]
        for q in v:
            a.append(np.matmul(pov,q))
        vec_new.append(a)
        
    vec_new=np.array(vec_new)
    
#    print (vec_new)
    
    x=vec_new[:,:,0]
    y=vec_new[:,:,1]
    z = vec_new[:,:,2]

#    print ("x {}  y {}  z {}".format(x,y,z))
    xa=x.min()
    ya=y.min()
    za=z.min()
    

    
#    z = (z.max()-z+z.min())
#    print(z.min(),z.max())
    
    
    
    
    xlen = int(x.max())-int(x.min())+1
    ylen=int(y.max())-int(y.min())+1
    
    img_width=int(sh_width*200/225) #влияет на размер тени
    
    #tmp=np.zeros([max(ylen+1,img_width),max(xlen+1,img_width)])
    
    shadow_size=np.random.randint(img_width*0.65,img_width*0.97) #для масштабирующего коэф-та
    size=max(xlen,ylen)
    
    size_par=shadow_size/size
    
    tmp=np.zeros([max(int(ylen*size_par+1),img_width),max(int(xlen*size_par+1),img_width)])
    zbufer = np.zeros([max(int(ylen*size_par+1),img_width),max(int(xlen*size_par+1),img_width)])
    tmpblack = np.zeros([max(int(ylen*size_par+1),img_width),max(int(xlen*size_par+1),img_width)])
    
    normal = RandomN()
    p2=np.random.randint(50,100)
    p1=np.random.randint(10,p2)
    for v in vec_new:
        
    
        #тут еще надо вставить домножение
        x1=((v[0,0]-xa)*size_par)
        x2=((v[1,0]-xa)*size_par)
        x3=((v[2,0]-xa)*size_par)
        
        y1=((v[0,1]-ya)*size_par)
        y2=((v[1,1]-ya)*size_par)
        y3=((v[2,1]-ya)*size_par)
        
        z1=((v[0,2]-za)*size_par)
        z2=((v[1,2]-za)*size_par)
        z3=((v[2,2]-za)*size_par)
        tmp,zbufer,tmpblack=TRIANGLE(x1,y1,x2,y2,x3,y3,z1,z2,z3,tmp,zbufer,tmpblack,normal,p2,p1)
    

        
    #вырезаем прямоугольник силуэтом из картинки
#    imin=tmp.shape[0]
    imax=0
#    jmin=tmp.shape[1]
    jmax=0
    
    for i in range (tmp.shape[0]-1):
        for j in range (tmp.shape[1]-1):
            if tmp[j,i]>0:
#                if i<imin: imin=i
                if i>imax: imax=i
#                if j<jmin: jmin=j
                if j>jmax: jmax=j
#            xlen=imax-imin
#    ylen=jmax-jmin
    
    zbufer = zbufer/zbufer.max()
    tmp=tmp/tmp.max()
    return tmp[:jmax,:imax],tmpblack[:jmax,:imax]
#    return tmp[:int(ylen*size_par+1),:int(xlen*size_par+1)]

def Rand05():
    if np.random.randint(2)==1:
        return True
    else:
        return False
    
def Rand033():
    if np.random.randint(3)==1:
        return True
    else:
        return False

def MAKE_IMG (shadow,deepth,size):
    global tmpold
    xlen = np.shape(shadow)[0]
    ylen = np.shape(shadow)[1]
    
    x0=np.random.randint(0,size-xlen)
    y0=np.random.randint(0,size-ylen)
    
#    '''
    mean = np.random.randint(1000)/1000 # mean background
    std = np.random.randint(100)/1000
    num_samples = [size,size]
    tmp = np.random.normal(mean, std, size=num_samples)
    tmp=abs(tmp)
#    tmp=tmp/tmp.max()
    
    
#    if Rand05(): 
    mean2=np.random.randint(1000)/1000# mean and st for shadow area
    std2=np.random.randint(100)/1000
    tmp2 = np.random.normal(mean2, std2, size=num_samples)
    tmp2=abs(tmp2)

    
#    tmp=np.zeros([size,size])
#    '''
    
    
#    tmp2 = RAND_BACKGR() #RAND_BACKGR_UNSPLASH ()
    tmp = RAND_BACKGR_UNSPLASH (size)*tmp
    tmp=tmp/tmp.max()
    
    if Rand05(): 
        tmp2 = (RAND_BACKGR_UNSPLASH (size)+np.random.randint(400))*tmp2
        tmp2=tmp2/tmp2.max()
    
    sh = tmp2 [:,:]
    sh = (sh[x0:x0+xlen,y0:y0+ylen])*deepth # 
#    tmp = RAND_BACKGR_UNSPLASH ()
#    tmpold= tmp+0
    tmp[x0:x0+xlen,y0:y0+ylen]=tmp[x0:x0+xlen,y0:y0+ylen]*(1-shadow)+shadow # makin white area under the shadow
    
    tmp[x0:x0+xlen,y0:y0+ylen]=tmp[x0:x0+xlen,y0:y0+ylen]*(1-shadow)+sh*((tmp[x0:x0+xlen,y0:y0+ylen])) #the area around the shadow + shadow
#    -----------------------------------------------------------
    segm = np.ones(num_samples)
#    print(np.shape(segm))
    segm[x0:x0+xlen,y0:y0+ylen]=1-shadow

    bbox = np.ones(num_samples)
    bbox[x0:x0+xlen,y0:y0+ylen]*=0
#    tmp=1-tmp
    

    tmp = Image.fromarray(np.uint8(255*(1-tmp)), 'L')
    segm = Image.fromarray(np.uint8(255*(1-segm)), 'L')
    bbox = Image.fromarray(np.uint8(255*(1-bbox)), 'L')
    return tmp,segm,bbox

def ADD_TO_FILE(img_grey,cap_class,filename):
    grey_str=''
    grey_flat=np.int_(img_grey.flatten())
    for item in grey_flat:
        grey_str+=" {}".format(item)

    #запись в файл
    my_file = open(filename, 'a')
    #class,img_height,img_width,data
    print("{},{},{},{}".format(cap_class,img_grey.shape[0],img_grey.shape[1],grey_str),file=my_file)
    my_file.close()
    
    
def CONV2D(image, kernel, bias):
    m, n = kernel.shape
    if (m == n):
        y, x = image.shape
        y = y - m + 1
        x = x - m + 1
        new_image = np.zeros((y,x))
        for i in range(y):
            for j in range(x):
                new_image[i][j] = np.sum(image[i:i+m, j:j+m]*kernel) + bias
    return new_image

def LOAD_STL(filename):

    f = open(filename,'r')
    k=0
    b=0
    arr2=[]
    stl_load=[]
    triangle=[]
    while True:
        k+=1
        line = f.readline()# Нулевая длина обозначает конец файла (EOF)
    #    print(line)
        if len(line) == 0 :
           break
        if(line.find("vertex")>0 ):
            b+=1
            arr=line.split()
            x=np.float32(arr[-3])
            y=np.float32(arr[-2])
            z=np.float32(arr[-1])
            triangle.append([x,y,z])
            if (b==3):
                stl_load.append(triangle)
                triangle=[]
                b=0
    #        print (x,y,z)
    stl_load=np.array(stl_load)
    return stl_load

def SHOW_COMBO(IM1,IM2,IM3,imSize):
    new_im = Image.new('RGB', (imSize*3,imSize)) #creates a new empty image, RGB mode, and size 444 by 95
    new_im.paste(IM1, (0,0))
    new_im.paste(IM2, (imSize,0))
    new_im.paste(IM3, (imSize*2,0)) 
    factor=1.5
    new_im = new_im.resize((int(imSize*3/factor),int(imSize/factor)), Image.ANTIALIAS)
    display(new_im)

'''
----------------------------------------
'''





# stl-файлы с самолетами:
planes = [#путь к файлу, номер класса

        ['stl/nA319.stl',1],
        ['stl/nB787.stl',2],    
        ]

RANGE=1
imSize = 224

for num in range (RANGE):

    for each in planes:
        filename = each[0]
        pl_class = each[1]
        stl_mesh = mesh.Mesh.from_file(filename)
        model,shadow = MAKE_SHADOW(stl_mesh,imSize)
#        SHOW(shadow)
#        SHOW(model)
        

        img,segm, bbox = MAKE_IMG(shadow,model,imSize)
        SHOW_COMBO(img,segm,bbox,imSize)
                        