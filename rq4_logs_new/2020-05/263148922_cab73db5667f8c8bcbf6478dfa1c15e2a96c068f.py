

#listado de las imagenes en el presente directorio
#0
print("Starting")


import os
from PIL import Image
import imageio
import numpy as np



#Search tif images
#1
print(" Searching .tif/.tiff images in the current directory")
Search_tif = [x for x in os.listdir() if x.endswith(".tif") or x.endswith(".tiff") ]  
 

#2
print("  "+  str(len(Search_tif))+" .tif/.tiff images were found")


# Define las listas
uint8 = []
uint16 = []
float32= []

###########################################################################################


#agrega las imagenes tif a su lista correspondiente dependiendo si son de 8, 16 o 32 bits.
for Itif in Search_tif:
    
    #print(Itif)

    im = imageio.imread(Itif)
    #print(im.dtype)
    if im.dtype == "uint8":
        uint8.append(Itif)
    if im.dtype == "uint16":
        uint16.append(Itif)
    if im.dtype == "float32":
        float32.append(Itif)

#3
#########################################################################################
i =1
for Itif8 in uint8:
    im = imageio.imread(Itif8)
    img_uint8 = im.astype(np.uint8)
    imageio.imwrite(Itif8[0:-4]+".png",img_uint8)
    i=i+1
print ( "   "+str(len(uint8)) + " images .tif/.tiff 8  bits converted to  .png  8 bits" )

#############################################################################################
j =1
for Itif16 in uint16:
    im = imageio.imread(Itif16)
    im=im/256
    img_uint8 = im.astype(np.uint8)
    imageio.imwrite(Itif16[0:-4]+".png",img_uint8)
    j=j+1
print ( "   "+str(len(uint16))+" images .tif/.tiff 16 bits converted to  .png  8 bits")

##############################################################################################
k =1
for Itif32 in float32:
    im = imageio.imread(Itif32)
    im = im/16777216
    img_uint8 = im.astype(np.uint8)
    imageio.imwrite(Itif32[0:-4]+".png",img_uint8)
    k=k+1
print ( "   "+str(len(float32)) + " images .tif/.tiff 32 bits converted to  .png  8 bits")

#############################################################################################




#4
print ( "    "+str(len(Search_tif)) + " images .tif/.tiff were converted to .png" )
#5
print("     Searching images png & jpg in the current directory")
Search_images = [x for x in os.listdir() if x.endswith(".png") or x.endswith(".jpg")]# or x.endswith(".tif") ]  # search png, jpg  , to add a new format add" or x.endswith(".EXTENTION") " to the []

#6
print("      "+  str(len(Search_images))+" .png & jpg images were found")


#crea un archivo excel y adjunta las imagenes

from openpyxl import Workbook

from openpyxl.drawing.image import Image

wb = Workbook()
ws = wb.active
ws.title = "Images"
dest_filename = 'name and images.xlsx'

for R in range(1,1000):
    ws.row_dimensions[R].height  = 200

wcolA = 25
#7
print("       Writing excel")
i=0
for I in Search_images:
    i=i+1
    img = Image(I)
    img.width = 200
    img.height = 200
    Cell1 = ("A"+str(i))
    Cell2 = ("B"+str(i))
    ws[Cell1] = I
    
    ws.add_image(img, Cell2)





ws.column_dimensions["A"].width = wcolA
ws.column_dimensions["B"].width = wcolA
ws.column_dimensions["C"].width = wcolA
ws.column_dimensions["D"].width = wcolA
wb.save(filename = dest_filename)
#8
print("        File ready")