

#listado de las imagenes en el presente directorio
print("Starting")

import os

print(" Searching images in the current directory")
Search_images = [x for x in os.listdir() if x.endswith(".png") or x.endswith(".jpg")]# search png and jpg, to add a new format add or x.endswith(".EXTENTION")

print("  "+  str(len(Search_images))+" images were found")

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

print("   Writing excel")
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
print("    File ready")