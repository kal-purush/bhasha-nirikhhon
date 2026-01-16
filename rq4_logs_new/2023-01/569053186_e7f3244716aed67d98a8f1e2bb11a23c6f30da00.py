from PIL import Image;
import os;

path = input()

img = Image.open(path)
img.convert('RGB')
pixel_values = list(img.getdata())
string = ''
file = open('mapa.txt','w')
i = 1
for pixel in pixel_values:
    red = bin(pixel[0])
    green = bin(pixel[1])
    blue = bin(pixel[2])
    valor = ''
    valor += red
    valor += green[2:]
    valor += blue[2:]
    string += str(int(valor,2))
    
    string += ','

    if(i%80 == 0):
        string+= '\n'
    i+=1

file.write(string)
file.close()


    
    
    
    




   