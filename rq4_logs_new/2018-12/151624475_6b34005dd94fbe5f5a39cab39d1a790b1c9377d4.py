from PIL import Image
import io
import math



def compareColor(rgb1, rgb2):
    #out = abs((rgb1[0] - rgb2[0]) + abs(rgb1[1] - rgb2[1]) + abs(rgb1[2] - rgb2[2]))
    #out = math.sqrt((rgb1[0] - rgb2[0])**2 + (rgb1[1] - rgb2[1])**2 + (rgb1[2] - rgb2[2])**2)
    out = math.sqrt(((rgb1[0] - rgb2[0])*.3)**2 + ((rgb1[1] - rgb2[1])*.59)**2 + ((rgb1[2] - rgb2[2])*.11)**2)
    #out = abs((rgb1[0] - rgb2[0])*.3 + abs(rgb1[1] - rgb2[1])*.59 + abs(rgb1[2] - rgb2[2])*.11)
    return out




def compare(rgb, deflist, outlist):
    paint = deflist[0]
    dif = 1000000
    for p in deflist:
        if compareColor(rgb,p) < dif:
            dif = compareColor(rgb,p)
            paint = p
    #print("closest to")
    for p in outlist:
        if (p[0] == paint[0] and p[1] == paint[1] and p[2] == paint[2]):
            return False
    outlist.append(paint)
    #print(paint)

def closest(rgb, paints): #gets the closest paint in list to the rgb value
    paint = paints[0]
    dif = 10000
    for p in paints:
        if compareColor(rgb,p) < dif:
            dif = compareColor(rgb,p)
            paint = p
    return paint

def repaint(filename, paints):
    im = Image.open(filename)
    w, h = im.size
    for x in range(0, w-1):
        for y in range(0,h-1):
            newPix = closest(im.getpixel((x,y)),paints)
            im.putpixel((x,y), newPix)
    im.save("result.jpg")



#this one is for dealing with a reference to image by path
def main(filename, numpaints, database):
    deflist = [(250, 250, 250), (200, 200, 200), (150, 150, 150), (100, 100, 100), (50, 50, 50), (0, 0, 0)]
    outlist = []
    maxcolors = 15

    if numpaints >= 5 & numpaints <=35:
        maxcolors = numpaints
    if database!=None:
        deflist = database
    else:
        print("no list")
        deflist = [(250, 250, 250), (200, 200, 200), (150, 150, 150), (100, 100, 100), (50, 50, 50), (0, 0, 0)]
    if filename != None:
        im = Image.open(filename)
    else:
        im = Image.open(filename)
    list = im.getcolors(im.size[0] * im.size[1])
    list.sort(reverse=True)
    j = 0
    #print (len(database))
    while (j < maxcolors and j < len(list)):
        #print(list[j][0])
        #print(list[j][1])
        #compare(list[j][1],deflist,outlist)
        if (compare(list[j][1], deflist, outlist) == False):
            maxcolors += 1
        j+= 1
        #print(j)

#    hold = outlist.copy()
#    hold = outlist[:]
 #   outlist.clear()
    #print(len(outlist))
    return outlist

#this one is for dealing with the image itself
def mainproper(file, numpaints, database):
    deflist = [(250, 250, 250), (200, 200, 200), (150, 150, 150), (100, 100, 100), (50, 50, 50), (0, 0, 0)]
    outlist = []
    maxcolors = 15

    if numpaints >= 5 & numpaints <=35:
        maxcolors = numpaints
    if database!=None:
        deflist = database
    if file != None:
        im = Image.open(io.BytesIO(file))
    else:
        return
    list = im.getcolors(im.size[0] * im.size[1])
    list.sort(reverse=True)
    j = 0
    print("here")

    while (j < maxcolors and j < len(list)):
        # print(list[j][0])
        # print(list[j][1])
        # compare(list[j][1],deflist,outlist)
        if (compare(list[j][1], deflist, outlist) == False):
            maxcolors += 1
        j += 1
        # print(j)

    #hold = outlist.copy()
    #outlist.clear()
    #print(len(hold))
    return outlist

#main("test.jpg",15, None)