
import os, sys, math
from lxml import etree   # lxml explores the OpenCV's haarcascade .xml file
from PIL import Image    # PIL.Image loads converts and shows the img to analyze
from PIL import ImageDraw # PIL.ImageDraw offers primitive to draw lines on img

def integrateImage(imageWidth,imageHeight) :
    pix2[0][0] = pow(pix2[0][0],2)
    for x in range(imageWidth) :
        for y in range(imageHeight) :
            if x == 0 and y != 0 :
                pix[x][y] += pix[x][y-1]
                pix2[x][y] = pix2[x][y-1] + pow(pix2[x][y],2)
            elif y == 0  and x != 0 :
                pix[x][y] += pix[x-1][y]
                pix2[x][y] = pix2[x-1][y] + pow(pix2[x][y],2)
            elif x != 0 and y != 0 :
                pix[x][y] += pix[x-1][y] + pix[x][y-1] - pix[x-1][y-1]
                pix2[x][y]=pix2[x-1][y]+pix2[x][y-1]-pix2[x-1][y-1]+pow(pix2[x][y],2)

def parseXml() :
    stages = cascade.find("stages")
    listHaarCascade = []
    for stage in stages :
        stageList = []

        trees = stage.find("trees")
        for tree in trees :
            treeArray = []
            for idx in range(2) :
                nodeList = []
                node = tree[idx+1]
                feature = node.find("feature")
                rects = feature.find("rects")

                rectsList = ()
                for rect in rects :
                    rectTextSplit = rect.text.split()
                    rectsList += (rectTextSplit,)
                nodeList.append(rectsList)
                nodeThreshold = float(node.find("threshold").text)
                nodeList.append(nodeThreshold)
                leftValue = node.find("left_val")
                nodeList.append(leftValue)
                rightValue = node.find("right_val")
                nodeList.append(rightValue)
                leftNode = node.find("left_node")
                nodeList.append(leftNode)
                rightNode = node.find("right_node")
                nodeList.append(rightNode)

                treeArray.append(nodeList)

            stageList.append(treeArray)

        stageThreshold = float(stage.find("stage_threshold").text)
        stageList.append(stageThreshold)
        listHaarCascade.append(stageList)

    return listHaarCascade

def evalFeature(windowX,windowY,windowWidth,windowHeight,rects,scale) :
    invArea=1.0/(windowWidth*windowHeight)
    featureSum = 0
    totalX=pix[windowX+windowWidth][windowY+windowHeight] \
          +pix[windowX][windowY] \
          -pix[windowX+windowWidth][windowY] \
          -pix[windowX][windowY+windowHeight]
    totalX2=pix2[windowX+windowWidth][windowY+windowHeight] \
           +pix2[windowX][windowY] \
           -pix2[windowX+windowWidth][windowY] \
           -pix2[windowX][windowY+windowHeight]
    vnorm=totalX2*invArea-pow(totalX*invArea,2)
    if vnorm > 1: vnorm = math.sqrt(vnorm)
    else        : vnorm = 1
    for rect in rects:
        x = int(scale*int(rect[0]))
        y = int(scale*int(rect[1]))
        width = int(scale*int(rect[2]))
        height = int(scale*int(rect[3]))
        weight = float(rect[4])
        featureSum += weight * \
                      (pix[windowX+x+width][windowY+y+height] \
                     + pix[windowX+x][windowY+y] \
                     - pix[windowX+x+width][windowY+y] \
                     - pix[windowX+x][windowY+y+height])
    return featureSum,invArea,vnorm

def evalStages(windowX,windowY,windowWidth,windowHeight,listHaarCascade,scale) :
    stagePass = True
    for stage in listHaarCascade:
        stageThreshold = stage[-1]
        stageSum = 0

        for tree in stage[:-1]:
            treeValue = 0
            idx = 0

            while True:
                node = tree[idx]
                rects = node[0]
                nodeThreshold = node[1]
                leftValue = node[2]
                rightValue = node[3]
                leftNode = node[4]
                rightNode = node[5]

                featureSum,invArea,vnorm = evalFeature(windowX,windowY,
                              windowWidth, windowHeight,rects,scale)

                if featureSum*invArea < nodeThreshold*vnorm:
                    if leftNode is None:
                        treeValue = float(leftValue.text)
                        break
                    else:
                        idx = int(leftNode.text)
                else:
                    if rightNode is None:
                        treeValue = float(rightValue.text)
                        break
                    else:
                        idx = int(rightNode.text)

            stageSum += treeValue

        stagePass = stageSum >= stageThreshold
        if not stagePass :
            return stagePass

    return stagePass

def detect(imageWidth,imageHeight) :
    listResult = []
    scale, scaleFactor = 10, 12.5
    windowWidth, windowHeight = (int(n) for n in cascade.find("size").text.split())
    while windowWidth < imageWidth and windowHeight < imageHeight:
        windowWidth = windowHeight = int(scale*20)
        step = int(scale*2.4)
        windowX = 0
        while windowX < imageHeight-scale*24:
            windowY = 0
            while windowY < imageWidth-scale*24:
                if evalStages(windowX,windowY,windowWidth,windowHeight,listHaarCascade,scale):
                    # All stages are validated, it means we detected something !
                    listResult.append((windowX, windowY, windowWidth, windowHeight))

                windowY += step
            windowX += step
        scale = scale * scaleFactor
    return listResult


def simplifyRects(listResult):
    if len(listResult)>0:
        centerList = []
        maxX,maxY,maxWidth,maxHeight=listResult[0]

        centerList.append((maxX+maxWidth/2,maxY+maxHeight/2))
        simplifiedList.append((listResult[0]))
        for rect in listResult :
            x,y,width,height = rect
            for center in centerList :
                centerX, centerY = center
                if x < centerX < x+width and y < centerY < y + height :
                    break

                elif x+width<maxX or maxX+maxWidth<x or y+height<maxY or maxY+maxHeight<y:
                    maxX=x
                    maxY=y
                    maxWidth=width
                    maxHeight=height
                    simplifiedList.append((rect))
                    centerList.append((maxX+maxWidth/2,maxY+maxHeight/2))
        return simplifiedList
    else:
        return []


def drawRect(simplifiedList) :
    lineWidth = 1
    for rect in simplifiedList :
        windowX,windowY,windowWidth,windowHeight = rect
        draw = ImageDraw.Draw(im)

        draw.line((windowX, windowY, windowX, windowY+windowHeight),
                  (255,0,0,255),lineWidth)
        draw.line((windowX, windowY+lineWidth/2, windowX+windowWidth, windowY+lineWidth/2),
                  (255,0,0,255),lineWidth) # Adds half of width
        draw.line((windowX+windowWidth,windowY,windowX+windowWidth,windowY+windowHeight),
                  (255,0,0,255),lineWidth)
        draw.line((windowX, windowY+windowHeight-lineWidth/2, windowX+windowWidth, 
                   windowY+windowHeight-lineWidth/2),(255,0,0,255),lineWidth)
        del draw
    im.show()

#### Main program ####
if len(sys.argv) != 2:
    print("Usage :", sys.argv[0], "FileName")
    sys.exit(1)

im = Image.open(sys.argv[1])
imageWidth, imageHeight = im.size
pixels = im.load()
pix  = [[int((10*pixels[y,x][0]+10*pixels[y,x][1]+50*pixels[y,x][2])/10)
        for x in range(imageWidth)] for y in range(imageHeight)]
pix2 = [[pix[y][x] for x in range(imageWidth)] for y in range(imageHeight)]
integrateImage(imageWidth,imageHeight)


cascade = etree.parse("haarcascade_frontalface_alt2.xml").getroot() \
                .find("haarcascade_frontalface_alt2")
listHaarCascade = parseXml()

simplifiedList=[]
listResult = detect(imageWidth,imageHeight)
chosenList = simplifyRects(listResult)
drawRect(chosenList)