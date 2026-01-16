import maya.cmds as cmds
import datetime

def cancelRenderPush(*args):
    cancel = 1
    print "Canceling"

def renderButtonPush(*args):
    xTile = cmds.intSliderGrp ( "intFieldName1", query = True, value = True)
    yTile = cmds.intSliderGrp ( "intFieldName2", query = True, value = True)
    xOffset = cmds.intSliderGrp ( "intFieldName3", query = True, value = True)
    yOffset = cmds.intSliderGrp ( "intFieldName4", query = True, value = True)
    
    cancel = 0
     
    x = xOffset
    if (xTile - 1) < xOffset:
        x = xTile - 1
    y = yOffset
    if (yTile - 1) < yOffset:
        y = yTile - 1
    
    outWidth = float(cmds.getAttr("defaultResolution.width"))
    outHeight = float(cmds.getAttr("defaultResolution.height"))
    xTileLenght = outWidth/xTile
    yTileLenght = outHeight/yTile
    print x, y
    for x in range(x, xTile):
        if cancel == 1:
            print "Canceling X"
            break
        for y in range(y, yTile):
            if cancel == 1:
                print "Canceling Y"
                break
            print "Starting [", x, "] [", y, "] ", datetime.datetime.now()
            cmds.vray("vfbControl", "-setregion", (int(x*xTileLenght)), (int(y*yTileLenght)), (int((x+1)*xTileLenght)), (int((y+1)*yTileLenght)))
            cmds.vrend()
            y += 1
        x += 1
        y = 0


cmds.window(title = "Batch Tiled V-Ray render", width = 300, height = 20)
cmds.columnLayout("columnLayoutName01", adjustableColumn = True) 

cmds.intSliderGrp ( "intFieldName1", label = "X Tiling", field = True, fieldMinValue = 1, fieldMaxValue = 100, minValue = 1, maxValue = 10, columnWidth3 = [50, 50, 50], columnAlign3 = ["left", "both", "left"], value = 1, parent = "columnLayoutName01")
cmds.intSliderGrp ( "intFieldName2", label = "Y Tiling", field = True, fieldMinValue = 1, fieldMaxValue = 100, minValue = 1, maxValue = 10, columnWidth3 = [50, 50, 50], columnAlign3 = ["left", "both", "left"], value = 1, parent = "columnLayoutName01")

cmds.intSliderGrp ( "intFieldName3", label = "X Offset", field = True, fieldMinValue = 0, fieldMaxValue = 99, minValue = 0, maxValue = 9, columnWidth3 = [50, 50, 50], columnAlign3 = ["left", "both", "left"], value = 0, parent = "columnLayoutName01")
cmds.intSliderGrp ( "intFieldName4", label = "X Offset", field = True, fieldMinValue = 0, fieldMaxValue = 99, minValue = 0, maxValue = 9, columnWidth3 = [50, 50, 50], columnAlign3 = ["left", "both", "left"], value = 0, parent = "columnLayoutName01")

cmds.button("nameButton0", label = "Render", width = 50, height = 20, backgroundColor = [0.2, 0.2, 0.2], parent = "columnLayoutName01", command=renderButtonPush)
cmds.button("nameButton1", label = "Cancel", width = 50, height = 20, backgroundColor = [0.2, 0.2, 0.2], parent = "columnLayoutName01", command=cancelRenderPush)

cmds.showWindow()
