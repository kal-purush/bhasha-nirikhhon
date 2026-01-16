#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-

###########################
# SmartMirror GUI v1.0.1b #
# Basic version           #
# by Schn33W0lf           #
###########################

from tkinter import *
from urllib.request import urlopen
import os
from base64 import encodestring
from time import sleep, strftime
from base64 import encodebytes
from io import BytesIO
from random import randint
from requests import head, get
import RPi.GPIO as IO
import PIL
from PIL import Image
from PIL import ImageTk
#SETTINGS#####################
##            [mode('BCM'/'BOARD'), B1, B2, B3, RED,    GRN,    w1-bus-pin (22),    jumper direction(GND:IO.FALLING/VCC:IO.RISING)]
gpio        = ['BCM',               2,  3,  4,  17,     27,     True,              IO.RISING]
##            [zoomed,  fullscreen, windowX,    windowY]
windowSizes = [False,   True,       0.75,       0.75]
##            [cartoonX,    cartoonY]
itemSizes   = [425*0.9,     596*0.9]
##            [version, weatherId,                                                                      tempSensorCpu,                              tempSensorIn,                       tempSensorOut,                      infoId]
Ids         = [0.4,     'http://www.daswetter.com/wimages/foto99e83cda40fd2d3cd0a4d11485dffca2.png.png',    '/sys/class/thermal/thermal_zone0/temp',    '/sys/bus/w1/devices/28-0417a312a0ff/w1_slave',   '/sys/bus/w1/devices/28-0517a27018ff/w1_slave',   'https://raw.githubusercontent.com/Schn33W0lf/RasPiSmartMirrorOS/master/res/test/headlines.txt']
##            [errors,  warnings,   infos,  debugInfos, logFeedback]
feedback    = [True,    True,       True,   True,      True]
texts       = ['Loading . . .', 'Here could be your advertisement! :P']
##            [allowshutdown]
fixes       = [True]
## other settings
logfile = '/opt/SM_GUI_py3.5-tk/logs/SM_'+strftime('%Y-%m-%dT%H-%M-%S%z')+'.log'
gpioActive = False
#FUNCTIONS####################
def errorMsg(error=0, force=False, msgType=0, function='functions > errorMsg', value='Unknown Error'):
    time = strftime('%Y-%m-%dT%H-%M-%S%z')  # could use getTime(2) but this function needs to be independent!
    returnMsg = False
    if error == 0:
        if feedback[0] == True or force == True:
            msg = time+' Error at       '+function+': '+value
            returnMsg = True
    elif error == 1:
        if feedback[1] == True or force == True:
            msg = time+' Warning at     '+function+': '+value
            returnMsg = True
    elif error == 2:
        if feedback[2] == True or force == True:
            msg = time+' Information at '+function+': '+value
            returnMsg = True
    elif error == 3:
        if feedback[3] == True or force == True:
            msg = time+' Debug-Infos at '+function+': '+value
            returnMsg = True
    else:
        msg = time+' Error at       functions > errorMsg: Invalid Error Code'
        returnMsg = True
    if msgType == 0:
        if returnMsg == True:
            return msg
    elif msgType == 1:
        if returnMsg == True:
            print(msg)
    else:
        print(time+' Error at       functions > errorMsg: Invalid Message Type')
    if feedback[4] == True:
        if returnMsg == True:
            open(logfile, 'a').write(msg+'\n')
def getTime(mode, value=None):
    errorMsg(3, False, 1, 'functions > getTime', 'Getting time...')
    if mode == 0:
        return strftime('%d.%m.%YT%H:%M:%S%z')
    elif mode == 1:
        return strftime('%H:%M:%S%n%A, %d.%m.%Y')
    elif mode == 2:
        return strftime('%Y.%m.%dT%H:%M:%S%z')
    elif mode == 3:
        return strftime('%Y-%m-%dT%H-%M-%S%z')
    elif mode == 4:
        return [strftime('%d-%m-%Y'),strftime('%H:%M:%S'), strftime('%z'), strftime('%Z')]
    elif mode == 5:
        if list(strftime('%S'))[1] in ['0', '5']:
            return True
        else:
            return False
    elif mode == 6:
        if strftime('%S') == '00':
            return True
        else:
            return False
    elif mode == 7:
        if strftime('%M:%S') in ['00:00', '00:30']:
            return True
        else:
            return False
    elif mode == 8:
        if strftime('%M:%S') == '00:00':
            return True
        else:
            return False
    elif mode == 9:
        if strftime('%H:%M:%S') == '00:00:00':
            return True
        else:
            return False
    elif mode == 10 and value != None and value != '':
        if getTime(2, None) == value:
            return True
        else:
            return False
    else:
        errorMsg(0, True, 1, 'getTime', 'Invalid mode')
def searchImg(url1, url2, rangeMin, rangeMax, debugInfos=False, fillZero=True):
    errorMsg(2, False, 1, 'functions > searcheImg', 'Searching Image...')
    statusCode = [0, 1]
    while (statusCode[0] not in [200, 201, 203, 204, 205, 206, 207, 208, 226]):
        url = randint(rangeMin, rangeMax)
        if url > 9:
            if url > 99:
                if url > 999:
                    url = ''+str(url)
                else:
                    url = '0'+str(url)
            else:
                url = '00'+str(url)
        else:
            url = '000'+str(url)
        url = url1+url+url2
        resp = head(url)
        statusCode[0] = resp.status_code
        errorMsg(3, False, 1, 'functions > searchImg', ('Code: '+str(statusCode[0])+' | URL: '+url+' | Try: '+str(statusCode[1])))
        statusCode[1] += 1
    errorMsg(2, False, 1, 'functions > searchImg', ('Current Picture: '+url+', Try'+str(statusCode[1])))
    return url
def createImg(url, resizeX=None, resizeY=None):
    errorMsg(2, False, 1, 'functions > createImg', 'Creating Image...')
    if type(resizeX) in [int, float] and type(resizeY) in [int, float]:
        return PIL.ImageTk.PhotoImage(PIL.Image.open(BytesIO(urlopen(url).read())).resize([int(round(resizeX)), int(round(resizeY))]))
    else:
         return PIL.ImageTk.PhotoImage(PIL.Image.open(BytesIO(urlopen(url).read())))
def loadGpio(gpioMode=None, setwarnings=False, pins=[[]], exitProgramm=False):
    errorMsg(2, False, 1, 'functions > loadGpio', 'Loading/ Unloading GPIOs')
    if gpioMode == 'BCM':
        IO.setmode(IO.BCM)
    elif gpioMode == 'BOARD':
        IO.setmode(IO.BOARD)
    if gpioMode == 'BCM' or gpioMode == 'BOARD':
        IO.setwarnings(setwarnings)
        for i in range(len(pins)):
            pin = pins[i]
            if pin[1] == 'IN':
                IO.setup(pin[0],IO.IN)
            elif pin[1] == 'OUT':
                IO.setup(pin[0],IO.OUT)
    elif gpioMode == 'CLEAN':
        IO.setwarnings(setwarnings)
        for i in range(len(pins)):
            IO.cleanup(pins[i])
##        Not Working...
##        if exitProgramm == True:
##            raise SystemExit
    elif gpioMode == None or gpioMode == '':
         errorMsg(0, True, 1, 'functions > gpioMode', 'Mode unset. use \'BCM\' or \'BOARD\' or \'UNLOAD\'! setting mode \''+gpioMode+'\' because it was the default value.')
    else:
        errorMsg(0, True, 1, 'functions > gpioMode', 'Invalid Mode. use \'BCM\' or \'BOARD\' or \'UNLOAD\'! setting mode \''+gpioMode+'\' because it was the default value.')
def statusLed(status=0, red=gpio[4], green=gpio[5]):
    if IO.getmode() in [IO.BCM, IO.BOARD]:
        if status in [0, 'off', 'OFF']:
            IO.output([red, green],IO.LOW)
            errorMsg(2, False, 1, 'functions > statusLed', 'status: 0 / off   / OFF')
        elif status in [1, 'green', 'GRN']:
            IO.output(red,IO.LOW)
            IO.output(green, IO.HIGH)
            errorMsg(2, False, 1, 'functions > statusLed', 'status: 1 / green / GRN')
        elif status in [2, 'red', 'RED']:
            IO.output(green,IO.LOW)
            IO.output(red, IO.HIGH)
            errorMsg(2, False, 1, 'functions > statusLed', 'status: 2 / red   / RED')
        elif status in [3, 'test', 'TST']:
            errorMsg(2, False, 1, 'functions > statusLed', 'status: 3 / test  / TST | Starting Test')
            i = 0
            for i in range(3):
                statusLed(i)
                i += 1
                sleep(1)
            i = 0
            statusLed(0)
            errorMsg(2, False, 1, 'functions > statusLed', 'status: 0 / off   / OFF | Test finished')
def gpioQuery(callback):
    global gpioActive
    print(gpioActive)
    if gpioActive == True:
        gpioActive = False
        if callback == gpio[1]:
            errorMsg(2, False, 1, 'functions > gpioQuery', 'Reloading cartoon. . .')
            statusLed(2)
            cartoon_url = searchImg('http://ruthe.de/cartoons/strip_', '.jpg', 0, 9999, True)
            cartoon_raw = createImg(cartoon_url, int(itemSizes[0]), int(itemSizes[1]))
            cartoon.config(image=cartoon_raw)
            cartoon.image = cartoon_raw
            cartoon_label.config(text=cartoon_url.split('_')[1].split('.')[0])
            statusLed(1)
        elif callback == gpio[2]:
            if fixes[0] == True:
                errorMsg(1, False, 1, 'functions > gpioQuery', 'Shutting down. . .')
                statusLed(2)
                sleep(0.1)
                statusLed(1)
                sleep(0.1)
                statusLed(2)
                sleep(0.25)
                statusLed(1)
                sleep(1)
                statusLed(2)
                #loadGpio('CLEAN', False, [2, 3, 4, 17, 27])
                os.system('shutdown 0')
            elif fixes[0] == False:
                statusLed(2)
                sleep(0.5)
                errorMsg(2, False, 1, 'functions > gpioQuery', 'Tried to shut down but shutdown was disabled.')
                statusLed(1)
            else:
                statusLed(2)
                sleep(0.5)
                errorMsg(0, False, 1, 'functions > gpioQuery', 'Invalid fix. \''+str(fixes[0])+'must be type bool, not type '+type(fixes[0])+'\'! Interpreted as True. Waiting 3 seconds till shutdown...')
                statusLed(1)
                sleep(3)
                fixes[0] = True
                gpioQuery(gpio[2])
        elif callback == gpio[3]:
            errorMsg(2, False, 1, 'functions > gpioQuery', 'Reloading Infos. . .')
            statusLed(2)
            raw = get(Ids[5])
            code = raw.status_code
            string = raw.text.split(';\n')
            info.config(text=string[randint(0, len(string)-1)])
            errorMsg(2, False, 1, 'functions > gpioQuery', 'Loaded Infos (Code '+str(code)+')')
            statusLed(1)
        else:
            errorMsg(0, False, 1, 'functions > gpioQuery', 'Unregistered Callback: '+str(callback))
        sleep(1)
        gpioActive = True
def tick():
    try:
        timeOld
    except NameError:
        timeOld = None
    timeNew = getTime(1)
    if timeOld != timeNew:
        clock.config(text=timeNew)
        timeOld = timeNew
    clock.after(200, tick)
def refreshT():
    errorMsg(3, False, 1, 'functions > tick/refreshT', 'Refreshing Temperatures')
    tempCpu.config(text=(round(float(open(Ids[2]).read())/1000, 1),'°C'))
    if gpio[6] == True:
        tempIns.config(text=(round(float(open(Ids[3]).read().split('t=')[1])/1000, 1),'°C'))
        tempOut.config(text=(round(float(open(Ids[4]).read().split('t=')[1])/1000, 1),'°C'))
    root.after(5000, refreshT)
def refreshW(refresh=True):
    if refresh == True:
        statusLed(2)
        errorMsg(2, False, 1, 'functions > tick/refreshW', 'Refreshing Weather')
        weather_raw = createImg(Ids[1])
        weather.config(image=weather_raw)
        weather.image = weather_raw
        statusLed(1)
    root.after(1800000, refreshW)
def toggleFullscreen(event):
    windowSizes[1] = not windowSizes[1]
    root.attributes('-fullscreen', windowSizes[1])
    return 'break'
def exitGUI(event):
    windowSizes[1] = False
    root.attributes('-fullscreen', False)
    root.destroy()
    raise SystemExit
#MAIN#########################
loadGpio(gpio[0], False, [[gpio[1], 'IN'], [gpio[2], 'IN'], [gpio[3], 'IN'], [gpio[4], 'OUT'], [gpio[5], 'OUT']])
statusLed(2)
root = Tk()
root.x = [root.winfo_screenwidth(), root.winfo_screenwidth()*windowSizes[2]]
root.y = [root.winfo_screenheight(), root.winfo_screenheight()*windowSizes[3]]
root.bind('<F11>', toggleFullscreen)
root.bind('<Escape>', exitGUI)
IO.add_event_detect(gpio[1], gpio[7], callback=gpioQuery)
IO.add_event_detect(gpio[2], gpio[7], callback=gpioQuery)
IO.add_event_detect(gpio[3], gpio[7], callback=gpioQuery)
root.title('SmartMirror v'+str(Ids[0])+' >> GUI')
root.geometry(
    '%dx%d+%d+%d' % (
        root.x[1],
        root.y[1],
        (root.winfo_screenwidth()/2-(root.winfo_screenwidth()*windowSizes[2])/2),
        (root.winfo_screenheight()/2-(root.winfo_screenheight()*windowSizes[3])/2)
    )
)
root.attributes(
    '-zoomed', windowSizes[0],
    '-fullscreen', windowSizes[1]
)
root.configure(
    background='black',
    cursor='none'
)
weather_raw = createImg(Ids[1])
weather = Label(
    root,
    image=weather_raw,
    cursor='none',
    borderwidth=0
)
weather.place(
    relx=0.5,
    y=0,
    anchor='n'
)
clock = Label(
    root,
    font=('Helvetica', 50),
    text=texts[0],
    fg='white',
    bg='black',
    cursor='none'
)
clock.place(
    relx=0.5,
    y=242,
    anchor='n'
)
cartoon_url = searchImg('http://ruthe.de/cartoons/strip_', '.jpg', 0, 9999, True)
cartoon_raw = createImg(cartoon_url, int(itemSizes[0]), int(itemSizes[1]))
cartoon = Label(
    root,
    image=cartoon_raw,
    cursor='none',
    borderwidth=0
)
cartoon.place(
    relx=0.5,
    y=422,
    anchor='n'
)
cartoon_label = Label(
    root,
    font=('Helvetica', 40),
    text=cartoon_url.split('_')[1].split('.')[0],
    fg='white',
    bg='black',
    cursor='none'
)
cartoon_label.place(
    relx=0.5,
    y=422+int(itemSizes[1]),
    anchor='n'
)
tempCpu = Label(
    root,
    font=('Helvetica', 40),
    text='$CPU°C',
    fg='white',
    bg='black',
    cursor='none'
)
tempCpu.place(
    relx=0.2,
    y=422+int(itemSizes[1]+100),
    anchor='n'
)
tempIns = Label(
    root,
    font=('Helvetica', 40),
    text='$INS°C',
    fg='white',
    bg='black',
    cursor='none'
)
tempIns.place(
    relx=0.5,
    y=422+int(itemSizes[1]+100),
    anchor='n'
)
tempOut = Label(
    root,
    font=('Helvetica', 40),
    text='$OUT°C',
    fg='white',
    bg='black',
    cursor='none'
)
tempOut.place(
    relx=0.8,
    y=422+int(itemSizes[1]+100),
    anchor='n'
)
info = Label(
    root,
    font=('Helvetica', 30),
    text=texts[1],
    fg='white',
    bg='black',
)
info.place(
    relx=0.5,
    y=422+int(itemSizes[1]+200),
    anchor='n'
)
tick()
refreshT()
refreshW(False)
gpioActive = True
gpioQuery(gpio[3])
statusLed(1)
root.mainloop()