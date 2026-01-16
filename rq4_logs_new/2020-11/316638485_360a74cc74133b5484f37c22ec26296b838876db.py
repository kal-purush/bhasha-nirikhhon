#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""dmc.py - 
   Duet (RepRap) Monitor Capture
   - Capture still images from a webcam when the Z index changes
   - Compile the still shots into a mp4 video upon print completion"""
import argparse
# load up the argument parser
parser = argparse.ArgumentParser(description='Duet Monitor Capture v2 - www.dr-b.io')
# add the arguments
parser.add_argument('-c','--camera', help='Input video source (EX: /dev/video0)',required=True)
parser.add_argument('-p','--printer', help='Printer URL or IP address (EX: http://192.168.1.30 or http://printer_network_name)',required=True)
parser.add_argument('-v','--verbose', help='Verbose output, useful for troubleshooting',required=False, action='store_true')
parser.add_argument('-s','--skip', help='Skip first x images of printing proceess (useful to skip bed probing if enabled)', nargs='?', const=1, type=int, default=0)
parser.add_argument('-t','--timer', help='Sleep timer between testing Z position (default 30 seconds)', nargs='?', const=1, type=int, default=30)
parser.add_argument('-f','--framerate', help='FFMPEG video creation framerate (default .15 seconds)', nargs='?', const=1, type=str, default=.15)
# get the arguments
args = parser.parse_args()
verbose = args.verbose

if(verbose):
 print('Loading modules')
# import the needed modules
import requests
import shutil
import time
import os
import glob
import subprocess
import platform
import sys
# pillow may not be installed
try:
 from PIL import Image
 from PIL import ImageDraw
 from PIL import ImageFont
except:
 print('Python pillow is needed for this application, please install it and try again.')
 sys.exit()

if(verbose):
 print('Modules loaded')
 print('Loading variables')

# define the variables that will be used, most variables are pretty descriptive
varSleepTimer = args.timer
varSkipImages = args.skip
varImagesSkipped = 0
varFrameRate = args.framerate
varPrinterURL = args.printer
varAppName = 'DMCv2'
varPythonVersion = '3.7.3'
varMachineStatusURL = '/rr_status?type=2'
varPrintNameURL = '/rr_fileinfo?type=1'
varImageCounter = 0
varLastPrinterStatus = ''
varLastZHeight = ''
varPrintName = ''
varffmpegCommand = 'ffmpeg -r 1/' + str(varFrameRate) + ' -i image-%015d.jpg -vcodec mpeg4 -y print.mp4'
varffmpegImageCommand = 'ffmpeg -f video4linux2 -video_size 1920x1080 -i ' +args.camera+ ' -f image2 -frames:v 3 -update 1 -y image-^.jpg'
varImageText = varAppName + ' - https://www.dr-b.io'


if(verbose):
 print('Variables loaded')
 print('Testing python version')
# see what version of python is on the system
if varPythonVersion > platform.python_version():
 print('WARNING: this process was developed using python ', varPythonVersion)
 print('You appear to be using pythong version ',platform.python_version())
 print('Different versions may work but they may also cause issues.')
 print('If you have trouble please upgrade python to at least ',varPythonVersion)
 print('')
 print('')

if(verbose):
 print('Testing ffmpeg availability')
# verify ffmpeg is installed and accessible
try: 
 # if ffmpeg is installed this should not cause an error
 subprocess.call(['ffmpeg','-version'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
except:
 # oops, ffmpeg could not be loaded. Alert the user!
 print('ffmpeg is required but could not be loaded!')
 print('Please make sure ffmpeg is correctly installed on this system and the user running this process has access to it.')
 sys.exit()

if(verbose):
 print('Testing printer http availability')
# test the printer URL to make sure it is accessible
request = requests.get(varPrinterURL)
if request.status_code != 200:
 print(varPrinterURL + ' offline, please verify URL and try again') 
 sys.exit()

# list out the variables that are defined
if(verbose):
 print('')
 print('Defined Variables:')
 for var in dir():
  if var.startswith('var') != 0:
   print(var + ' = ' + str(vars()[var]))
 print('')
 print('Pre-tests complete, start processing')

# let the user know the process has started and tell them they can CTRL+C to exit
print(varAppName + ' started')
print('Press CTRL+C to exit')

# try/except to catch the CTRL+C break and not output error/debugging information
try:
 # main loop to process things
 while(True):
  if(verbose):
   print('Getting printer status')
  # get the printer status
  printerStatus = requests.get(varPrinterURL + varMachineStatusURL)
  # save the JSON data
  printerStatus = printerStatus.json()
  # extract the z coordinate
  zPosition = printerStatus['coords']['xyz'][2]
  if(verbose):
   print('Z position: ' + str(zPosition))
  # extract the printer status
  printerStatus = printerStatus['status']
  if(verbose):
   print('Printer status: ' + printerStatus)
  if printerStatus == 'P': # see if we are printing
   # save the last printer status
   varLastPrinterStatus = printerStatus
   # if we dont already have a print filename get it
   if varPrintName == '':
    # get the file information
    varPrintName = requests.get(varPrinterURL + varPrintNameURL)
    # get the JSON that has the filename
    varPrintName = varPrintName.json()
    # extract the filename (and strip .gcode from the name)
    varPrintName = varPrintName['fileName'].lower().replace('.gcode','')
    if(verbose):
     print('Print name: ' + varPrintName)
   if varLastZHeight != zPosition: # see if the z position has changed (to take a snapshot)
    if(verbose):
     print('Z position changed')
    # save the last z position
    varLastZHeight = zPosition
    if varImagesSkipped >= varSkipImages:
     if(verbose):
      print('Capturing image')
     # call ffmpeg to capture the image
     subprocess.call(varffmpegImageCommand.replace('^',str(varImageCounter).zfill(15)).split(),stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
     if(verbose):
      print('Image captured, inserting watermark')
     thisImage = Image.open('image-'+str(varImageCounter).zfill(15)+'.jpg')
     #Store image width and height
     w, h = thisImage.size
     drawing = ImageDraw.Draw(thisImage)
     font = ImageFont.truetype("/usr/share/fonts/truetype/freefont/FreeMonoBold.ttf",size=50)
     #font = ImageFont.truetype('Roboto-Black.ttf', 50)
     text = '  '+varImageText+'  '
     # get the text dimensions
     text_w, text_h = drawing.textsize(text, font)
     # get a buffer around the text dimensions
     pos = w - text_w, (h - text_h) - 50
     # set a background color
     c_text = Image.new('RGB', (text_w, (text_h)), color = '#000000')
     # draw the text background
     drawing = ImageDraw.Draw(c_text)
     drawing.text((0,0), text, fill="#ffffff", font=font)
     # set the text alpha
     c_text.putalpha(100)
     # put the text on the image
     thisImage.paste(c_text, pos, c_text)
     # save the image
     thisImage.save('image-'+str(varImageCounter).zfill(15)+'.jpg')
     # increment the image counter
     varImageCounter += 1
     if(verbose):
      print('Watermarked image saved')
    else:
     varImagesSkipped += 1
     if(verbose):
      print('Skipped ' + str(varImagesSkipped) + ' of ' + str(varSkipImages) + ' images')
    

  if printerStatus == 'I':  # if the printer is idle
   if(verbose):
    print('Printer is idle')
   if varLastPrinterStatus == 'P': # and the last print status was printing (just completed a print)
    if(verbose):
     print('Printer just completed a print job')
    # update the print status
    varLastPrinterStatus = printerStatus
    if(verbose):
     print('Creating video from still shots using ffmpeg')
     print(varffmpegCommand)
    # run ffmpeg to convert the still images to a video
    subprocess.call(varffmpegCommand.split(),stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    completeFileName = varPrintName.replace(' ','_') + '_' + time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime()) + '.mp4'
    if(verbose):
     print('Moving print.mp4 to ' + completeFileName)
    # move (rename) the video to the correct filename and timestamp
    shutil.move('print.mp4', completeFileName)
    if(verbose):
     print('Getting a list of still images to delete')
    # get the temporary still images created
    fileList = glob.glob('image-*.jpg')
    for file in fileList:
     if(verbose):
      print('Deleting ' + file)
     try:
      os.remove(file)
     except:
      print('Error removing ',file)
    # reset the print name to prepare for the next print
    varPrintName = ''
    # reset the image counter
    varImageCounter = 0
    varImagesSkipped = 0
  if(verbose):
   print('Sleeping '+str(varSleepTimer)+' seconds')
  # sleep for a set time
  time.sleep(varSleepTimer)
except KeyboardInterrupt:
 pass
