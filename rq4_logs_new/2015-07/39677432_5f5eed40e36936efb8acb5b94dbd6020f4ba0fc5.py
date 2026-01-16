#!/usr/bin/python2
# -*- coding: utf-8 -*-
""" Clash of Clans Auto Tool for Genymotion (VirtualBox Android VM)
    Original source : https://gist.github.com/thuandt/e9cf284d7eb2d61dd99a
"""

import os
import sys
import virtualbox
import subprocess
import cv2.cv as cv
import tesseract
import PIL.ImageOps as ImageOps
from time import sleep
from PIL import Image
import traceback
import time

# Genymotion Android VM name
minK = 180
vbox = virtualbox.VirtualBox()
genymotion_vm_name = ""
scriptDir = os.path.dirname(os.path.realpath(__file__))
soundFile = scriptDir + "/sound/Gun_Shot-Marvin-1140816320.mp3"
tessdataPrefix = scriptDir



def getMenu():
 result = "Choose function?\n"
 result += "1. Keep Alive\n"
 result += "2. Auto Search\n"
 result += "3. Change Minimum K to stop ["+str(minK)+"]\n"
 result += "4. Change vm ["+genymotion_vm_name+"]\n"
 result += "5. Quit\n"
 return result


def selectVM(genymotion_vm_name):
  global genymotion_session
  genymotion_vm = vbox.find_machine(genymotion_vm_name)
  genymotion_session = genymotion_vm.create_session()
  print "VM "+genymotion_vm_name

def click(x,y):
  global genymotion_session
  ts = time.time()
  genymotion_session.console.mouse.put_mouse_event_absolute(x,y,0,0,0)
  genymotion_session.console.mouse.put_mouse_event_absolute(x,y,0,0,1)
  genymotion_session.console.mouse.put_mouse_event_absolute(x,y,0,0,0)
  print "Clicking "+str(ts)+" : " + str(x)+","+str(y)
  
def keep_alive():
    while True:
        print "Keepalive"
        click(330,200)
        sleep(60)


def auto_search():
    # click search button
    print "Next"
    click(660,300)
    sleep(6)

    ts = time.time()
    #destImg = "/tmp/coc/screens/"+genymotion_vm_name+"-"+str(ts)+"-screen.png"
    #destLoot = "/tmp/coc/loots/"+genymotion_vm_name+"-"+str(ts)+"-loot.png"
    destImg = "/tmp/"+genymotion_vm_name+"-screen.png"
    destLoot = "/tmp/"+genymotion_vm_name+"-loot.png"
    print destLoot
    # processing
    subprocess.call("adb shell screencap -p /sdcard/screen.png", shell=True)
    subprocess.call("adb pull /sdcard/screen.png '"+destImg+"'", shell=True)
    im = Image.open(destImg)
    box = (52, 67, 170, 130)
    loot = im.crop(box).convert('L')
    loot = ImageOps.invert(loot)
    loot.save(destLoot, "png")
    subprocess.call("convert "+destLoot+" -white-threshold 15% "+destLoot, shell=True)

    api = tesseract.TessBaseAPI()
    api.Init(tessdataPrefix, "coc",tesseract.OEM_DEFAULT)
    api.SetVariable("tessedit_char_whitelist", "0123456789")
    api.SetPageSegMode(tesseract.PSM_AUTO)

    image = cv.LoadImage(destLoot, cv.CV_LOAD_IMAGE_UNCHANGED)
    tesseract.SetCvImage(image,api)
    text = api.GetUTF8Text()
    conf = api.MeanTextConf()
    total_loot = text.splitlines()
    print "total loot : "+text
    gold_loot, elixir_loot = total_loot[0:2]
    print "gold : \t"+gold_loot+"\nelixir :\t "+elixir_loot
    gold_expr = gold_loot.find(" ") == 3 and int(gold_loot.split(" ")[0]) >= minK
    elixir_expr = elixir_loot.find(" ") == 3 and int(elixir_loot.split(" ")[0]) >= minK

    if gold_expr or elixir_expr:
        print gold_loot
        print elixir_loot
        print destImg
        print destLoot
        subprocess.call("play "+soundFile, shell=True)
        api.End()
        return True

    return False

if __name__ == "__main__":
    try:
        for vm in vbox.machines:
          print vm.name + " " + str(vm.state)
          if vm.state >= virtualbox.library.MachineState.running:
            genymotion_vm_name = vm.name
            break
        selectVM(genymotion_vm_name)
        while True:
            print getMenu()
            answer = raw_input("Your choice: ")
            if answer == "1":
                try:
                    keep_alive()
                except:
                    pass
            elif answer == "2":
                try:
                    while auto_search() is False:
                        pass
                except:
                    pass
            elif answer == "3":
                minK = raw_input("Enter new minimum K to stop (old : "+str(minK)+"): ")
            elif answer == "4":
                print "VM List :"
                i=1
                for vm in vbox.machines:
                  if vm.state >= virtualbox.library.MachineState.running:
                    print str(i) + " " + vm.name
                    i=i+1
                answer = raw_input("Your choice: ")
                i=1
                for vm in vbox.machines:
                  if vm.state >= virtualbox.library.MachineState.running:
                    if str(i)==answer:
                      genymotion_vm_name=str(vm.name)
                      break
                    i=i+1
                selectVM(genymotion_vm_name)
            elif answer == "5":
                sys.exit(0)
    except KeyboardInterrupt:
      print "\nKeyboardInterrupt\nExit"
      sys.exit(0)
    except SystemExit:
      print "Exit"
      sys.exit(0)
    except:
        traceback.print_exc()
        print "Unexpected error:\n", sys.exc_info()[0]
        sys.exit(1)