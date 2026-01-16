#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys
import subprocess
import urllib
from urllib2 import urlopen
from lxml import html
import os

from PyQt4 import QtGui, QtCore
from PyQt4.Qt import *


seconds = 2	#Start counting down from 2
last_title = ''
ya_music_url = ''

class myMessageBox(QMessageBox):
    seconds = 2
    @pyqtSlot()
    def timeoutSlot(self):
        global seconds
        seconds -= 1
        if seconds==0:
            QMessageBox.close(self)
            QMessageBox.done(self, 1)

class SystemTrayIcon(QtGui.QSystemTrayIcon):
    def __init__(self, icon, parent=None):
       self.parent = parent
       QtGui.QSystemTrayIcon.__init__(self, icon, parent)
       self.menu = QtGui.QMenu(parent)
       nextAction = self.menu.addAction("Next", self.next)
       playAction = self.menu.addAction("Play/Pause", self.play)
       likeAction = self.menu.addAction("Like", self.like)
       textAction = self.menu.addAction("Text", self.text)
       downloadAction = self.menu.addAction("Download", self.download)
       exitAction = self.menu.addAction("Exit", self.exit)
       self.setContextMenu(self.menu)

       self.timer = QTimer()
       self.timer.timeout.connect(self.update)
       self.timer.start(3000)

    def next(self):
      self.remoteCall('next')

    def play(self):
      self.remoteCall('playpause')

    def like(self):
      self.remoteCall('like')

    def download(self):
      self.remoteCall('download')

    def text(self):
      self.remoteCall('getCurrent')

    def download(self):
        info = self.remoteCall('getCurrent')
        title = info['result']['title'] + " - " + info['result']['artists'][0]['name']
        url = 'http:' + info['result']['_$e_']
        name = QtGui.QFileDialog.getSaveFileName(self.parent, "Save File", title + '.mp3', "MP3 files (*.mp3);;All Files (*)")
        urllib.urlretrieve('http:' + info['result']['_$e_'], unicode(name))

    def update(self):
        info = self.remoteCall('getCurrent')
        if info:
            if info['result']['lyricsAvailable']:
                global ya_music_url
                ya_music_url = 'https://music.yandex.ru/album/' + str(info['result']['albums'][0]['id']) + '/track/' + str(info['result']['id'])
            title = info['result']['title'] + " - " + info['result']['artists'][0]['name']
            global last_title
            if last_title != title:
                last_title = title
                messageBox = myMessageBox(0,"Auto-Close QMessagebox", '<font size="4">' + title + '</font>')
                messageBox.setWindowFlags(Qt.WindowStaysOnTopHint | Qt.FramelessWindowHint)
                messageBox.setStandardButtons(QMessageBox.NoButton);
                messageBox.__pos = QPoint(1200,  75)
                messageBox.move(messageBox.__pos)
                global seconds
                seconds = 2
                timer = QTimer()
                messageBox.connect(timer,SIGNAL("timeout()"), messageBox,SLOT("timeoutSlot()"))
                timer.start(1000)
                messageBox.show()
                messageBox.exec_()

    def remoteCall(self, action):
      proc = subprocess.Popen(["echo 'Mu.Flow." + action + "();' | nc localhost 32000 & sleep 0.3 ; kill $!"], stdout=subprocess.PIPE, shell=True)
      (out, err) = proc.communicate()
      if action == "getCurrent" and out:
        info = json.loads(out)
        return info

    def exit(self):
      QtCore.QCoreApplication.exit()

def main():
   app = QtGui.QApplication(sys.argv)
   app.setQuitOnLastWindowClosed(False)

   w = QtGui.QWidget()
   trayIcon = SystemTrayIcon(QtGui.QIcon(os.path.join(os.path.dirname(__file__), 'icons/icon.ico')), w)
   trayIcon.show()

   sys.exit(app.exec_())

if __name__ == '__main__':
    main()