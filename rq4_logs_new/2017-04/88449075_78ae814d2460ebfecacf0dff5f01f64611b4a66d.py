# -*- coding: utf-8 -*-
"""
Created on Sat Apr  8 11:30:38 2017

@author: ZhiBin
"""

import re, requests
from bs4 import BeautifulSoup
from PyQt5.QtWidgets import QApplication, QCheckBox, QPushButton
from PyQt5.QtGui import QPixmap
import PyQt5.uic

ui_file = 'mainwindowmz.ui'
(class_ui, class_basic_class) = PyQt5.uic.loadUiType(ui_file)

class Window(class_basic_class, class_ui):
    def __init__(self):
        super(Window, self).__init__()
        self.setupUi(self)
        totlist = Mzitu().printli()
        self.checkBoxs = [self._addCheckbox(item[0], item[1]) for item in totlist]
        self.pusjButtons = [self._addpushButtonpic(item[0], item[4]) for item in totlist]
        self.pushButton.clicked.connect(self.getSelList)
        
    def getSelList(self):
        sellist = [(item.objectName(), item.text()) for item in self.checkBoxs if item.isChecked() == True]
        [self.textBrowser.append('http://www.mzitu.com/'+text[0]) for text in sellist]
        return sellist

    def _addCheckbox(self, idd, boxtitle):
        checkBox = QCheckBox(self.verticalLayoutWidget)
        checkBox.setObjectName(idd)
        checkBox.setText(boxtitle)
        self.verticalLayout.addWidget(checkBox)
        return checkBox
    
    def _addpushButtonpic(self, idd, href):
        pushButton = QPushButton(self.verticalLayoutWidget_2)
        pushButton.setObjectName(idd)
        pushButton.setText(idd)
        self.verticalLayout_pic.addWidget(pushButton)
        pushButton.clicked.connect(lambda: self._showpic(idd, href))
        return pushButton
    
    def _showpic(self, idd, href):
        pic = requests.get(href).content
        pixmap = QPixmap()
        pixmap.loadFromData(pic)
        self.label.setPixmap(pixmap)

class Mzitu():
    
    def __init__(self):
        url = "http://www.mzitu.com/"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
                   'Host': 'www.mzitu.com'}
        response = requests.get(url, headers = headers)
        content = BeautifulSoup(response.text, 'lxml')
        linkblock = content.find('div', class_="postlist")
        self.linklist = linkblock.find_all('li')
    
    def printli(self):
        linklist = self.linklist
        linksum = list()
        
        for link in linklist:
            url = link.a.get('href')
            picurl = link.img.get('data-original')
            linkid = re.search(r'(\d+)', url).group()
            firstspan = link.span
            titleword = firstspan.get_text()
            secondspan = firstspan.find_next_sibling('span')
            uploadtime = secondspan.get_text()
            thirdspan = secondspan.find_next_sibling('span')
            viewcount = thirdspan.get_text()
            linksum.append((linkid, titleword, uploadtime, viewcount, picurl))
        return linksum
            
if __name__ == '__main__':
    import sys
    
    app = QApplication(sys.argv)
    MainWindow = Window()
    MainWindow.show()
    sys.exit(app.exec_())