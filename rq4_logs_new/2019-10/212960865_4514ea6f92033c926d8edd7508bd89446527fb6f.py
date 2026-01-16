# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'eight.ui'
#
# Created by: PyQt5 UI code generator 5.13.0
#
# WARNING! All changes made in this file will be lost!


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setFixedSize(1009, 843)
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(1009, 843)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setStyleSheet("QWidget#centralwidget{border-image: url(:/beijing1/icons/eight.png);}")
        self.centralwidget.setObjectName("centralwidget")
        self.page = QtWidgets.QSpinBox(self.centralwidget)
        self.page.setGeometry(QtCore.QRect(380, 110, 111, 41))
        self.page.setLayoutDirection(QtCore.Qt.LeftToRight)
        self.page.setStyleSheet("font: 12pt \"微软雅黑\";")
        self.page.setFrame(True)
        self.page.setAlignment(QtCore.Qt.AlignLeading|QtCore.Qt.AlignLeft|QtCore.Qt.AlignVCenter)
        self.page.setMaximum(1000000)
        self.page.setObjectName("page")
        self.pushButton = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton.setGeometry(QtCore.QRect(830, 110, 91, 41))
        self.pushButton.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.pushButton.setMouseTracking(True)
        self.pushButton.setStyleSheet("font: 12pt \"微软雅黑\";\n"
"background-color: rgba(255, 255, 255, 255);")
        self.pushButton.setObjectName("pushButton")
        self.textEdit = QtWidgets.QTextEdit(self.centralwidget)
        self.textEdit.setGeometry(QtCore.QRect(0, 170, 1011, 621))
        self.textEdit.setStyleSheet("background-color: rgba(255, 255, 255, 150);\n"
"font: 25 14pt \"微软雅黑 Light\";")
        self.textEdit.setReadOnly(True)
        self.textEdit.setObjectName("textEdit")
        self.label = QtWidgets.QLabel(self.centralwidget)
        self.label.setGeometry(QtCore.QRect(520, 110, 131, 41))
        self.label.setStyleSheet("background-color: rgba(255, 254, 253, 0);\n"
"font: 16pt \"微软雅黑\";")
        self.label.setAlignment(QtCore.Qt.AlignCenter)
        self.label.setObjectName("label")
        self.userid = QtWidgets.QLineEdit(self.centralwidget)
        self.userid.setGeometry(QtCore.QRect(650, 110, 113, 41))
        self.userid.setStyleSheet("font: 10pt \"微软雅黑\";")
        self.userid.setText("")
        self.userid.setObjectName("userid")
        self.pushButton_2 = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton_2.setGeometry(QtCore.QRect(0, 0, 81, 81))
        self.pushButton_2.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.pushButton_2.setMouseTracking(True)
        self.pushButton_2.setStyleSheet("border-image: url(:/beijing1/icons/back.png);")
        self.pushButton_2.setText("")
        self.pushButton_2.setObjectName("pushButton_2")
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 1009, 26))
        self.menubar.setObjectName("menubar")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        self.pushButton.clicked.connect(MainWindow.list_search)
        self.pushButton_2.clicked.connect(MainWindow.back)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))
        self.pushButton.setText(_translate("MainWindow", "查询"))
        self.textEdit.setHtml(_translate("MainWindow", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
"<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
"p, li { white-space: pre-wrap; }\n"
"</style></head><body style=\" font-family:\'微软雅黑 Light\'; font-size:14pt; font-weight:24; font-style:normal;\">\n"
"<p style=\"-qt-paragraph-type:empty; margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><br /></p></body></html>"))
        self.label.setText(_translate("MainWindow", "user_id:"))
import background1_rc