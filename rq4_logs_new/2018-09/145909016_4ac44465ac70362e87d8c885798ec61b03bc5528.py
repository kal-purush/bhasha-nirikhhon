# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'login_page.ui'
#
# Created by: PyQt5 UI code generator 5.11.2
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_login_page(object):
    def setupUi(self, login_page):
        login_page.setObjectName("login_page")
        login_page.resize(301, 502)
        login_page.setMinimumSize(QtCore.QSize(301, 502))
        login_page.setMaximumSize(QtCore.QSize(301, 502))
        login_page.setStyleSheet("background-color: rgb(255, 255, 255);")
        self.centralwidget = QtWidgets.QWidget(login_page)
        self.centralwidget.setStyleSheet("")
        self.centralwidget.setObjectName("centralwidget")
        self.header = QtWidgets.QFrame(self.centralwidget)
        self.header.setGeometry(QtCore.QRect(0, 0, 301, 151))
        self.header.setStyleSheet("background-color: rgb(0, 255, 0);\n"
"")
        self.header.setFrameShape(QtWidgets.QFrame.StyledPanel)
        self.header.setFrameShadow(QtWidgets.QFrame.Raised)
        self.header.setObjectName("header")
        self.logo = QtWidgets.QLabel(self.header)
        self.logo.setGeometry(QtCore.QRect(10, 20, 281, 71))
        self.logo.setStyleSheet("background-color: rgb(192, 89, 255);")
        self.logo.setObjectName("logo")
        self.emailWrapper = QtWidgets.QWidget(self.centralwidget)
        self.emailWrapper.setGeometry(QtCore.QRect(0, 190, 291, 61))
        self.emailWrapper.setProperty("lineEdit_3", "")
        self.emailWrapper.setObjectName("emailWrapper")
        self.emailInput = QtWidgets.QLineEdit(self.emailWrapper)
        self.emailInput.setGeometry(QtCore.QRect(10, 30, 241, 21))
        self.emailInput.setObjectName("emailInput")
        self.label_5 = QtWidgets.QLabel(self.emailWrapper)
        self.label_5.setGeometry(QtCore.QRect(10, 10, 241, 16))
        self.label_5.setObjectName("label_5")
        self.pasWrapper = QtWidgets.QWidget(self.centralwidget)
        self.pasWrapper.setGeometry(QtCore.QRect(0, 280, 291, 61))
        self.pasWrapper.setProperty("lineEdit_3", "")
        self.pasWrapper.setObjectName("pasWrapper")
        self.pasInput = QtWidgets.QLineEdit(self.pasWrapper)
        self.pasInput.setGeometry(QtCore.QRect(10, 30, 241, 21))
        self.pasInput.setObjectName("pasInput")
        self.label_7 = QtWidgets.QLabel(self.pasWrapper)
        self.label_7.setGeometry(QtCore.QRect(10, 10, 231, 16))
        self.label_7.setObjectName("label_7")
        self.footer = QtWidgets.QWidget(self.centralwidget)
        self.footer.setGeometry(QtCore.QRect(0, 390, 301, 101))
        self.footer.setObjectName("footer")
        self.loginButton = QtWidgets.QPushButton(self.footer)
        self.loginButton.setGeometry(QtCore.QRect(115, 30, 71, 21))
        self.loginButton.setStyleSheet("background-color: rgb(177, 177, 177);")
        self.loginButton.setObjectName("loginButton")
        self.createButton = QtWidgets.QPushButton(self.footer)
        self.createButton.setGeometry(QtCore.QRect(80, 60, 141, 21))
        self.createButton.setObjectName("createButton")
        self.emailWrapper.raise_()
        self.header.raise_()
        self.pasWrapper.raise_()
        self.footer.raise_()
        login_page.setCentralWidget(self.centralwidget)
        self.statusBar = QtWidgets.QStatusBar(login_page)
        self.statusBar.setObjectName("statusBar")
        login_page.setStatusBar(self.statusBar)

        self.retranslateUi(login_page)
        QtCore.QMetaObject.connectSlotsByName(login_page)

    def retranslateUi(self, login_page):
        _translate = QtCore.QCoreApplication.translate
        login_page.setWindowTitle(_translate("login_page", "MainWindow"))
        self.logo.setText(_translate("login_page", "<html><head/><body><p align=\"center\"><span style=\" font-size:36pt;\">PassVault</span></p></body></html>"))
        self.emailInput.setText(_translate("login_page", "Enter your email"))
        self.label_5.setText(_translate("login_page", "Email"))
        self.pasInput.setText(_translate("login_page", "Enter your password"))
        self.label_7.setText(_translate("login_page", "Password"))
        self.loginButton.setText(_translate("login_page", "Log in"))
        self.createButton.setText(_translate("login_page", "Create an account "))
