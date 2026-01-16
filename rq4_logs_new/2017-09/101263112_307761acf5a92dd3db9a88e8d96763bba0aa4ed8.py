# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'dialog1.ui'
#
# Created by: PyQt5 UI code generator 5.5.1
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_InitalDialog(object):
    def setupUi(self, InitalDialog):
        InitalDialog.setObjectName("InitalDialog")
        InitalDialog.resize(524, 129)
        self.layoutWidget = QtWidgets.QWidget(InitalDialog)
        self.layoutWidget.setGeometry(QtCore.QRect(10, 21, 502, 85))
        self.layoutWidget.setObjectName("layoutWidget")
        self.formLayout = QtWidgets.QFormLayout(self.layoutWidget)
        self.formLayout.setObjectName("formLayout")
        self.label = QtWidgets.QLabel(self.layoutWidget)
        self.label.setObjectName("label")
        self.formLayout.setWidget(0, QtWidgets.QFormLayout.LabelRole, self.label)
        self.comboBox1 = QtWidgets.QComboBox(self.layoutWidget)
        self.comboBox1.setObjectName("comboBox1")
        self.comboBox1.addItem("")
        self.comboBox1.addItem("")
        self.comboBox1.addItem("")
        self.formLayout.setWidget(1, QtWidgets.QFormLayout.LabelRole, self.comboBox1)
        self.PushButton1ok = QtWidgets.QPushButton(self.layoutWidget)
        self.PushButton1ok.setObjectName("PushButton1ok")
        self.formLayout.setWidget(2, QtWidgets.QFormLayout.LabelRole, self.PushButton1ok)

        self.retranslateUi(InitalDialog)
        QtCore.QMetaObject.connectSlotsByName(InitalDialog)

    def retranslateUi(self, InitalDialog):
        _translate = QtCore.QCoreApplication.translate
        InitalDialog.setWindowTitle(_translate("InitalDialog", "Dependency"))
        self.label.setText(_translate("InitalDialog", "Dependency 1"))
        self.comboBox1.setItemText(0, _translate("InitalDialog", "-"))
        self.comboBox1.setItemText(1, _translate("InitalDialog", "In welchen Firmen hattest du dein Projekt (Branche/OEM/Tier1,2,3)?"))
        self.comboBox1.setItemText(2, _translate("InitalDialog", "Wer ist der Kunde?"))
        self.PushButton1ok.setText(_translate("InitalDialog", "ok"))
