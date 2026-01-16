# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'form.ui'
#
# Created: Tue Aug 30 19:58:13 2016
#      by: PyQt4 UI code generator 4.11.2
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    def _fromUtf8(s):
        return s

try:
    _encoding = QtGui.QApplication.UnicodeUTF8
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig)

class Ui_SelectedWidget(object):
    def setupUi(self, Form):
        Form.setObjectName(_fromUtf8("Form"))
        Form.resize(270, 109)
        self.btn_Qtd = QtGui.QPushButton(Form)
        self.btn_Qtd.setGeometry(QtCore.QRect(20, 20, 111, 71))
        self.btn_Qtd.setObjectName(_fromUtf8("btn_Qtd"))
        self.btn_Origin = QtGui.QPushButton(Form)
        self.btn_Origin.setGeometry(QtCore.QRect(150, 20, 101, 71))
        self.btn_Origin.setObjectName(_fromUtf8("btn_Origin"))
        self.retranslateUi(Form)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        Form.setWindowTitle(_translate("Form", "Form", None))
        self.btn_Qtd.setText(_translate("Form", "QTD", None))
        self.btn_Origin.setText(_translate("Form", "VNPY", None))


if __name__ == "__main__":
    import sys
    app = QtGui.QApplication(sys.argv)
    Form = QtGui.QWidget()
    ui = Ui_SelectedWidget()
    ui.setupUi(Form)
    Form.show()
    sys.exit(app.exec_())
