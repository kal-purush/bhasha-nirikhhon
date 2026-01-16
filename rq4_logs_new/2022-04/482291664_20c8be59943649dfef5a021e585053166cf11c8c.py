import sys
from PyQt5 import QtCore, QtGui
from PyQt5.QtWidgets import QWidget, QLabel, QLineEdit, QPushButton, QApplication
from sign import Register


class MainWindow(object):
    def setupUi(self, Form):
        Form.setObjectName("Form")
        Form.resize(370, 377)
        font = QtGui.QFont()
        font.setFamily("MS Shell Dlg 2")
        font.setPointSize(14)
        font.setBold(False)
        font.setItalic(False)
        font.setWeight(7)
        Form.setFont(font)
        Form.setStyleSheet("QWidget{\n"
                           "background-color: rgb(86, 86, 86);\n"
                           "    font: 63 14pt \"MS Shell Dlg 2\";\n"
                           "}\n"
                           "QLineEdit{\n"
                           "background-color: rgb(121, 167, 177);\n"
                           "border-radius:15px;\n"
                           "}\n"
                           "QPushButton{\n"
                           "background-color: rgb(121, 167, 177);\n"
                           "}\n"
                           "QLabel{\n"
                           "color: rgb(255, 255, 255);\n"
                           "}")
        Form.setInputMethodHints(QtCore.Qt.ImhNone)
        self.label = QLabel(Form)
        self.label.setGeometry(QtCore.QRect(140, 10, 71, 31))
        font = QtGui.QFont()
        font.setFamily("MS Shell Dlg 2")
        font.setPointSize(14)
        font.setBold(False)
        font.setItalic(False)
        font.setWeight(7)
        self.label.setFont(font)
        self.label.setAlignment(QtCore.Qt.AlignCenter)
        self.label.setObjectName("label")
        self.lineEdit = QLineEdit(Form)
        self.lineEdit.setGeometry(QtCore.QRect(49, 110, 261, 30))
        font = QtGui.QFont()
        font.setFamily("MS Shell Dlg 2")
        font.setPointSize(14)
        font.setBold(False)
        font.setItalic(False)
        font.setWeight(7)
        self.lineEdit.setFont(font)
        self.lineEdit.setStatusTip("")
        self.lineEdit.setWhatsThis("")
        self.lineEdit.setInputMethodHints(QtCore.Qt.ImhNone)
        self.lineEdit.setText("")
        self.lineEdit.setMaxLength(25)
        self.lineEdit.setEchoMode(QLineEdit.Normal)
        self.lineEdit.setDragEnabled(False)
        self.lineEdit.setReadOnly(False)
        self.lineEdit.setObjectName("lineEdit")
        self.lineEdit_2 = QLineEdit(Form)
        self.lineEdit_2.setGeometry(QtCore.QRect(49, 190, 261, 30))
        font = QtGui.QFont()
        font.setFamily("MS Shell Dlg 2")
        font.setPointSize(14)
        font.setBold(False)
        font.setItalic(False)
        font.setWeight(7)
        self.lineEdit_2.setFont(font)
        self.lineEdit_2.setInputMethodHints(
            QtCore.Qt.ImhHiddenText | QtCore.Qt.ImhNoAutoUppercase | QtCore.Qt.ImhNoPredictiveText | QtCore.Qt.ImhSensitiveData)
        self.lineEdit_2.setMaxLength(22)
        self.lineEdit_2.setEchoMode(QLineEdit.Password)
        self.lineEdit_2.setObjectName("lineEdit_2")
        self.label_2 = QLabel(Form)
        self.label_2.setGeometry(QtCore.QRect(50, 75, 131, 21))
        self.label_2.setObjectName("label_2")
        self.label_3 = QLabel(Form)
        self.label_3.setGeometry(QtCore.QRect(50, 156, 131, 20))
        self.label_3.setObjectName("label_3")
        self.pushButton_2 = QPushButton(Form)
        self.pushButton_2.setGeometry(QtCore.QRect(200, 260, 111, 41))
        self.pushButton_2.setStyleSheet("")
        self.pushButton_2.setObjectName("pushButton_2")
        self.pushButton = QPushButton(Form)
        self.pushButton.setGeometry(QtCore.QRect(60, 260, 111, 41))
        self.pushButton.setStyleSheet("")
        self.pushButton.setObjectName("pushButton")

        # connect buttons
        self.pushButton.clicked.connect(self.btn_login_clicked)
        self.pushButton_2.clicked.connect(self.show_window_2)

        self.retranslateUi(Form)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        _translate = QtCore.QCoreApplication.translate
        Form.setWindowTitle(_translate("Form", "Form"))
        self.label.setText(_translate("Form", "Log In"))
        self.lineEdit.setPlaceholderText(_translate("Form", "Username"))
        self.lineEdit_2.setPlaceholderText(_translate("Form", "Password"))
        self.label_2.setText(_translate("Form", "Username"))
        self.label_3.setText(_translate("Form", "Password"))
        self.pushButton_2.setText(_translate("Form", "Register"))
        self.pushButton.setText(_translate("Form", "Log in"))

    def btn_login_clicked(self):
        pass



    def show_window_2(self):
        self.window2 = QWidget()
        self.ui = Register()
        self.ui.setupUi(self.window2)
        self.window2.show()
        Form.hide()



if __name__ == "__main__":
    app = QApplication(sys.argv)
    Form = QWidget()
    ui = MainWindow()
    ui.setupUi(Form)
    Form.show()
    sys.exit(app.exec_())