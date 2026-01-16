import sys
import os
import string
import random

import sqlalchemy, sqlite3

from PyQt5 import QtCore, QtGui, QtWidgets, uic
from PyQt5.QtCore import pyqtSlot
from PyQt5.QtWidgets import QMessageBox

import add_password_ui
import crypt_db


class AddPassword(QtWidgets.QMainWindow):
    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self, parent)
        self.ui = add_password_ui.Ui_AddPass()
        self.ui.setupUi(self)
        self.ui.pushButtonOk.clicked.connect(self.save_to_db)
        self.ui.pushButtonGenerate.clicked.connect(self.gen_password)
        self.ui.pushButtonCancel.clicked.connect(self.close_window)

    def closeEvent(self, e):  # Функция открытия диалогового окна для подтверждения закрытия окна регистрации
        result = QtWidgets.QMessageBox.question(self, "ConfirmDIalog", "Really quit?",
                                                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        if result == QtWidgets.QMessageBox.Yes:
            e.accept()
        else:
            e.ignore()

    def save_to_db(self):
        usernameguess = self.ui.emailInput.text()
        passwordguess = self.ui.pasInput.text()
        # Проверка, если такая учетная запись существует, то вывыодить сообщение об ошибке
        # if usernameguess == USERNAME and passwordguess == PASSWORD:
        #     QMessageBox.question(self, 'Wrong login', f'\n This login and password already exists!\n', QMessageBox.Ok)
        # Проверка если поля пустые
        if (len(usernameguess) == 0 and len(passwordguess) == 0) or (
                (len(usernameguess) == 0 or len(passwordguess)) == 0):
            QMessageBox.question(self, 'Empty login or password', f'\n Please, enter login and password\n',
                                 QMessageBox.Ok)
        else:
            QMessageBox.question(self, 'Success', f'\n These login and password saved to DB!\n', QMessageBox.Ok)
            password = crypt_db.encrypt(passwordguess).decode('utf-8')
            crypt_db.create_master_password(password)

            self.close()
            os.system('python main_page.py')

    def close_window(self): # функция закрытия окна добавления пароля при нажатии на кнопку Cancel
        self.close()
        os.system('python main_page.py')

    def gen_password(self):  # Функция генерации пароля
        length = 10
        chars = string.ascii_letters + string.digits + '!@#$%^&*()'
        random.seed = (os.urandom(1024))
        generated_password = (''.join(random.choice(chars) for i in range(length)))
        print(generated_password)
        return generated_password


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    add_pass = AddPassword()
    add_pass.show()
    sys.exit(app.exec_())