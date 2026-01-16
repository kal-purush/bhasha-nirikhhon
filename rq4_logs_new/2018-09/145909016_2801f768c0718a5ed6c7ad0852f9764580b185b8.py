import sys
import os

from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QMessageBox

from login_page_py import *
import crypt_db

USERNAME = crypt_db.get_master_login()
PASSWORD = crypt_db.get_master_pass()


class LoginWindow(QtWidgets.QMainWindow):
    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self, parent)
        self.ui = Ui_login_page()
        self.ui.setupUi(self)
        # self.ui.pushButtonSave.triggered.connect(self.save_to_db)
        self.ui.loginButton.clicked.connect(self.check_data)

        self.ui.createButton.clicked.connect(self.change_window)

    # Функция проверки правильности данных пользователя
    def check_data(self):
        usernameGuess = self.ui.emailInput.text()
        passwordGuess = self.ui.pasInput.text()

        print(usernameGuess)
        print(passwordGuess)

        if usernameGuess == USERNAME and passwordGuess == PASSWORD:
            QMessageBox.question(self, 'Approved', f'\n<--Welcome back master {usernameGuess}!-->\n', QMessageBox.Yes)
            print(f'\n<--Welcome back master {usernameGuess}!-->\n')
            # crypt_db.output()
        else:
            QMessageBox.question(self, 'Approved', '\nYou are not prepared!!!', QMessageBox.Yes)
            print('\nYou are not prepared!!!')

        print('\n<--Greetings you!-->')

    def change_window(self):
        self.close()
        os.system('python registration_page.py')



if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)

    login_page = LoginWindow()
    login_page.show()

    sys.exit(app.exec_())

