from PyQt5 import QtCore, QtGui, QtWidgets

import sys

class Ui_Main(QtWidgets.QWidget):
       def setupUi(self, Main):
           Main.setObjectName("Main")
           Main.showFullScreen()

           self.QtStack = QtWidgets.QStackedLayout()

           self.stack1 = QtWidgets.QWidget()
           self.stack2 = QtWidgets.QWidget()
           self.stack3 = QtWidgets.QWidget()

           self.Window1UI()
           self.Window2UI()
           self.Window3UI()

           self.QtStack.addWidget(self.stack1)
           self.QtStack.addWidget(self.stack2)
           self.QtStack.addWidget(self.stack3)

       def Window1UI(self):
           self.stack1.showFullScreen()

           # Кнопка открыть замок
           self.openDoorButton = QtWidgets.QPushButton(self.stack1)
           self.openDoorButton.setText("Открыть замок")
           self.openDoorButton.setGeometry(QtCore.QRect(100, 220, 271, 91))
           self.openDoorButton.show()

           # Кнопка камера 1
           self.camButton1 = QtWidgets.QPushButton(self.stack1)
           self.camButton1.setText("Камера 1")
           self.camButton1.setGeometry(QtCore.QRect(102, 70, 271, 91))
           self.camButton1.show()

           # Кнопка открыть настройки
           self.configButton = QtWidgets.QPushButton(self.stack1)
           self.configButton.setText("Настройки")
           self.configButton.setGeometry(QtCore.QRect(400, 220, 271, 91))
           self.configButton.show()

           # Кнопка Камера 2
           self.camButton2 = QtWidgets.QPushButton(self.stack1)
           self.camButton2.setText("Камера 2")
           self.camButton2.setGeometry(QtCore.QRect(400, 70, 271, 91))
           self.camButton2.show()

           # Кнопка закрыть приложение ВРЕМЯНКА
           self.closeAppButton = QtWidgets.QPushButton(self.stack1)
           self.closeAppButton.setText("Закрыть")
           self.closeAppButton.setGeometry(QtCore.QRect(700, 20, 91, 51))
           self.closeAppButton.show()

       def Window2UI(self):
           self.stack2.showFullScreen()

           # Кнопка открыть замок
           self.testCallButton = QtWidgets.QPushButton(self.stack2)
           self.testCallButton.setText("Тестовый звонок")
           self.testCallButton.setGeometry(QtCore.QRect(100, 220, 271, 91))
           self.testCallButton.show()

           # Кнопка камера 1
           self.returnButton = QtWidgets.QPushButton(self.stack2)
           self.returnButton.setText("Назад")
           self.returnButton.setGeometry(QtCore.QRect(102, 70, 271, 91))
           self.returnButton.show()

           # Кнопка открыть настройки
           self.phoneConfButton = QtWidgets.QPushButton(self.stack2)
           self.phoneConfButton.setText("Настройки телефона")
           self.phoneConfButton.setGeometry(QtCore.QRect(400, 220, 271, 91))
           self.phoneConfButton.show()

           # Кнопка Камера 2
           self.networkSettingButton = QtWidgets.QPushButton(self.stack2)
           self.networkSettingButton.setText("Настройки сети")
           self.networkSettingButton.setGeometry(QtCore.QRect(400, 70, 271, 91))
           self.networkSettingButton.show()

       def Window3UI(self):
           self.stack3.showFullScreen()
           self.stack3.setStyleSheet("background: blue")