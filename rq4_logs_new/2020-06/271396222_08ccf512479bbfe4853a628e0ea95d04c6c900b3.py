# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'configWindow.ui'
##
## Created by: Qt User Interface Compiler version 5.15.0
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PyQt5.QtCore import (QCoreApplication, QDate, QDateTime, QMetaObject,
    QObject, QPoint, QRect, QSize, QTime, QUrl, Qt)
from PyQt5.QtGui import (QBrush, QColor, QConicalGradient, QCursor, QFont,
    QFontDatabase, QIcon, QKeySequence, QLinearGradient, QPalette, QPainter,
    QPixmap, QRadialGradient)
from PyQt5.QtWidgets import *


class Ui_Dialog(object):
    def setupUi(self, Dialog):
        if not Dialog.objectName():
            Dialog.setObjectName(u"Dialog")
        Dialog.showFullScreen()

        # Кнопка открыть замок
        self.testCallButton = QPushButton(Dialog)
        self.testCallButton.setObjectName(u"testCallButton")
        self.testCallButton.setGeometry(QRect(100, 220, 271, 91))
        self.testCallButton.show()

        # Кнопка камера 1
        self.networkSettingButton = QPushButton(Dialog)
        self.networkSettingButton.setObjectName(u"networkSettingButton")
        self.networkSettingButton.setGeometry(QRect(102, 70, 271, 91))
        self.networkSettingButton.show()

        # Кнопка конфиг
        self.returnButton = QPushButton(Dialog)
        self.returnButton.setObjectName(u"returnButton")
        self.returnButton.setGeometry(QRect(400, 220, 271, 91))
        self.returnButton.show()

        # Кнопка камера 2
        self.phoneConfButton = QPushButton(Dialog)
        self.phoneConfButton.setObjectName(u"phoneConfButton")
        self.phoneConfButton.setGeometry(QRect(400, 70, 271, 91))
        self.phoneConfButton.show()

        self.retranslateUi(Dialog)

        QMetaObject.connectSlotsByName(Dialog)
    # setupUi

    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(QCoreApplication.translate("Dialog", u"Dialog", None))
        self.testCallButton.setText(QCoreApplication.translate("Dialog",
                                                               u"Тестовый звонок",
                                                               None))
        self.networkSettingButton.setText(QCoreApplication.translate("Dialog", u"Настройки сети", None))
        self.returnButton.setText(
            QCoreApplication.translate("Dialog", u"Вернуться назад", None))
        self.phoneConfButton.setText(QCoreApplication.translate("Dialog", u"Настройки телефона", None))
    # retranslateUi
