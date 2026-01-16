from PyQt5.QtCore import *
from PyQt5.QtWidgets import *


class EventDialog(QDialog):
    def __init__(self, parent=None):
        super(EventDialog, self).__init__(parent)
        self.setupUi()

    def setupUi(self):
        if self.objectName():
            self.setObjectName(u"EventDialog")
        self.setWindowTitle("Create event")
        self.resize(364, 385)

        self.buttonBox = QDialogButtonBox(self)
        self.buttonBox.setObjectName(u"buttonBox")
        self.buttonBox.setGeometry(QRect(50, 340, 260, 35))
        self.buttonBox.setOrientation(Qt.Horizontal)
        self.buttonBox.setStandardButtons(QDialogButtonBox.Cancel | QDialogButtonBox.Ok)
        self.buttonBox.setCenterButtons(True)

        self.textBrowser = QTextBrowser(self)
        self.textBrowser.setObjectName(u"textBrowser")
        self.textBrowser.setGeometry(QRect(15, 130, 330, 190))

        self.themeEdit = QLineEdit(self)
        self.themeEdit.setObjectName(u"themeEdit")
        self.themeEdit.setGeometry(QRect(70, 10, 270, 25))

        self.beginningDate = QDateEdit(self)
        self.beginningDate.setObjectName(u"beginningDate")
        self.beginningDate.setGeometry(QRect(70, 50, 110, 25))

        self.endingDate = QDateEdit(self)
        self.endingDate.setObjectName(u"endingDate")
        self.endingDate.setGeometry(QRect(70, 80, 110, 25))

        self.beginningTime = QTimeEdit(self)
        self.beginningTime.setObjectName(u"beginningTime")
        self.beginningTime.setGeometry(QRect(190, 50, 120, 25))

        self.themeLabel = QLabel(self)
        self.themeLabel.setObjectName(u"themeLabel")
        self.themeLabel.setGeometry(QRect(5, 10, 60, 20))
        self.themeLabel.setText("Тема")

        self.beginningLabel = QLabel(self)
        self.beginningLabel.setObjectName(u"beginningLabel")
        self.beginningLabel.setGeometry(QRect(5, 50, 60, 20))
        self.beginningLabel.setText("Начало")

        self.endingLabel = QLabel(self)
        self.endingLabel.setObjectName(u"endingLabel")
        self.endingLabel.setGeometry(QRect(5, 80, 60, 20))
        self.endingLabel.setText("Окончание")

        self.commentLabel = QLabel(self)
        self.commentLabel.setObjectName(u"commentLabel")
        self.commentLabel.setGeometry(QRect(150, 110, 70, 16))
        self.commentLabel.setText("Комментарий")

        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)

        QMetaObject.connectSlotsByName(self)