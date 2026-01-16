import sys

from PyQt5 import QtWidgets
from PyQt5.QtGui import QPixmap
from PyQt5.QtWidgets import QApplication, QMainWindow

winWidth = 1000
winHeight = 500


def window():
    app = QApplication(sys.argv)
    win = QMainWindow()
    win.setGeometry(200, 200, winWidth, winHeight)  # (upper left x, upper left y, win width, win height)
    win.setFixedSize(winWidth, winHeight)   # setting fixed dimensions, so user is not available to change window size
    win.setWindowTitle("gui human trusts")

    label = QtWidgets.QLabel(win)
    label.setText("Test")
    label.move(50, 50)

    win.show()  # show QMainWindow
    sys.exit(app.exec_())


window()