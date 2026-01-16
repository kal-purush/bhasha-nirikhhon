import sys
from random import uniform, randint

from PyQt6 import uic
from PyQt6.QtWidgets import QMainWindow
from PyQt6 import QtCore
from PyQt6.QtGui import QPainter, QColor
from PyQt6.QtWidgets import QApplication


class Window(QMainWindow):
    def __init__(self):
        super(Window, self).__init__()
        uic.loadUi('UI.ui', self)
        self.pushButton.clicked.connect(self.paint)

        self.do_paint = True

    def paintEvent(self, event):
        if self.do_paint:
            qp = QPainter()
            qp.begin(self)
            self.draw_ellipse(qp)
            qp.end()
        self.do_paint = False

    def paint(self):
        self.do_paint = True
        self.update()

    def draw_ellipse(self, qp):
        size = uniform(20, 100)
        qp.setBrush(QColor('yellow'))
        center = QtCore.QPointF(randint(40, 600), randint(40, 500))
        qp.drawEllipse(center, size, size)


def except_hook(cls, exception, traceback):
    sys.__excepthook__(cls, exception, traceback)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = Window()
    ex.show()
    sys.excepthook = except_hook
    sys.exit(app.exec())