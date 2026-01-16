import sys

from PyQt5.QtWidgets import QMainWindow, QApplication
from ui_code.qrcode_ui import Qrcode_Window

class QR_MainWindow(QMainWindow, Qrcode_Window):
    def __init__(self):
        super(QR_MainWindow, self).__init__()
        self.setupUi(self)
        self.setWindowTitle('支付界面')

# 以下为测试代码，可以不用管
if __name__ == '__main__':
    app = QApplication(sys.argv)
    qr_ui = QR_MainWindow()
    qr_ui.show()
    sys.exit(app.exec())
