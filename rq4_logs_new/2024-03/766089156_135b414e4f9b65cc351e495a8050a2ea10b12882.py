# This Python file uses the following encoding: utf-8
import sys
from datetime import datetime

from PySide6.QtWidgets import QApplication, QMainWindow

# Important:
# You need to run the following command to generate the ui_form.py file
#     pyside6-uic form.ui -o ui_form.py, or
#     pyside2-uic form.ui -o ui_form.py
from ui_form import Ui_MainWindow

class MainWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.pushButton.clicked.connect(self.add_clicked)
        self.ui.pushButton_2.clicked.connect(self.clean_clicked)

    def add_clicked(self):
        # get current timestamp
        timestamp = datetime.now().timestamp()
        self.ui.comboBox.addItem(str(timestamp))

    def clean_clicked(self):
        self.ui.comboBox.clear()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    widget = MainWindow()
    widget.show()
    sys.exit(app.exec())