import sys
from PySide6.QtWidgets import QApplication, QMainWindow, QFileDialog
from PySide6.QtCore import QFile
from Ui_menu import Ui_menu
from Video_player import Video_player

class Menu(QMainWindow, Ui_menu):
    def __init__(self):
        super(Menu, self).__init__()

        self.setupUi(self)
        
        self.setAcceptDrops(True)
        self.uploadBtn.clicked.connect(self.upload_video)

    def upload_video(self):
        fileName, fileType = QFileDialog.getOpenFileName(self, "Open file", "./", "MKV Files (*.mkv);;MP4 Files (*.mp4)")
        self.close()
        self.playWidget = Video_player(fileName)
        self.playWidget.show()

    def dragEnterEvent(self, event):
        if '.mkv' in event.mimeData().text() or '.mp4' in event.mimeData().text():
            event.accept()
        else:
            event.ignore()
        
    def dropEvent(self, event):
        self.playWidget = Video_player(event.mimeData().text().replace('file:///', ''))
        self.close()
        self.playWidget.show()
        

if __name__ == '__main__':
    app = QApplication(sys.argv)  
    window = Menu()
    window.show()
    app.exec()