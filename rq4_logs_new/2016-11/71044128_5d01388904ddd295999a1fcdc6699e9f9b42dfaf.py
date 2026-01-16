from PyQt5 import Qt
import sys
app = Qt.QApplication(sys.argv)
systemtray_icon = Qt.QSystemTrayIcon(Qt.QIcon('icon.jpg'))
systemtray_icon.show()
systemtray_icon.showMessage('Title', 'Content')