from PyQt5.QtWidgets import QApplication
from types.visualization.mainwindow import MainWindow

import sys
import fileinput

if __name__ == "__main__":
    app = QApplication(sys.argv)
    main_window = MainWindow()

    if len(app.arguments()) == 2 and app.arguments()[1] == "--ti":
        str_input = sys.stdin.read()
        main_window.set_clusters(str_input)

    main_window.showMaximized()

    sys.exit(app.exec())