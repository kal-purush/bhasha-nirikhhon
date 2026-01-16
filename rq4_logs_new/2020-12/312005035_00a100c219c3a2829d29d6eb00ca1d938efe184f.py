from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(1400, 820)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")

        self.InputImage = QtWidgets.QLabel(self.centralwidget)
        self.InputImage.setGeometry(QtCore.QRect(20, 10, 512, 512))
        self.InputImage.setFrameShape(QtWidgets.QFrame.Box)
        self.InputImage.setText("")
        self.InputImage.setObjectName("InputImage")

        self.MeasuredImage = QtWidgets.QLabel(self.centralwidget)
        self.MeasuredImage.setGeometry(QtCore.QRect(868, 10, 512, 512))
        self.MeasuredImage.setFrameShape(QtWidgets.QFrame.Box)
        self.MeasuredImage.setText("")
        self.MeasuredImage.setObjectName("MeasuredImage")

        self.MetricsTable = QtWidgets.QTableView(self.centralwidget)
        self.MetricsTable.setGeometry(QtCore.QRect(572, 160, 256, 211))
        self.MetricsTable.setObjectName("MetricsTable")

        self.RMSELabel = QtWidgets.QLabel(self.centralwidget)
        self.RMSELabel.setGeometry(QtCore.QRect(590, 180, 211, 20))
        self.RMSELabel.setObjectName("RMSELabel")

        self.PSNRLabel = QtWidgets.QLabel(self.centralwidget)
        self.PSNRLabel.setGeometry(QtCore.QRect(590, 230, 211, 20))
        self.PSNRLabel.setObjectName("PSNRLabel")

        self.SSIMLabel = QtWidgets.QLabel(self.centralwidget)
        self.SSIMLabel.setGeometry(QtCore.QRect(590, 280, 211, 20))
        self.SSIMLabel.setObjectName("SSIMLabel")

        self.FSIMLabel = QtWidgets.QLabel(self.centralwidget)
        self.FSIMLabel.setGeometry(QtCore.QRect(590, 330, 211, 20))
        self.FSIMLabel.setObjectName("FSIMLabel")

        self.CalculateMetricsButton = QtWidgets.QPushButton(self.centralwidget)
        self.CalculateMetricsButton.setGeometry(QtCore.QRect(640, 410, 120, 23))
        self.CalculateMetricsButton.setObjectName("CalculateMetricsButton")

        self.BrightnessSlider = QtWidgets.QSlider(self.centralwidget)
        self.BrightnessSlider.setGeometry(QtCore.QRect(100, 580, 1200, 25))
        self.BrightnessSlider.setOrientation(QtCore.Qt.Horizontal)
        self.BrightnessSlider.setObjectName("BrightnessSlider")

        self.NoiseSlider = QtWidgets.QSlider(self.centralwidget)
        self.NoiseSlider.setGeometry(QtCore.QRect(100, 650, 1200, 25))
        self.NoiseSlider.setOrientation(QtCore.Qt.Horizontal)
        self.NoiseSlider.setObjectName("NoiseSlider")

        self.BrightnessLabel = QtWidgets.QLabel(self.centralwidget)
        self.BrightnessLabel.setEnabled(True)
        self.BrightnessLabel.setGeometry(QtCore.QRect(100, 550, 111, 21))
        font = QtGui.QFont()
        font.setPointSize(10)
        self.BrightnessLabel.setFont(font)
        self.BrightnessLabel.setObjectName("BrightnessLabel")

        self.NoiseLabel = QtWidgets.QLabel(self.centralwidget)
        self.NoiseLabel.setEnabled(True)
        self.NoiseLabel.setGeometry(QtCore.QRect(100, 620, 111, 21))
        font = QtGui.QFont()
        font.setPointSize(10)
        self.NoiseLabel.setFont(font)
        self.NoiseLabel.setObjectName("NoiseLabel")

        self.ContrastSlider = QtWidgets.QSlider(self.centralwidget)
        self.ContrastSlider.setGeometry(QtCore.QRect(100, 720, 1200, 25))
        self.ContrastSlider.setOrientation(QtCore.Qt.Horizontal)
        self.ContrastSlider.setObjectName("ContrastSlider")

        self.ContrastLabel = QtWidgets.QLabel(self.centralwidget)
        self.ContrastLabel.setEnabled(True)
        self.ContrastLabel.setGeometry(QtCore.QRect(100, 690, 111, 21))
        font = QtGui.QFont()
        font.setPointSize(10)
        self.ContrastLabel.setFont(font)
        self.ContrastLabel.setObjectName("ContrastLabel")

        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 1400, 21))
        self.menubar.setObjectName("menubar")
        self.menuFile = QtWidgets.QMenu(self.menubar)
        self.menuFile.setObjectName("menuFile")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)
        self.actionOpen_image = QtWidgets.QAction(MainWindow)
        self.actionOpen_image.setObjectName("actionOpen_image")
        self.actionExit = QtWidgets.QAction(MainWindow)
        self.actionExit.setObjectName("actionExit")
        self.menuFile.addAction(self.actionOpen_image)
        self.menuFile.addSeparator()
        self.menuFile.addAction(self.actionExit)
        self.menubar.addAction(self.menuFile.menuAction())

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)
        self.actionExit.triggered.connect(self.closeProgram)
        self.actionOpen_image.triggered.connect(self.loadImage)


    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Image Similarity"))
        self.RMSELabel.setText(_translate("MainWindow", "RMSE: "))
        self.PSNRLabel.setText(_translate("MainWindow", "PSNR: "))
        self.SSIMLabel.setText(_translate("MainWindow", "SSIM: "))
        self.FSIMLabel.setText(_translate("MainWindow", "FSIM: "))
        self.CalculateMetricsButton.setText(_translate("MainWindow", "Calculate metrics"))
        self.BrightnessLabel.setText(_translate("MainWindow", "Brightness"))
        self.NoiseLabel.setText(_translate("MainWindow", "Noise"))
        self.ContrastLabel.setText(_translate("MainWindow", "Contrast"))
        self.menuFile.setTitle(_translate("MainWindow", "File"))
        self.actionOpen_image.setText(_translate("MainWindow", "Open image"))
        self.actionOpen_image.setStatusTip(_translate("MainWindow", "Open an image from your computer"))
        self.actionOpen_image.setShortcut(_translate("MainWindow", "Ctrl+O"))
        self.actionExit.setText(_translate("MainWindow", "Exit"))
        self.actionExit.setStatusTip(_translate("MainWindow", "Exit from the program"))
        self.actionExit.setShortcut(_translate("MainWindow", "Ctrl+W"))
    
    def closeProgram(self):
        app.quit()
    
    def loadImage(self):
        path_to_project, _ = QtWidgets.QFileDialog.getOpenFileName(None, "Choose image","", "Image Files (*.png *.jpg *.bmp)")
        if path_to_project!='':
            pixmap = QtGui.QPixmap(path_to_project)
            self.InputImage.setPixmap(pixmap)
            self.InputImage.setAlignment(QtCore.Qt.AlignLeft)
            self.MeasuredImage.setPixmap(pixmap)
            self.MeasuredImage.setAlignment(QtCore.Qt.AlignLeft)

if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())