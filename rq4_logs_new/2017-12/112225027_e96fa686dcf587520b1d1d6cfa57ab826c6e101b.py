from PyQt4 import QtCore, QtGui

try:
    _encoding = QtGui.QApplication.UnicodeUTF8
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig)

class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(1000, 700)
        self.gridLayout_2 = QtGui.QGridLayout(Dialog)
        self.gridLayout_2.setObjectName("gridLayout_2")
        self.grp = QtGui.QGroupBox(Dialog)
        self.grp.setTitle("")
        self.grp.setObjectName("grp")
        self.horizontalLayout = QtGui.QHBoxLayout(self.grp)
        self.horizontalLayout.setObjectName("horizontalLayout")
        
        self.btn_start = QtGui.QPushButton(self.grp)
        self.btn_start.setObjectName("btn_start")
        self.horizontalLayout.addWidget(self.btn_start)
        
        
        self.btn_stop = QtGui.QPushButton(self.grp)
        self.btn_stop.setObjectName("btn_stop")
        self.horizontalLayout.addWidget(self.btn_stop)
        self.btn_stop.setEnabled(False)
        
        self.btn_save = QtGui.QPushButton(self.grp)
        self.btn_save.setObjectName("btn_save")
        self.horizontalLayout.addWidget(self.btn_save)

                        
        self.gridLayout_2.addWidget(self.grp, 0, 1, 1, 1)
        self.scrl = QtGui.QScrollArea(Dialog)
        self.scrl.setWidgetResizable(True)
        self.scrl.setObjectName("scrl")
        self.scrollAreaWidgetContents_3 = QtGui.QWidget()
        self.scrollAreaWidgetContents_3.setGeometry(QtCore.QRect(0, 0, 580, 385))
        self.scrollAreaWidgetContents_3.setObjectName("scrollAreaWidgetContents_3")
        self.verticalLayout_2 = QtGui.QVBoxLayout(self.scrollAreaWidgetContents_3)
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        
        self.tbl = QtGui.QTableWidget(self.scrollAreaWidgetContents_3)
        self.tbl.setObjectName("tbl")
        self.tbl.setColumnCount(5)
        self.tbl.setRowCount(0)
        self.verticalLayout_2.addWidget(self.tbl)
        #self.tbl.cellClicked.connect(self.handleCellClicked)
        self.tbl.setHorizontalHeaderLabels(("Time","Source","Destination","Protocol","Info"))
        self.tbl.horizontalHeader().setResizeMode(4, QtGui.QHeaderView.Stretch)
        
        self.txt = QtGui.QTextBrowser(self.scrollAreaWidgetContents_3)
        self.txt.setObjectName("txt")
        self.verticalLayout_2.addWidget(self.txt)
        
        self.hexa = QtGui.QTextBrowser(self.scrollAreaWidgetContents_3)
        self.hexa.setObjectName("hexa")
        self.verticalLayout_2.addWidget(self.hexa)
        
        self.scrl.setWidget(self.scrollAreaWidgetContents_3)
        self.gridLayout_2.addWidget(self.scrl, 3, 1, 1, 1)
        self.grp_fltr = QtGui.QGroupBox(Dialog)
        self.grp_fltr.setTitle("")
        self.grp_fltr.setObjectName("grp_fltr")
        self.horizontalLayout_2 = QtGui.QHBoxLayout(self.grp_fltr)
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")

        self.lbl_iface = QtGui.QLabel(self.grp_fltr)
        self.lbl_iface.setObjectName("lbl_iface")
        self.horizontalLayout_2.addWidget(self.lbl_iface)
        
        self.combo = QtGui.QComboBox(self.grp_fltr)
        self.combo.setObjectName("combo")
        self.horizontalLayout_2.addWidget(self.combo)
        
        self.lbl_fltr = QtGui.QLabel(self.grp_fltr)
        self.lbl_fltr.setObjectName("lbl_fltr")
        self.horizontalLayout_2.addWidget(self.lbl_fltr)
        
        self.in_fltr = QtGui.QLineEdit(self.grp_fltr)
        self.in_fltr.setObjectName("in_fltr")
        self.horizontalLayout_2.addWidget(self.in_fltr)
        
        self.btn_apply = QtGui.QPushButton(self.grp_fltr)
        self.btn_apply.setObjectName("btn_apply")
        self.horizontalLayout_2.addWidget(self.btn_apply)

        self.gridLayout_2.addWidget(self.grp_fltr, 1, 1, 2, 1)

        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)
        

        
        
    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(_translate("Dialog", "Network Sniffer", None))
        self.btn_start.setText(_translate("Dialog", "Start", None))
        self.btn_stop.setText(_translate("Dialog", "Stop", None))
        self.btn_save.setText(_translate("Dialog", "Save", None))
        self.lbl_fltr.setText(_translate("Dialog", "Filter", None))
        self.lbl_iface.setText(_translate("Dialog", "Interface", None))
        self.btn_apply.setText(_translate("Dialog", "Apply", None))