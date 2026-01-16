from mainwindow_ui import *
from new_ui import Ui_Dialog
import json

class SecondWindow(QtWidgets.QDialog, Ui_Dialog):
	def __init__(self, name='', ext=''):
		QtWidgets.QDialog.__init__(self)
		self.setupUi(self)

		self.nameEdit.setText(name)
		self.extEdit.setText(ext)

		self.cancelButton.clicked.connect(self.hide)
		self.nameEdit.returnPressed.connect(self.extEdit.setFocus)
		self.extEdit.returnPressed.connect(self.accept)

	def accept(self):
		pass

class MainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
	def __init__(self):
		QtWidgets.QMainWindow.__init__(self)
		self.setupUi(self)

		with open("data.json") as file:
			self.data = json.load(file)
			file.close()

		self.actionNew_filter.triggered.connect(self.new)
		self.actionEdit_selected_filter.triggered.connect(self.edit)

	def new(self):
		self.new = SecondWindow()
		self.new.show()

	def edit(self):
		self.edit = SecondWindow(name=name, ext=ext)
		self.edit.show()

if __name__ == '__main__':
	app = QtWidgets.QApplication([])
	window = MainWindow()
	window.show()
	app.exec_()