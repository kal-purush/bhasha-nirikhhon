# from interfaces.mainwindow_ui import *
# from interfaces.new_ui import Ui_Dialog
# from interfaces.folder_ui import Ui_Dialog as Ui_Folder
import json
import sys
from utils.organize import App
import multiprocessing
from PyQt5 import QtWidgets, QtGui, QtCore
from PyQt5.QtWidgets import QMessageBox
from PyQt5 import uic
import py_resources
from constants import *

class FolderWindow(QtWidgets.QDialog):
	folder_path = Path('resources/folder')
	def __init__(self):
		QtWidgets.QDialog.__init__(self)
		uic.loadUi(interfaces / 'folder.ui', self)

		self.cancelButton.clicked.connect(self.hide)
		self.acceptButton.clicked.connect(self.accept)
		self.btn_Browse.clicked.connect(self.browse)

		with open(self.folder_path, 'r') as file:
			self.lineEdit.setText(file.read())
			file.close()

	def accept(self):
		with open(self.folder_path, 'w') as file:
			file.write(self.lineEdit.text())
			file.close()
		self.hide()

	def browse(self):
		fileName = QtWidgets.QFileDialog.getExistingDirectory(self, 'Select directory')
		if fileName:
			self.lineEdit.setText(fileName)

class SecondWindow(QtWidgets.QDialog):
	def __init__(self, mainw, mode, c=''):
		QtWidgets.QDialog.__init__(self)
		uic.loadUi(interfaces / 'new.ui', self)

		self.mode = mode
		self.mainw = mainw
		self.id = c

		self.mainw.stop_program()

		self.cancelButton.clicked.connect(self.hide)
		self.acceptButton.clicked.connect(self.accept)
		self.nameEdit.returnPressed.connect(self.extEdit.setFocus)
		self.extEdit.returnPressed.connect(self.accept)

		if self.mode == 'edit':
			self.nameEdit.setText(self.mainw.data[self.id]['name'])
			self.extEdit.setText('; '.join(self.mainw.data[self.id]['ext']) + ';')

	def accept(self):
		if self.mode == 'new':
			self.mainw.data.append({'name': self.nameEdit.text(), 'ext': list(filter(lambda x: x != '' and x != ' ', self.extEdit.text().replace(' ', '').split(';')))})
			self.mainw.set_info()
			self.mainw.get_info()
			self.hide()

		elif self.mode == 'edit':
			self.mainw.data[self.id]['name'] = self.nameEdit.text()
			self.mainw.data[self.id]['ext'] = list(filter(lambda x: x != '' and x != ' ', self.extEdit.text().replace(' ', '').split(';')))
			self.mainw.set_info()
			self.mainw.get_info()
			self.hide()


class MainWindow(QtWidgets.QMainWindow):
	data_path = Path('resources/data.json')
	def __init__(self):
		QtWidgets.QMainWindow.__init__(self)
		uic.loadUi(interfaces / 'mainwindow.ui', self)

		self.organize_app = App()

		self.tree.setColumnWidth(0, 0)

		self.actionNewFilter.triggered.connect(self.new)
		self.actionEditFilter.triggered.connect(self.edit)
		self.actionDeleteFilter.triggered.connect(self.delete)

		self.actionSetFolder.triggered.connect(self.set_folder)
		self.actionStartProgram.triggered.connect(self.start_program)
		self.actionStopProgram.triggered.connect(self.stop_program)

		self.actionDestroy.triggered.connect(self.confirm_destroy)
		self.actionExit.triggered.connect(self.hide)

		self.get_info()

	def confirm_destroy(self):
		q = QMessageBox.question(self, 'Destroy program', "Are you sure?", QMessageBox.Yes | QMessageBox.No)
		if q == QMessageBox.Yes:
			self.stop_program()
			sys.exit()

	def closeEvent(self, event):
		self.hide()

	def new(self):
		self.new = SecondWindow(self, 'new')
		self.new.show()

	def edit(self):
		item = self.tree.currentItem()
		self.edit = SecondWindow(self, 'edit', int(item.text(0)))
		self.edit.show()

	def delete(self):
		item = self.tree.currentItem()
		del self.data[int(item.text(0))]
		self.set_info()
		self.get_info()

	def set_folder(self):
		self.set = FolderWindow()
		self.set.show()

	def stop_program(self):
		try:
			self.main_thread.terminate()
		except AttributeError:
			pass

	def start_program(self):
		self.stop_program()
		self.main_thread = multiprocessing.Process(target=self.organize_app.loop)
		self.main_thread.start()

	def get_info(self):
		with open(self.data_path, 'r') as file:
			self.data = json.load(file)
			self.tree.clear()
			for i in range(len(self.data)):
				item = QtWidgets.QTreeWidgetItem()
				item.setText(0, str(i))
				item.setText(1, self.data[i]['name'])
				item.setText(2, '; '.join(self.data[i]['ext']) + ';')
				self.tree.addTopLevelItem(item)
			file.close()

	def set_info(self):
		with open(self.data_path, 'w') as file:
			file.write(json.dumps(self.data))
			file.close()

def main():
	app = QtWidgets.QApplication([])
	window = MainWindow()

	app.setQuitOnLastWindowClosed(False)

	# Create the icon
	icon = QtGui.QIcon(":/images/icono")

	# Create the tray
	tray = QtWidgets.QSystemTrayIcon()
	tray.setIcon(icon)
	tray.setVisible(True)

	# Create the menu
	menu = QtWidgets.QMenu()
	action = QtWidgets.QAction("Open")
	action.triggered.connect(window.show)
	menu.addAction(action)

	# Add the menu to the tray
	tray.setContextMenu(menu)

	window.show()

	sys.exit(app.exec_())

if __name__ == '__main__':
	main()