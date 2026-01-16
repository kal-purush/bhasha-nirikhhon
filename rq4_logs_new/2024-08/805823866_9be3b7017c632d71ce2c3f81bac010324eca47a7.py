import sys
import hashlib
from pathlib import Path
from PyQt6.QtWidgets import (QWidget, QLabel, QLineEdit, QGridLayout, 
                             QApplication, QPushButton, QFileDialog)

class FileHash(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
    
    def initUI(self):
        #create window with  labels
        FileOpen1 = QLabel('Open First File: ')
        FileHash1 = QLabel('First File Hash: ')
        #create window with 2nd row of labels
        FileOpen2 = QLabel('Open Second File: ')
        FileHash2 = QLabel('Second File Hash: ')
        FileHashResult = QLabel('Hash Result: ')
        
        #create first two line edits
        self.OpenEdit1 = QLineEdit()
        self.HashEdit1 = QLineEdit()
        #create next three line edits
        self.OpenEdit2 = QLineEdit()
        self.HashEdit2 = QLineEdit()
        self.HashResult = QLineEdit()
        #create buttons and action if clicked
        open_button1 = QPushButton(parent=self, text="Open First File")
        open_button1.clicked.connect(self.open_dialog_1)

        open_button2 = QPushButton(parent=self, text="Open Second File")
        open_button2.clicked.connect(self.open_dialog_2)

        open_button3 = QPushButton(parent=self, text="Compare File Hash")
        open_button3.clicked.connect(self.compareHash)

        #create grid layout
        grid = QGridLayout()
        #put the label and line edit for file
        grid.addWidget(FileOpen1, 0, 0)
        grid.addWidget(self.OpenEdit1, 0, 1)
        #put the label and line edit for hash
        grid.addWidget(FileHash1, 1, 0)
        grid.addWidget(self.HashEdit1, 1, 1)
        #put the button here
        grid.addWidget(open_button1, 0, 2)

        grid.addWidget(FileOpen2, 2, 0)
        grid.addWidget(self.OpenEdit2, 2, 1)
        #put the label and line edit for hash
        grid.addWidget(FileHash2, 3, 0)
        grid.addWidget(self.HashEdit2, 3, 1)
        #put the 2nd button here
        grid.addWidget(open_button2, 2, 2)

        grid.addWidget(FileHashResult, 4, 0)
        grid.addWidget(self.HashResult, 4, 1)
        grid.addWidget(open_button3, 4, 2)

        self.setLayout(grid)
        self.setFixedSize(650, 225)
        self.setWindowTitle('File Hash Demo')
        self.show()

    def open_dialog_1(self):
        fname, ok = QFileDialog.getOpenFileName(
            self,
            "Open File",
            "${HOME}",
            "All Files (*);; Text Files (*.txt);; PNG Files (*.png)",
        )
        if fname:
            path = Path(fname)
            self.OpenEdit1.setText(str(path))
        #get file hash
        self.md5_hash = hashlib.md5()
        with open(fname, "rb") as f:
            for byte_block in iter(lambda: f.read(4096),b''):
                self.md5_hash.update(byte_block)
                self.HashEdit1.setText(self.md5_hash.hexdigest())
    
    def open_dialog_2(self):
        fname, ok = QFileDialog.getOpenFileName(
            self,
            "Open File",
            "${HOME}",
            "All Files (*);; Text Files (*.txt);; PNG Files (*.png)",
        )
        if fname:
            path = Path(fname)
            self.OpenEdit2.setText(str(path))
        #get file hash
        self.md5_hash = hashlib.md5()
        with open(fname, "rb") as f:
            for byte_block in iter(lambda: f.read(4096),b''):
                self.md5_hash.update(byte_block)
                self.HashEdit2.setText(self.md5_hash.hexdigest())

    def compareHash(self):
        hash_a = self.HashEdit1.text()
        hash_b = self.HashEdit2.text()

        if hash_a == hash_b:
            self.HashResult.clear() 
            self.HashResult.setText("Hashes Match")
        else:
            self.HashResult.clear() 
            self.HashResult.setText("Hashes Don't Match")

def main():
    app = QApplication(sys.argv)
    FHash = FileHash()
    sys.exit(app.exec())

if __name__=='__main__':
    main()