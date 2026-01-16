import sys
from PyQt5.QtWidgets import *
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import pyqtSlot
from calculator import calcul

class App(QMainWindow):

    def __init__(self):
        super().__init__()
        self.title = 'PyQt5 textbox - pythonspot.com'
        self.left = 1500
        self.top = 700
        self.width = 500
        self.height = 500
        self.initUI()
    
    def initUI(self):
        self.setWindowTitle(self.title)
        self.setGeometry(self.left, self.top, self.width, self.height)

        # get variable
        self.v_label = QLabel(self)
        self.v_label.move(20, 10)
        self.v_label.setText('Variable')
    
        self.variable = QLineEdit(self)
        self.variable.move(20, 50)
        self.variable.resize(300, 30)

        # get equation
        self.e_label = QLabel(self)
        self.e_label.move(20, 90)
        self.e_label.resize(110, 30)
        self.e_label.setText('Equation')

        self.equation = QLineEdit(self)
        self.equation.move(20, 130)
        self.equation.resize(300, 30)

        # show equation
        self.a_label = QLabel(self)
        self.a_label.move(20, 170)
        self.a_label.resize(300, 30)

        # differential
        self.d_label = QLabel(self)
        self.d_label.move(20, 230)
        self.d_label.resize(300, 30)

        self.da_label = QLabel(self)
        self.da_label.move(50, 240)
        self.da_label.resize(300, 100)
        
        # Create a button in the window
        self.button = QPushButton('click', self)
        self.button.move(330, 130)
        
        # connect button to function on_click
        self.button.clicked.connect(self.on_click)
        self.show()
    
    def on_click(self):
        temp = self.variable.text()
        eq = self.equation.text()

        var_list = []
        temp = temp.split(',')

        if temp[0] != '':
            for n in range(len(temp)):
                var = temp[n].split(' ')
                # if var[-1] in ['e', 'pi']:
                #     raise Exception("%s is not available" % (var[-1]))
                var_list.append(var[-1])

        ans, diff = calcul(eq, var_list)

        self.a_label.setText('Equation is: ' + ans)
        self.d_label.setText('Differential')

        diff_ans = ''
        for n in range(len(diff)):
            diff_ans = diff_ans + 'Differentiated by ' + var_list[n] + ': ' + diff[n] + '\n'

        self.da_label.setText(diff_ans)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = App()
    sys.exit(app.exec_())