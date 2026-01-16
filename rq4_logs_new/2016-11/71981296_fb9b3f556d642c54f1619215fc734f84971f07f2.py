import sys
from caf_plot_gui import controller
from caf_plot_gui import model 
from PyQt5 import QtGui, QtWidgets

def usage():
    print("caf_plot_gui CONFIG_FILE")

def main():
    if len(sys.argv) != 2:
        usage()
        return
    app = QtWidgets.QApplication(sys.argv)
    form = controller.controller(model.model(sys.argv[1]))
    form.show()
    app.exec_()

if __name__ == '__main__':
    main()