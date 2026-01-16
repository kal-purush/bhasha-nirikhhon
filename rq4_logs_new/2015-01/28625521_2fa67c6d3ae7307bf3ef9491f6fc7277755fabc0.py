from PyQt4 import QtGui 
import sys
# in the above steps we import the required
app_obj  = QtGui.QApplication(sys.argv)	# here we create a application object for our pgm
window = QtGui.QWidget()
# here we create a  empty window using QtGui.QWidget()
window.setWindowTitle("Sample window")
#here we set the window title
window.resize(300,400)
#here we set the size of the window .
button_print = QtGui.QPushButton("Print",window)
# here we create a PushButton named button_print
button_print.move(100,20)
#here we set the position of button
window.setWindowIcon(QtGui.QIcon("icons/expeyes.png"))
# here we set the window icon
window.show()
# to display our window 
sys.exit(app_obj.exec_())
#here we 