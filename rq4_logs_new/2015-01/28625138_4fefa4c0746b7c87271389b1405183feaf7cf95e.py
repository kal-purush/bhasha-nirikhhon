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
window.setWindowIcon(QtGui.QIcon("icons/expeyes.png"))
window.show()
# to display our window 
sys.exit(app_obj.exec_())
#here we enter to the mainloop