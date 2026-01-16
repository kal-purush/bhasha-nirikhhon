# text area in window
from PyQt4 import QtGui 
import sys
# in the above steps we import the required
app_obj  = QtGui.QApplication(sys.argv)	# here we create a application object for our pgm
window = QtGui.QWidget()
# here we create a  empty window using QtGui.QWidget()
window.setWindowTitle("Sample window")
#here we set the window title
window.resize(510,510)
# set window size
text = QtGui.QTextEdit(window)
# creates a Text Widget
text.resize(500,500)
text.move(5,5)
# set the size of Text Widget
window.show()
# to display our window 
sys.exit(app_obj.exec_())
#here we ENTER TO mainloop