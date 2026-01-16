
from PySide import QtGui, QtCore, QtOpenGL
from OpenGL import GL, GLU, GLUT 
import sys


class QGLContext( QtOpenGL.QGLWidget ):
    
    def __init__( self ):
        super( QGLContext, self ).__init__()
        GLUT.glutInit()
        GL.glEnable( GL.GL_DEPTH_TEST)
        GL.glClearColor( 0., 0., 0., 0. )
        # Enable Lighting
        GL.glLightfv( GL.GL_LIGHT0, GL.GL_POSITION, [6.0, 6.0, 6.0, 0.0] )
        GL.glLightfv( GL.GL_LIGHT0, GL.GL_DIFFUSE,  [1.0, 1.0, 1.0, 0.0] )
        # # GL.glLightfv( GL.GL_LIGHT0, GL.GL_SPECULAR, [1.0, 1.0, 1.0, 1.0] )
        # GL.glLightModelfv( GL.GL_LIGHT_MODEL_AMBIENT, [1.0, 1.0, 1.0, 0.0] )
        GL.glLightf( GL.GL_LIGHT0, GL.GL_LINEAR_ATTENUATION, 0.05 )
        GL.glLightf( GL.GL_LIGHT0, GL.GL_CONSTANT_ATTENUATION, 0.1 )
        GL.glEnable( GL.GL_LIGHTING )
        GL.glEnable( GL.GL_LIGHT0 )
        GL.glShadeModel( GL.GL_SMOOTH )
        GL.glEnable( GL.GL_CULL_FACE )
        self.pointCamera()

    def paintGL( self ):
        GL.glClear( GL.GL_COLOR_BUFFER_BIT | GL.GL_DEPTH_BUFFER_BIT )

        GL.glPushMatrix()
        GLU.gluLookAt( 0, 2.5, 7, 0,0,0, 0,1,0 )
        GL.glMaterialfv( GL.GL_FRONT, GL.GL_DIFFUSE, [0.3, 0.2, 0.6, 1.0] )
        GL.glFrontFace( GL.GL_CW )
        GLUT.glutSolidTeapot( 2.5 )
        GL.glFrontFace( GL.GL_CCW )
        GL.glPopMatrix()
        GL.glFlush()

    def resizeGL( self, w, h ):
        GL.glViewport( 0, 0, w, h )
        self.pointCamera()

    def pointCamera( self ):        
        GL.glMatrixMode( GL.GL_PROJECTION )
        GL.glLoadIdentity()
        GLU.gluPerspective( 60.0, self.width()/float(self.height()), .10, 100.0 )
        GL.glMatrixMode( GL.GL_MODELVIEW )
        GL.glLoadIdentity()


class Test( QtGui.QMainWindow ):
    def __init__( self, parent_app ):
        super( Test, self ).__init__()
        self._parent_app = parent_app
        self._ctx = QGLContext()
        self._parent_app.setApplicationName( "Testing GL" )
        self.setCentralWidget( self._ctx )
        self.resize( 720, 405 )
        self.show()
        self._parent_app.exec_()

if __name__=="__main__":
    app = QtGui.QApplication( sys.argv )
    gl = Test( app )