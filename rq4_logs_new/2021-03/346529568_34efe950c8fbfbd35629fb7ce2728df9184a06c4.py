from OpenGL.GL import *
from glew_wish import *
import glfw
from math import *

class Bala:
    desfase = 90
    velocidad = 1.0
    posicionX = 0.0
    posicionY = 0.0
    anguloBala = 0.0

    def actualizar(self, tiempo_delta):
        
        self.posicionY = self.posicionY + \
            (sin((self.anguloBala + self.desfase) * 3.14159 / 180) * self.velocidad * tiempo_delta)
        self.posicionX = self.posicionX + \
            (cos((self.anguloBala + self.desfase) * 3.14159 / 180) * self.velocidad * tiempo_delta)

    def dibujar(self):
        glPushMatrix()
        glTranslate(self.posicionX, self.posicionY, 0.0)
        glRotate(self.anguloBala, 0.0, 0.0, 1.0)
        glBegin(GL_QUADS)
        glColor3f(1.0, 1.0, 1.0)
        glVertex3f(-0.01, 0.01, 0.0)
        glVertex3f(0.01, 0.01, 0.0)
        glVertex3f(0.01, -0.01, 0.0)
        glVertex3f(-0.01, -0.01, 0.0)
        glEnd()
        glPopMatrix()