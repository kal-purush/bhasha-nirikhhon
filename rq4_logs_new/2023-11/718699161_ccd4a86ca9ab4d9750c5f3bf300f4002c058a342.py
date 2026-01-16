import pygame as pg
from OpenGL.GL import *

class App:

    def __init__(self):
        #инициализация pygame
        pg.init()
        pg.display.set_mode((640, 480), pg.OPENGL|pg.DOUBLEBUF) #создание окна: размеры окна, использование opengl, двойная буфферизация - один буфер отрисовывается, в то время как второй виден на экране
        self.clock = pg.time.Clock()

        #инициализация opengl
        glClearColor(0.1, 0.2, 0.2, 1)  #цвет фона/очистки

        self.mainLoop()

    def mainLoop(self):
        running = True

        while(running):
            #проверка событий
            for event in pg.event.get():
                if event.type == pg.QUIT:
                    running = False

            #обновление экрана
            glClear(GL_COLOR_BUFFER_BIT)    #очистка экрана
            pg.display.flip()

            self.clock.tick(60)
        self.quit() #выход из приложения

    def quit(self):
        pg.quit()

if __name__ == "__main__":
    myApp = App()