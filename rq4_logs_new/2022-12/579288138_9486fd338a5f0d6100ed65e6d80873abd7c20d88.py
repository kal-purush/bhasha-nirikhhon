import pyxel

WINDOW_WIDTH = 256
WINDOW_HEIGHT = 256


class App:
    def __init__(self):
        pyxel.init(WINDOW_WIDTH, WINDOW_HEIGHT)
        pyxel.load('./assets/my_resource.pyxres')
        self.rabbit = Rabbit()
        pyxel.run(self.update, self.draw)

    def update(self):
        self.rabbit.update()

    def draw(self):
        pyxel.cls(0)
        self.rabbit.draw()


class Rabbit:
    INIT_START_X = 20
    INIT_START_Y = WINDOW_HEIGHT // 2

    RUN_U = 0
    RUN_V = 16
    RUN_W = 48
    RUN_H = 32

    def __init__(self):
        self.x = self.INIT_START_X
        self.y = self.INIT_START_Y

    def update(self):
        key_input = 0
        if pyxel.btn(pyxel.KEY_W):
            key_input -= 1
        if pyxel.btn(pyxel.KEY_S):
            key_input += 1
        self.y += self.RUN_H * key_input

    def draw(self):
        pyxel.blt(self.x, self.y, 0, self.RUN_U, self.RUN_V, self.RUN_W, self.RUN_H, 0)




App()