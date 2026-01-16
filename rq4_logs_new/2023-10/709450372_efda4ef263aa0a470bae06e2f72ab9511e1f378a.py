import numpy as np
import taichi as ti
from copy import copy

ti.init(arch=ti.cuda)


@ti.data_oriented
class Fractal:
    def __init__(self, width=1500, height=1000) -> None:
        self.width = width
        self.height = height
        self.res = (width, height)
        self.frame = ti.Vector.field(3, ti.f32, shape=self.res)
        self.window = ti.ui.Window('pygame', self.res, pos = ((1920-width)//2, (1080-height)//2))
        self.canvas = self.window.get_canvas()
        self.xscale = ti.Vector([-2, 1], ti.f64)
        self.yscale = ti.Vector([-1, 1], ti.f64)
        self.size_pixel = (self.xscale.y-self.xscale.x)/self.width
        self.offset_default = ti.Vector( [-0.5, 0.0], ti.f64)
        self.offset = copy(self.offset_default)
        self.max_iter = 20
    
    def events(self):
        if self.window.get_event( ti.ui.PRESS ):
            if self.window.event.key in ['q', 'Q']:
                exit()
            if self.window.event.key in ['r', 'R', ti.ui.RMB]:
                self.offset = self.offset_default
                self.xscale = ti.Vector([-2, 1])
                self.yscale = ti.Vector([-1, 1])
                self.size_pixel = (self.xscale.y-self.xscale.x)/self.width
                self.max_iter = 20
            
        if self.window.is_pressed(ti.ui.LMB):
            self.adjust_zoom(1)
            self.offset = self.adjust_offset()
            self.max_iter = -int(np.log10(self.size_pixel))*10

    
    def adjust_offset(self):
        return self.offset + 10*self.size_pixel*(ti.Vector(self.window.get_cursor_pos())-ti.Vector([0.5,0.5]))

    def adjust_zoom(self, factor=5):
        x = self.xscale.x +3*factor*self.size_pixel
        y = self.xscale.y -3*factor*self.size_pixel
        self.xscale -= self.xscale-ti.Vector([x, y])
        x = self.yscale.x +2*factor*self.size_pixel
        y = self.yscale.y -2*factor*self.size_pixel
        self.yscale -= self.yscale-ti.Vector([x, y])
        self.size_pixel = (self.xscale.y-self.xscale.x)/self.width

    
    @ti.func
    def calc_z(self, x: ti.u32, y: ti.u32, size_pixel: ti.f64, offset: ti.math.vec2, max_iter: ti.i16):
        cx = offset.x +size_pixel*(x-self.width//2)
        cy = offset.y +size_pixel*(y-self.height//2)
        c = ti.Vector([cx, cy], ti.f32)
        z = ti.Vector([0, 0], ti.f32)
        i = 0
        while i < max_iter and (z.x**2 +z.y**2) <= 4:
            z = ti.Vector([ (z.x**2 -z.y**2 +c.x), (2*z.x*z.y +c.y) ])
            i += 1
        col_i = i/max_iter
        color = ti.Vector([col_i, col_i, 1-col_i])
        return color
    
    @ti.kernel
    def update(self, size_pixel: ti.f32, offset: ti.math.vec2, max_iter: ti.i16):
        for x, y in self.frame:
            self.frame[x, y] = self.calc_z(x, y, size_pixel, offset, max_iter)
    
    def run(self):
        while self.window.running:
            self.events()
            self.update(self.size_pixel, self.offset, self.max_iter)
            self.canvas.set_image(self.frame)
            self.window.show()


if __name__ == '__main__':
    app = Fractal()
    app.run()