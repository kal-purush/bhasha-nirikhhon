import matplotlib

# matplotlib.use('QT4Agg')
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import axes3d
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d.axes3d import Axes3D, get_test_data
import numpy as np


class MyAxes3DZ(axes3d.Axes3D):
    
    def __init__(self, baseObject, sides_to_draw):
        self.__class__ = type(baseObject.__class__.__name__,
                              (self.__class__, baseObject.__class__),
                              {})
        self.__dict__ = baseObject.__dict__
        self.sides_to_draw = list(sides_to_draw)
        self.mouse_init()
    
    def set_some_features_visibility(self, visible):
        for t in self.w_zaxis.get_ticklines() + self.w_zaxis.get_ticklabels():
            t.set_visible(visible)
        self.w_zaxis.line.set_visible(visible)
        self.w_zaxis.pane.set_visible(visible)
        self.w_zaxis.label.set_visible(visible)
    
    def draw(self, renderer):
        # set visibility of some features False
        self.set_some_features_visibility(False)
        # draw the axes
        super(MyAxes3DZ, self).draw(renderer)
        # set visibility of some features True.
        # This could be adapted to set your features to desired visibility,
        # e.g. storing the previous values and restoring the values
        self.set_some_features_visibility(True)
        
        zaxis = self.zaxis
        draw_grid_old = zaxis.axes._draw_grid
        # disable draw grid
        zaxis.axes._draw_grid = False
        
        tmp_planes = zaxis._PLANES
        
        if 'l' in self.sides_to_draw:
            # draw zaxis on the left side
            zaxis._PLANES = (tmp_planes[2], tmp_planes[3],
                             tmp_planes[0], tmp_planes[1],
                             tmp_planes[5], tmp_planes[4])
            zaxis.draw(renderer)
        if 'r' in self.sides_to_draw:
            # draw zaxis on the right side
            zaxis._PLANES = (tmp_planes[3], tmp_planes[2],
                             tmp_planes[1], tmp_planes[0],
                             tmp_planes[4], tmp_planes[5])
            zaxis.draw(renderer)
        
        zaxis._PLANES = tmp_planes
        
        # disable draw grid
        zaxis.axes._draw_grid = draw_grid_old


class MyAxes3DX(axes3d.Axes3D):
    
    def __init__(self, baseObject, sides_to_draw):
        self.__class__ = type(baseObject.__class__.__name__,
                              (self.__class__, baseObject.__class__),
                              {})
        self.__dict__ = baseObject.__dict__
        self.sides_to_draw = list(sides_to_draw)
        self.mouse_init()
    
    def set_some_features_visibility(self, visible):
        for t in self.w_zaxis.get_ticklines() + self.w_zaxis.get_ticklabels():
            t.set_visible(visible)
        self.w_zaxis.line.set_visible(visible)
        self.w_zaxis.pane.set_visible(visible)
        self.w_zaxis.label.set_visible(visible)
    
    def draw(self, renderer):
        # set visibility of some features False
        self.set_some_features_visibility(False)
        # draw the axes
        super(MyAxes3DX, self).draw(renderer)
        # set visibility of some features True.
        # This could be adapted to set your features to desired visibility,
        # e.g. storing the previous values and restoring the values
        self.set_some_features_visibility(True)
        
        xaxis = self.xaxis
        draw_grid_old = xaxis.axes._draw_grid
        # disable draw grid
        xaxis.axes._draw_grid = False
        
        tmp_planes = xaxis._PLANES
        
        if 'l' in self.sides_to_draw:
            #print(123)
            # draw zaxis on the left side
            xaxis._PLANES = (tmp_planes[5], tmp_planes[4],
                             tmp_planes[2], tmp_planes[3],
                             tmp_planes[0], tmp_planes[1])
            xaxis.draw(renderer)
        if 'r' in self.sides_to_draw:
            #print(456)
            # draw zaxis on the right side
            xaxis._PLANES = (tmp_planes[1], tmp_planes[0],
                             tmp_planes[3], tmp_planes[2],
                             tmp_planes[5], tmp_planes[4])
            xaxis.draw(renderer)
        
        xaxis._PLANES = tmp_planes
        
        # disable draw grid
        xaxis.axes._draw_grid = draw_grid_old


class MyAxes3DY(axes3d.Axes3D):
    
    def __init__(self, baseObject, sides_to_draw):
        self.__class__ = type(baseObject.__class__.__name__,
                              (self.__class__, baseObject.__class__),
                              {})
        self.__dict__ = baseObject.__dict__
        self.sides_to_draw = list(sides_to_draw)
        self.mouse_init()
    
    def set_some_features_visibility(self, visible):
        for t in self.w_zaxis.get_ticklines() + self.w_zaxis.get_ticklabels():
            t.set_visible(visible)
        self.w_zaxis.line.set_visible(visible)
        self.w_zaxis.pane.set_visible(visible)
        self.w_zaxis.label.set_visible(visible)
    
    def draw(self, renderer):
        # set visibility of some features False
        self.set_some_features_visibility(False)
        # draw the axes
        super(MyAxes3DY, self).draw(renderer)
        # set visibility of some features True.
        # This could be adapted to set your features to desired visibility,
        # e.g. storing the previous values and restoring the values
        self.set_some_features_visibility(True)
        
        yaxis = self.yaxis
        draw_grid_old = yaxis.axes._draw_grid
        # disable draw grid
        yaxis.axes._draw_grid = False
        
        tmp_planes = yaxis._PLANES
        
        if 'l' in self.sides_to_draw:
            print(123)
            # draw zaxis on the left side
            yaxis._PLANES = (tmp_planes[5], tmp_planes[4],
                             tmp_planes[2], tmp_planes[3],
                             tmp_planes[0], tmp_planes[1])
            yaxis.draw(renderer)
        if 'r' in self.sides_to_draw:
            print(456)
            # draw zaxis on the right side
            yaxis._PLANES = (tmp_planes[4], tmp_planes[5],
                             tmp_planes[2], tmp_planes[3],
                             tmp_planes[0], tmp_planes[1])
            yaxis.draw(renderer)

        yaxis._PLANES = tmp_planes
        
        # disable draw grid
        yaxis.axes._draw_grid = draw_grid_old

def example_surface(ax):
    """ draw an example surface. code borrowed from http://matplotlib.org/examples/mplot3d/surface3d_demo.html """
    from matplotlib import cm
    import numpy as np
    X = np.arange(-5, 5, 0.25)
    Y = np.arange(-5, 5, 0.25)
    X, Y = np.meshgrid(X, Y)
    R = np.sqrt(X ** 2 + Y ** 2)
    Z = np.sin(R)
    surf = ax.plot_surface(X, Y, Z, rstride = 1, cstride = 1, cmap = cm.coolwarm, linewidth = 0, antialiased = False)


if __name__ == '__main__':
    fig = plt.figure()
    # ax = fig.add_subplot(131, projection = '3d')
    # ax.set_title('z-axis left side')
    # ax = fig.add_axes(MyAxes3DY(ax, 'l'))
    # example_surface(ax)  # draw an example surface
    # ax = fig.add_subplot(132, projection = '3d')
    # ax.set_title('z-axis both sides')
    # ax = fig.add_axes(MyAxes3DY(ax, 'lr'))
    # example_surface(ax)  # draw an example surface
    
    #x
    ax = fig.add_subplot(3,5,1, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)

    ax = fig.add_subplot(3,5,2, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.xaxis.set_draw_side('lower_front')
    
    ax = fig.add_subplot(3,5,3, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.xaxis.set_draw_side('upper_front')

    ax = fig.add_subplot(3,5,4, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.xaxis.set_draw_side('lower_back')

    ax = fig.add_subplot(3,5,5, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.xaxis.set_draw_side('upper_back')
    #y
    ax = fig.add_subplot(3,5,6, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)

    ax = fig.add_subplot(3,5,7, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.yaxis.set_draw_side('lower_left')

    ax = fig.add_subplot(3,5,8, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.yaxis.set_draw_side('lower_right')

    ax = fig.add_subplot(3,5,9, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.yaxis.set_draw_side('upper_left')

    ax = fig.add_subplot(3,5,10, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.yaxis.set_draw_side('upper_right')

    #z

    ax = fig.add_subplot(3, 5, 11, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)

    ax = fig.add_subplot(3, 5, 12, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.zaxis.set_draw_side('left')

    ax = fig.add_subplot(3, 5, 13, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.zaxis.set_draw_side('right')

    ax = fig.add_subplot(3, 5, 14, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.zaxis.set_draw_side('front')

    ax = fig.add_subplot(3, 5, 15, projection = '3d')
    X, Y, Z = get_test_data(0.05)
    ax.plot_wireframe(X, Y, Z, rstride = 10, cstride = 10)
    ax.zaxis.set_draw_side('back')

    plt.show()

"""

It does not make sense to squuze the texts and the images so we delivered a new feature
so the user can select where the ax can be placed. Also because the nature of 3D,
one cannot guarantee axis is always at a particular position.

There are 2 axes that cannot be killed due to if I kill them all the tests will fail.

Other than that the fix should not break anything.

A extended X/Y/ZAxis was implemented and would remember the preferred positions.
and a extra rendering would be conducted if other positions exists.

"""