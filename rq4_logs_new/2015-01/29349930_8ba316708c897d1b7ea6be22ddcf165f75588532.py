# Copyright 2015 Broad Institute, all rights reserved.

import javabridge
import matplotlib
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.backend_bases import \
     FigureManagerBase, NavigationToolbar2
import numpy as np

class FigureCanvasSwing(FigureCanvasAgg):
    def __init__(self, figure):
        FigureCanvasAgg.__init__(self, figure)
        self.__ref_id, self.__ref = javabridge.create_jref(self)
        self.__cpython = javabridge.make_instance(
            'org/cellprofiler/javabridge/CPython', '()V')

        paint_script = (
            'import javabridge\n'
            'self = javabridge.redeem_jref("%s")\n'
            'self.draw(javabridge.JWrapper(graphics))\n') % self.__ref_id
        
        component = javabridge.run_script("""
        new javax.swing.JComponent() {
            paintComponent: function(graphics) {
                locals = new java.util.Hashtable();
                locals.put("graphics", graphics);
                cpython.exec(script, locals, locals);
            }
        }
        """, dict(cpython = self.__cpython, script = paint_script))
        self.__component = javabridge.JWrapper(component)
        self.__event_queue_class = None
        on_size_script = (
            'import javabridge\n'
            'self = javabridge.redeem_jref("%s")\n'
            'self._on_size(event)') % self.__ref_id
        self.__component_adapter = javabridge.run_script("""
        new java.awt.event.ComponentAdapter() {
            componentResized: function(event) {
                locals = new java.util.Hashtable();
                locals.put("event", event);
                cpython.exec(script, locals, locals);
            }
        }
        """, dict(cpython = self.__cpython, script = on_size_script))
        self.__component.addComponentListener(self.__component_adapter)
           
    @property
    def component(self):
        return self.__component
    
    def draw(self, graphics=None):
        '''Render the figure
        
        :param graphics: the AWT Graphics gc that should be used to draw.
                         If None and not in the AWT thread, the call is
                         reflected via EventQueue.invokeAndWait, otherwise
                         a graphics context is created and drawn.
        '''
        FigureCanvasAgg.draw(self)
        self.jimage = _convert_agg_to_awt_image(self.get_renderer(), None)
        self._isDrawn = True
        self.gui_repaint(graphics)
        
    def gui_repaint(self, graphics = None):
        '''Do the actual painting on the Java canvas'''
        if not self._eqc.isDispatchThread():
            self.__component.repaint()
        else:
            if self.__component.isShowing():
                color = javabridge.JClassWrapper(
                    'java.awt.Color')(*self.figure.get_facecolor())
                if graphics is None:
                    graphics = self.__component.getGraphics()
                graphics.drawImage(self.jimage, 0, 0, color, None)

    def blit(self, bbox=None):
        raise NotImplementedError("TODO: implement blit")
    
    def _on_size(self, event):
        w = float(self.__component.getWidth())
        h = float(self.__component.getHeight())
        if w <= 0 or h <= 0:
            return
        dpival = self.figure.dpi
        winch = w/dpival
        hinch = h/dpival
        self.figure.set_size_inches(winch, hinch)
        self._isDrawn = False
        self.__component.repaint()
        FigureCanvasAgg.resize_event(self)
        
    
    @property
    def _eqc(self):
        '''java.awt.EventQueue.class'''
        if self.__event_queue_class is None:
            self.__event_queue_class = javabridge.JClassWrapper(
                "java.awt.EventQueue")
        return self.__event_queue_class
        
def _convert_agg_to_awt_image(renderer, awt_image):
    '''Use the renderer to draw the figure on a java.awt.BufferedImage
    
    :param renderer: the RendererAgg that's in charge of rendering the figure
    
    :param awt_image: a javabridge JB_Object holding the BufferedImage or
                      None to create one.
    
    :returns: the BufferedImage
    '''
    env = javabridge.get_env()
    w = int(renderer.width)
    h = int(renderer.height)
    buf = np.frombuffer(renderer.buffer_rgba(), np.uint8).reshape(w * h, 4)
    cm = javabridge.JClassWrapper('java.awt.image.DirectColorModel')(
        32, int(0x000000FF), int(0x0000FF00), int(0x00FF0000), -16777216)
    raster = cm.createCompatibleWritableRaster(w, h)
    for i in range(4):
        samples = env.make_int_array(buf[:, i].astype(np.int32))
        raster.setSamples(0, 0, w, h, i, samples)
                          
    awt_image = javabridge.JClassWrapper('java.awt.image.BufferedImage')(
        cm, raster, False, None)
    return awt_image