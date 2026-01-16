
import os
import sys
import time

MACOS_HANG_TIME = 0.01

_platform = sys.platform

# Linux
if _platform == 'linux':
    from Xlib import X
    from Xlib.display import Display
    from Xlib.ext.xtest import fake_input

    display = Display(os.environ['DISPLAY'])

    def _linux_mouse_get_coord():
        pointer_data = display.screen().root.query_pointer()._data
        return (pointer_data['root_x'], pointer_data['root_y'])

    def _linux_mouse_move(x, y):
        fake_input(display, X.MotionNotify, x=x, y=y)
        display.sync()

    mouse_get_coord = _linux_mouse_get_coord
    mouse_move = _linux_mouse_move

# macOS
elif _platform == 'darwin':
    import Quartz
    import AppKit

    def _macos_mouse_get_coord():
        mouse_loc = AppKit.NSEvent.mouseLocation()
        return (int(mouse_loc.x), int(Quartz.CGDisplayPixelsHigh(0) - mouse_loc.y))

    def __macos_mouse_send_event(ev, x, y, button):
        mouse_event = Quartz.CGEventCreateMouseEvent(None, ev, (x, y), button)
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, mouse_event)

    def _macos_mouse_move(x, y):
        __macos_mouse_send_event(Quartz.kCGEventMouseMoved, x, y, 0)
        time.sleep(MACOS_HANG_TIME)

    mouse_get_coord = _macos_mouse_get_coord
    mouse_move = _macos_mouse_move


# Windows
elif _platform == 'win32':
    import ctypes

    class Cursor(ctypes.Structure):
        _fields_ = [('x', c_long), ('y', c_long)]

    def _windows_mouse_get_coord():
        c = Cursor()
        ctypes.windll.user32.GetCursorPos(ctypes.byref(c))
        return (c.x, c.y)

    def _windows_mouse_move(x, y):
        ctypes.windll.user32.SetCursorPos(x, y)

    mouse_get_coord = _windows_mouse_get_coord
    mouse_move = _windows_mouse_move