from Cocoa import *
from Foundation import *
from PyObjCTools import AppHelper



class AppDelegate(NSObject):
    def applicationDidFinishLaunching_(self, aNotification):
        NSEvent.addGlobalMonitorForEventsMatchingMask_handler_(NSMouseMovedMask, self.mouseMoved)
        self.v = CGPointMake(0, 0)

    def calcAcceleration(self, delta, k=1):
        return CGPointMake(delta.x*k, delta.y*k)

    def updateV(self, v0, a):
        return CGPointMake(v0.x + a.x, v0.y + a.y)

    def updateP(self, d0, v):
        return CGPointMake(d0.x + v.x, d0.y + v.y)

    def mouseMoved(self, event):
        print "delta D: (%d, %d)" % (event.deltaX(), event.deltaY())

    def moveMouse(to):
        evsrc = Quartz.CGEventSourceCreate(Quartz.kCGEventSourceStateCombinedSessionState);
        Quartz.CGEventSourceSetLocalEventsSuppressionInterval(evsrc, 0.0);
        Quartz.CGAssociateMouseAndMouseCursorPosition (0);
        Quartz.CGWarpMouseCursorPosition(to);
        Quartz.CGAssociateMouseAndMouseCursorPosition (1);

def main():
    app = NSApplication.sharedApplication()
    delegate = AppDelegate.alloc().init()
    NSApp().setDelegate_(delegate)
    AppHelper.runEventLoop()


if __name__ == '__main__':
   main()