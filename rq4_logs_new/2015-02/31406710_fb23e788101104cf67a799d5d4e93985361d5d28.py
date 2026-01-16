import Quartz

def mouseMoved(proxy, t, event, refcon):
    # Do some sanity check.
    if t != Quartz.kCGEventMouseMoved:
        return event;

    # The incoming mouse position.
    currentPoint = Quartz.CGEventGetLocation(event);

    # We can change aspects of the mouse event.
    # For example, we can use CGEventSetLoction(event, newLocation).
    # Here, we just print the location.
    print "(%f, %f)\n" % (currentPoint.x, currentPoint.y);

    #moveMouse(CGPointMake(currentPoint.x+5, currentPoint.y+5));

    # We must return the event for it to be useful.
    return event

def timerCalled(timer, refcon):
    print 'timer Called'


screenBounds = Quartz.CGDisplayBounds(Quartz.CGMainDisplayID())
print 'the main screen is (%d, %d)' % (screenBounds.size.width, screenBounds.size.height)
eventMask = (1 << Quartz.kCGEventMouseMoved)
eventTap = Quartz.CGEventTapCreate(Quartz.kCGSessionEventTap, Quartz.kCGHeadInsertEventTap,
                            0, eventMask, mouseMoved, None)
if eventTap is None:
    print 'Failed to create tap event'


runLoopSource = Quartz.CFMachPortCreateRunLoopSource(
                    Quartz.kCFAllocatorDefault, eventTap, 0)

# Add to the current run loop.
Quartz.CFRunLoopAddSource(Quartz.CFRunLoopGetCurrent(), runLoopSource,
                   Quartz.kCFRunLoopCommonModes);

timerEvent = Quartz.CFRunLoopTimerCreate(None, Quartz.CFAbsoluteTimeGetCurrent(), 0.2, 0, 0, timerCalled, None)
if timerEvent is None:
    print 'timerEvent is None'

Quartz.CFRunLoopAddTimer(Quartz.CFRunLoopGetCurrent(), timerEvent, Quartz.kCFRunLoopCommonModes)

# Enable the event tap.
Quartz.CGEventTapEnable(eventTap, True);

# Set it all running.
Quartz.CFRunLoopRun();

