#! /usr/bin/env python
# -*- coding: utf-8

from datetime import datetime, timedelta
import threading
import time
import sys

class ConsoleDispThread (threading.Thread):
    """ Console display thread """
    def __init__(self, the_globs):
        threading.Thread.__init__(self)
        self.termevent=threading.Event()
        self.globs = the_globs
        
    def run(self):

        print ( "  P   PTEC timedelta(s)" )
        while not self.termevent.isSet():
            p_app = self.globs.p_app
            ptec  = self.globs.ptec
            last_frame_time = self.globs.last_frame_time
            td = (datetime.now() - last_frame_time)
            td = td - timedelta(0,0,td.microseconds) #throw away the microseconds.
            print '\r%5i  %s     %s' % (p_app,ptec,td),
            sys.stdout.flush()
            time.sleep(0.5)
            
    def stop(self):
        self.termevent.set()