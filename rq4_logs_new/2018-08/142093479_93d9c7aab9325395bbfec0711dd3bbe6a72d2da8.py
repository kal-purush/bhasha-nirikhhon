import sys

sys.path.append("../radioFreq")
sys.path.append("../config")
sys.path.append("../network")

from RadioConfig import RadioConfig
from RadioCtrl import RadioCtrl
from GqrxCtrl import GqrxCtrl

import curses
import os

# get the curses screen window
screen = curses.initscr()

# turn off input echoing
curses.noecho()

# respond to keys immediately (don't wait for enter)
curses.cbreak()

# map arrow keys to special values
screen.keypad(True)

# Create Radio Config object
radioConfig = RadioConfig()

# Create new radio controller object
radioCtrl = RadioCtrl(radioConfig)

freq = 0

# Setup GQRX
gqrx = GqrxCtrl("192.168.29.245", True)

#print "Left or Right arrows to changes frequency, Up arrow to change bands, Q to quit."
try:
    while True:
        screen.addstr(0,0, "Left or Right arrows to changes frequency, Up arrow to change bands, Q to quit.")
        char = screen.getch()
        if char == ord('q'):
            break
        elif char == curses.KEY_RIGHT:
            os.system('clear')
            freq = radioCtrl.getNextFrequency
	    gqrx.gqrxTuneFreq(freq)
            print ("Band Name=" + radioCtrl.getCurrBandName)
        elif char == curses.KEY_LEFT:
            os.system('clear')
            freq = radioCtrl.getPrevFrequency
	    gqrx.gqrxTuneFreq(freq)
            print ("Band Name=" + radioCtrl.getCurrBandName)
        elif char == curses.KEY_UP:
            os.system('clear')
            radioCtrl.setNextBand()
	    demod = radioCtrl.getCurrDemod
            freq = radioCtrl.getCurrFreqency
	    gqrx.gqrxDemodMode(demod)
	    gqrx.gqrxTuneFreq(freq)
            print ("Band Name=" + radioCtrl.getCurrBandName)
        elif char == curses.KEY_DOWN:
            print ("TBD.")


finally:
    # shut down cleanly
    curses.nocbreak();
    screen.keypad(0);
    curses.echo()
    curses.endwin()