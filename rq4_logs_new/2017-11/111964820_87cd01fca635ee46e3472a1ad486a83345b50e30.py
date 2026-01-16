# Class to set up a diagnostic display associated with the main experiment
#
# Created:
# Nov 17: Naveed Ejaz
from psychopy import visual

class DiagnosticDisplay:
    handle      = []
    autodraw    = []

    # constructor
    def __init__(self, params):
        self.handle     = []
        self.autodraw   = params['diagnosticwin_autodraw']

    # turn diagnostic window on/off
    def diagnostic(self, mode, params):
        if mode == 'on':
            if self.handle == []:
                self.handle = visual.Window(params['diagnosticwin_size'], color=params['diagnosticwin_bgcolor'], pos=params['diagnosticwin_pos'], name="DiagnosticDisplay")
        elif mode == 'off':
            self.close()

    # write text to diagnostic
    def write(self,text):
        txt = visual.TextStim(self.handle, text=text)
        txt.setAutoDraw(self.autodraw)
        return txt

    # flip buffer to screen
    def flip(self):
        if self.handle != []:
            self.handle.flip()

	# close window
    def close(self):
        if self.handle != []:
            self.handle.close()
            self.handle = []