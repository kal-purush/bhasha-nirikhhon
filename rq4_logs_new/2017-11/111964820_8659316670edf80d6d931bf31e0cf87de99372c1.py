# Class to set up a visual display associated with the main experiment
#
# Created:
# Nov 17: Naveed Ejaz
from psychopy import visual

class ExperimentDisplay:
    handle = []

    # constructor
    def __init__(self, params):
        self.handle = visual.Window(params['expwin_size'], color=params['expwin_bgcolor'], pos=params['expwin_pos'], name="ExperimentDisplay")

    # flip buffer to screen
    def flip(self):
		self.handle.flip()

	# close window
    def close(self):
		self.handle.close()
