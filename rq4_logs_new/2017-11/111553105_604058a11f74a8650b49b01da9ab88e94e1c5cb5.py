from engine.engine import *


class GameControllerScript(Component):
    def __init__(self):
        Component.__init__(self)
    
    def start(self):
        Component.start(self)
    
    def update(self):
        Component.update(self)

        event_manager = self.game_object.get_component(EventManager)
        if event_manager:
            if event_manager.get_key_down("escape"):
                print("ESC pressed: ending program...")
                quit()