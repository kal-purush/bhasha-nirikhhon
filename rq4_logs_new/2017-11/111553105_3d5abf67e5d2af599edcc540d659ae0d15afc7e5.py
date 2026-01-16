class GameObject(object):    
    def __init__(self):
        self.name = "Game Object"
        
        self.components = []
        self.children = []
        self.parent = None
    
    def get_component(self, component_type):
        for component in self.components:
            if type(component) == component_type:
                return component
        return None
    
    def add_component(self, component):
        self.components.append(component)
        component.game_object = self
    
    def remove_component(self, component):
        self.components.remove(component)
        component.game_object = None
    
    def add_child(self, game_object):
        self.children.append(game_object)
        game_object.parent = self
    
    def remove_child(self, game_object):
        self.children.remove(game_object)
        game_object.parent = None
    
    def update(self):
        for component in self.components:
            if component.enabled:
                component.update()
        
        for game_object in self.children:
            game_object.update()