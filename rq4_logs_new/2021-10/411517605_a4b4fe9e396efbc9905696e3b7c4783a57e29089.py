
class ScreenService:
    """
        - add methods to the interface
            i.e. ScreenService.DrawImages(Actors)
            (this is in core)
        - create the trait Image
            i.e. image
        - Implement the methods in concrete class
            i.e. PygameScreenService
            A. Loop Through Actors
            If has Image trait:
                convert image data to what pygame needs
                use pygame to draw
    """
    def __init__(self):
        self._images_cache = {}
        pass

    def draw_actors(self, actors):
        pass
    
    def initialize(self):
        pass

    def load_images(self, actors):
        """
            load all the images into a dictionary cache
        """
        pass

    def draw_images(self, actors):
        """
            first thing: check to see if it's in the cache
        """
        pass

    def release(self):
        pass