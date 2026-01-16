import pygame
from genie_core.cast.actors import Actors
from genie_core.cast.actor import Actor
from genie_core.cast.trait import Trait
from genie_core.cast.Body import Body
from genie_core.cast.Image import Image

class PygameScreenService:
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
        if not pygame.get_init():
            pygame.init()
        self._images_cache = {}
    
    def initialize(self):
        pass
    
    def _load_image(self, actor : Actor):
        image_path = actor.get_trait(Image).get_path()
        image = pygame.image.load(image_path)

        width = actor.get_trait(Body).get_width()
        height = actor.get_trait(Body).get_height()
        rotation = actor.get_trait(Image).get_rotation()

        transformed_image = pygame.transform.rotate(
            pygame.transform.scale(image, (width, height)), 
            rotation)
        return transformed_image

    def load_images(self, actors : Actors):
        """
            load all the images into a dictionary cache
        """
        actors_with_image = actors.with_traits(Image)
        for actor in actors_with_image:
            image_path = actor.get_trait(Image).get_path()
            self._images_cache[image_path] = self._load_image(actor)

    def draw_images(self, actors : Actors, window : pygame.Surface):
        """
            first thing: check to see if it's in the cache
        """
        actors_with_body_image = actors.with_traits(Body, Image)
        for actor in actors_with_body_image:
            position = actor.get_trait(Body).get_position()
            image_path = actor.get_trait(Image).get_path()
            if image_path in self._images_cache.keys():
                window.blit(self._images_cache[image_path], position)
            else:
                window.blit(self._load_image(actor), position)
        
        pygame.display.update()

    def release(self):
        pass