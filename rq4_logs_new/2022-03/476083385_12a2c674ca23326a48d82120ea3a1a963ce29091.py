import arcade
from game.constants import *

class Image:
    """
    Get an image from a file to use as an actor.

    Arg:
        image_filename (file): File image can be found in.

    Attribute:
        _image (file): Load file image into arcade
    """
    def __init__(self, image_filename):
        self._image = arcade.load_texture(IMAGES_DIRECTORY + image_filename)

    def get_image(self):
        """Get image and return it"""
        return self._image