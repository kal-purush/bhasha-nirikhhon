import pygame
import time
from pygame.sprite import Sprite, Group
from threading import Timer


class Alien(Sprite):
    aliens = Group()
    """A class to represent a single alien in the fleet."""
    def __init__(self, game_set, screen):
        """Initialize the alien and set its starting position."""
        super(Alien, self).__init__()
        self.screen = screen
        self.time = time.time()
        self.game_set = game_set
        # Load the alien image and set its rect attribute.
        self.image = pygame.image.load('images/happy_alien.png')
        self.rect = self.image.get_rect()
        # Start each new alien near the top left of the screen.
        self.rect.x = self.rect.width
        self.rect.y = self.rect.height
        # Store the alien's exact position.
        self.x = float(self.rect.x)
        self.flag = 1

    def update(self):
        """Alien changes on being hit by wait bullet"""
        self.flag = 0
        self.image = pygame.image.load('images/sad_alien.png')
        self.tout = Timer(5.0, self.__del__)
        self.tout.start()

    def blitme(self):
        """Draw the alien at its current location."""
        self.screen.blit(self.image, self.rect)

    def __del__(self):
        """Delete the Alien"""
        Alien.aliens.remove(self)