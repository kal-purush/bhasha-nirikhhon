import pygame
import sys
from pygame.sprite import Sprite

class Alien(Sprite):
    """A class to represent a single alien in the fleet."""

    def __init__(self, ai_settings, screen):
        """Initialize the alien and set its starting position."""
        super(Alien, self).__init__()
        self.screen = screen
        self.ai_settings = ai_settings

        #Load the alien images and set its rect attribute.
        self.images = [pygame.image.load('images/alien1.bmp'), pygame.image.load('images/alien2.bmp'), pygame.image.load('images/alien3.bmp'), pygame.image.load('images/alien4.bmp'), pygame.image.load('images/alien5.bmp'), pygame.image.load('images/alien6.bmp'), pygame.image.load('images/alien7.bmp'), pygame.image.load('images/alien8.bmp')]
        self.index = 0
        self.image = self.images[self.index]
        self.rect = pygame.Rect(2, 2, 80, 53)

        # Start each new alien at the top left of the screen.
        self.rect.x = self.rect.width
        self.rect.y = self.rect.height

        # Store the alien's exact position.
        self.x = float(self.rect.x)

    def update_sprite(self):
        """This method iterates through the elements inside self.images and 
        displays the next one each tick."""
        self.index += 1
        if self.index >= len(self.images):
            self.index = 0
        self.image = self.images[self.index]

    def blitme(self):
        """Draw the alien at its current location."""
        self.screen.blit(self.image, self.rect)

    def check_edges(self):
        """Return True if alien is at edge of screen."""
        screen_rect = self.screen.get_rect()
        if self.rect.right >= screen_rect.right:
            return True
        elif self.rect.left <= 0:
            return True

    def update(self):
        """Move the alien right."""
        self.x += (self.ai_settings.alien_speed_factor * self.ai_settings.fleet_direction)
        self.rect.x = self.x
        self.index += 1
        if self.index >= len(self.images):
            self.index = 0
        self.image = self.images[self.index]