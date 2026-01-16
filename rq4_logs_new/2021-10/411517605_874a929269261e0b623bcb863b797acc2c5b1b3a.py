import sys
import unittest
import pygame

sys.path.append('..\\..')
sys.path.append('..')

from genie.constants import mouse
from genie.services.MouseService import MouseService

FPS = 60
W_SIZE = (900, 500)
WIN = pygame.display.set_mode(W_SIZE)
WHITE = (255, 255, 255)
BLACK = (0, 0, 0, 0)


def main():
    """
        A simple game loop with nothing going on except for the checking of
        whether the mouse buttons are pressed

        If none of the mouse buttons are pressed, the loop while continually print out:
            "All mouse buttons released!"
        If any of the mouse buttons are pressed, the console will print out:
            "<button> mouse is pressed"
        ... and the "All mouse buttons released!" message will not print out

        The loop will also continually print out whether the mouse has moved
            from the last iteration AND the current coordinates of the mouse

    """
    # What we're trying to test:
    ms = MouseService()

    # Game loop:
    clock = pygame.time.Clock()
    running = True
    while running:
        clock.tick(FPS)
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False

        # Test is_key_pressed():
        mouse_pressed = ms.is_button_pressed(
            mouse.LEFT, mouse.MIDDLE, mouse.RIGHT, mouse.EXTRA1, mouse.EXTRA2)

        if mouse_pressed[mouse.LEFT]:
            print("LEFT mouse is pressed")
        if mouse_pressed[mouse.MIDDLE]:
            print("MIDDLE mouse is pressed")
        if mouse_pressed[mouse.RIGHT]:
            print("RIGHT mouse is pressed")
        if mouse_pressed[mouse.EXTRA1]:
            print("EXTRA1 mouse is pressed")
        if mouse_pressed[mouse.EXTRA2]:
            print("EXTRA2 mouse is pressed")

        # Test is_key_released():
        mouse_released = ms.is_button_released(
            mouse.LEFT, mouse.MIDDLE, mouse.RIGHT, mouse.EXTRA1, mouse.EXTRA2)

        if (mouse_released[mouse.LEFT] and mouse_released[mouse.MIDDLE] and
                mouse_released[mouse.RIGHT] and mouse_released[mouse.EXTRA1] and
                mouse_released[mouse.EXTRA2]):
            print("All mouse buttons released!")
        
        # Test has_mouse_moved()
        mouse_moved = ms.has_mouse_moved()
        print("Mouse moved: ", mouse_moved)

        # Test get_current_coordinates()
        mouse_position = ms.get_current_coordinates()
        print ("Mouse coordinates: ", mouse_position)

        # Test get_last_coordinates()
        mouse_last_coordinates = ms.get_last_coordinates()
        print ("Last coordinates: ", mouse_last_coordinates)



    pygame.quit()


if __name__ == "__main__":
    main()