import pygame


def handle_keys(event):
    """Handling for general state (ie the only current state)"""
    # Window Handling
    if event.type == pygame.QUIT:
        return {'quit': True}
    if event.type == pygame.VIDEORESIZE:
        return {'resize': True}

    # Mouse Handling
    if event.type == pygame.MOUSEBUTTONDOWN:
        if event.button == 1:
            return {'lmb_down': "Hello, left button!"}
        elif event.button == 2:
            return {'mmb_down': "Hello, middle button!"}
        elif event.button == 3:
            return {'rmb_down': "Hello, right button!"}
    if event.type == pygame.MOUSEBUTTONUP:
        if event.button == 1:
            return {'lmb_up': "Goodbye, left button!"}
        elif event.button == 2:
            return {'mmb_up': "Goodbye, middle button!"}
        elif event.button == 3:
            return {'rmb_up': "Goodbye, right button!"}
    if event.type == pygame.MOUSEMOTION:
        if event.buttons == (0, 1, 0):
            return {'mmb_drag': "Moving around"}

    # Key Handling
    # if event.type == pygame.KEYDOWN:
    #     if event.key == pygame.K_RIGHT:
    #         pass    # return {'move': (+1, 0)}
    #     elif event.key == pygame.K_LEFT:
    #         pass    # return {'move': (-1, 0)}
    #     elif event.key == pygame.K_UP:
    #         pass    # return {'move': (0, -1)}
    #     elif event.key == pygame.K_DOWN:
    #         pass    # return {'move': (0, +1)}

    return {}   # Necessary catch