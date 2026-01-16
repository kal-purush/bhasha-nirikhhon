"""
Allows creation of lists of coordinates by drawing.
"""
from basePy import *
#from sys import exit
from func import pointToLine, mirror

pygame.init()
clock = pygame.time.Clock()
Surf = pygame.display.set_mode((1000, 800))


def main():
    """
    Main loop.
    """
    global clock
    points = []
    rect = None
    drawShadow = True
    # Above all...
    Surf.fill(white)
    pygame.display.update()
    while True:
        Surf.fill(white)
        # Write messages
        message_to_screen(Surf, 'CTR+S to save points, CTRL+Z to delete last point. Try SHIFT for easy rectangles.', red, -375)
        message_to_screen(Surf, 'Press T to toggle shadowing, #M to mirror current points. ESC to quit.', red, 375)
        # Represent chain
        points_pos = None
        if len(points) >= 1:
            for point in points:
                # Draw point
                pygame.draw.rect(Surf, black,pygame.Rect(point[0],point[1],1,1), 1)
                # Calculate wanted point position (some highest mathematics #420)
            if pygame.key.get_mods() & pygame.KMOD_SHIFT:
                if abs(mouse_pos_rounded[0] - points[-1][0]) > abs(mouse_pos_rounded[1] - points[-1][1]):
                    # y > x
                    if abs(mouse_pos_rounded[0] - points[-1][0]) > 2 * abs(mouse_pos_rounded[1] - points[-1][1]):
                        # y > 2x    ---> get the usual
                        x, y = mouse_pos_rounded[0], points[-1][1]
                    else:
                        #Pay no attention, just some HIGH MATHS
                        diagonal = -1 if mouse_pos_rounded[0] > points[-1][0] and mouse_pos_rounded[1] < points[-1][1] or mouse_pos_rounded[0] < points[-1][0] and mouse_pos_rounded[1] > points[-1][1] else 1
                        x, y = pointToLine( (mouse_pos_rounded[0], mouse_pos_rounded[1]),
                                            (points[-1][0], points[-1][1]),
                                            (points[-1][0] + 1,
                                             points[-1][1] + diagonal),
                                            True)
                else:
                    if 2*abs(mouse_pos_rounded[0] - points[-1][0]) < abs(mouse_pos_rounded[1] - points[-1][1]):
                        x, y = points[-1][0], mouse_pos_rounded[1]
                    else:
                         diagonal = -1 if mouse_pos_rounded[0] > points[-1][0] and mouse_pos_rounded[1] < points[-1][1] or mouse_pos_rounded[0] < points[-1][0] and mouse_pos_rounded[1] > points[-1][1] else 1
                         x, y = pointToLine( (mouse_pos_rounded[0], mouse_pos_rounded[1]),
                                            (points[-1][0], points[-1][1]),
                                            (points[-1][0] + diagonal,
                                             points[-1][1] + 1), True)

                points_pos = (x, y)
            else:
                points_pos = mouse_pos_rounded

            # Drawing a chain...
            pointsToDraw = list(points)
            # Add a shadow
            if drawShadow: pointsToDraw.append(points_pos)
            # Draw points and update
            if len(points) > 1 or drawShadow:
                rect = pygame.draw.aalines(Surf, black, False, pointsToDraw)

            pygame.display.update()

        # Event loop
        for event in pygame.event.get():
            mouseAt = pygame.mouse.get_pos()
            mouse_pos_rounded = ((round(mouseAt[0], -1), round(mouseAt[1], -1))) # For mouse events
            if event.type == pygame.QUIT or (event.type == pygame.KEYDOWN and event.key == pygame.K_ESCAPE):
                pygame.quit()
                #exit()

            # Keyboard events
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_z:
                    if (pygame.key.get_mods() & pygame.KMOD_CTRL) and len(points) > 0:
                        del (points[-1])
                        if len(points) ==0:
                            Surf.fill(white)
                            pygame.display.update()
                elif event.key == pygame.K_s and (pygame.key.get_mods() & pygame.KMOD_CTRL):  # CTRL+S
                    with open('coords.txt', 'a') as out:
                        out.write(str(points) + '\n')
                        points = []
                        Surf.fill(white)
                        pygame.display.update()
                elif event.key == pygame.K_t:
                    drawShadow = toggle(drawShadow)
                elif event.key == pygame.K_m:
                    points = mirror(points)

            # Mouse events
            elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                if pygame.key.get_mods() & pygame.KMOD_SHIFT and len(points)>=1:
                    points.append(points_pos)
                else:
                    points.append(mouse_pos_rounded)
clock.tick(65)


def toggle(bool):
    """
    :param bool: boolean that needs to be toggled
    :return: if True -> False, else -> True
    """
    if bool: return False
    else: return True

if __name__ == '__main__':
    main()