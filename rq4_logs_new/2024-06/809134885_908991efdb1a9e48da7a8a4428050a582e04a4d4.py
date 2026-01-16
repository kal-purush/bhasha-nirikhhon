import pygame
import random
import sys

# Initialize Pygame
pygame.init()

# Submit delay
submit_delay = 10000  # milliseconds

# Screen dimensions
screen_width = 1000
screen_height = 1000
screen = pygame.display.set_mode((screen_width, screen_height))
pygame.display.set_caption("Drag and Drop Text")

# Colors
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
GRAY = (200, 200, 200)
BORDER_COLOR = (100, 100, 100)
GREEN = (0, 255, 0)
RED = (255, 0, 0)

# Fonts
font = pygame.font.SysFont(None, 36)

# Load background image
background_image = pygame.image.load('image.png')
background_image = pygame.transform.scale(background_image, (screen_width, screen_height))

class Box:
    def __init__(self, text, rect):
        self.text = text
        self.rect = rect
        self.assigned_text = None
        self.correct = None  # None means unchecked, True means correct, False means incorrect

    def draw(self, surface):
        pygame.draw.rect(surface, BORDER_COLOR, self.rect, 3)
        pygame.draw.rect(surface, GRAY, self.rect)
        if self.assigned_text:
            text_surface = font.render(self.assigned_text, True, BLACK)
            surface.blit(text_surface, (self.rect.x + 10, self.rect.y + 10))
        if self.correct is not None:
            if self.correct:
                mark_surface = font.render("Y", True, GREEN)
            else:
                mark_surface = font.render("N", True, RED)
            surface.blit(mark_surface, (self.rect.right + 10, self.rect.centery - mark_surface.get_height() // 2))

class DraggableText:
    def __init__(self, text, pos):
        self.text = text
        self.pos = pos
        self.rect = font.render(text, True, BLACK).get_rect(topleft=pos)
        self.assigned_box = None

    def draw(self, surface):
        text_surface = font.render(self.text, True, BLACK)
        surface.blit(text_surface, self.pos)

    def update_position(self, new_pos):
        # Ensure the text does not go off the screen
        max_x = screen_width - self.rect.width
        max_y = screen_height - self.rect.height
        self.pos = (
            max(0, min(new_pos[0], max_x)),
            max(0, min(new_pos[1], max_y))
        )
        self.rect.topleft = self.pos

def main():
    texts = ["Internet", "Internet Edge", "Security Edge", "Network Core", "Application Edge", "Wireless Network", "User Device 2", "User Device 1"]
    
    boxes = [
        Box(texts[0], pygame.Rect((479, 20), (200, 40))),
        Box(texts[1], pygame.Rect((619, 210), (200, 40))),
        Box(texts[2], pygame.Rect((275, 370), (200, 40))),
        Box(texts[3], pygame.Rect((622, 359), (200, 40))),
        Box(texts[4], pygame.Rect((12, 610), (200, 40))),
        Box(texts[5], pygame.Rect((460, 630), (200, 40))),
        Box(texts[6], pygame.Rect((766, 792), (200, 40))),
        Box(texts[7], pygame.Rect((507, 967), (200, 40)))
    ]
    
    random.shuffle(texts)
    draggable_texts = [DraggableText(text, (50, 50 + i * 70)) for i, text in enumerate(texts)]

    # Submit button
    submit_button_rect = pygame.Rect(50, screen_height - 70, 200, 50)
    last_submit_time = 0


    dragging_text = None
    dragging_offset = (0, 0)

    running = True
    while running:
        screen.blit(background_image, (0, 0))  # Draw the background image

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                sys.exit()

            elif event.type == pygame.MOUSEBUTTONDOWN:
                pos = pygame.mouse.get_pos()
                current_time = pygame.time.get_ticks()+submit_delay
                if submit_button_rect.collidepoint(pos) and current_time-last_submit_time >= submit_delay:
                    for box in boxes:
                        if box.assigned_text == box.text:
                            box.correct = True
                        else:
                            box.correct = False
                            
                    last_submit_time = current_time
                else:
                    for draggable in draggable_texts:
                        if draggable.rect.collidepoint(pos):
                            dragging_text = draggable
                            dragging_offset = (draggable.rect.x - pos[0], draggable.rect.y - pos[1])
                            if draggable.assigned_box:
                                draggable.assigned_box.assigned_text = None
                                draggable.assigned_box = None
                            break

            elif event.type == pygame.MOUSEBUTTONUP:
                if dragging_text:
                    pos = pygame.mouse.get_pos()
                    dropped = False
                    for box in boxes:
                        if box.rect.collidepoint(pos) and box.assigned_text is None:
                            box.assigned_text = dragging_text.text
                            dragging_text.update_position((box.rect.x + 10, box.rect.y + 10))
                            dragging_text.assigned_box = box
                            dropped = True
                            break
                    if not dropped:
                        dragging_text.update_position((dragging_text.pos[0], dragging_text.pos[1]))  # Return to original position
                    dragging_text = None

            elif event.type == pygame.MOUSEMOTION and dragging_text:
                pos = pygame.mouse.get_pos()
                new_pos = (pos[0] + dragging_offset[0], pos[1] + dragging_offset[1])
                dragging_text.update_position(new_pos)


        pos = pygame.mouse.get_pos()
        # print(pos)
        # Draw boxes
        for box in boxes:
            box.draw(screen)

        # Draw draggable texts
        for draggable in draggable_texts:
            if not draggable.assigned_box:
                draggable.draw(screen)

        # Draw the submit button
        pygame.draw.rect(screen, BORDER_COLOR, submit_button_rect, 3)
        pygame.draw.rect(screen, GRAY, submit_button_rect)
        submit_text_surface = font.render("Submit", True, BLACK)
        screen.blit(submit_text_surface, (submit_button_rect.x + 10, submit_button_rect.y + 10))


        # Draw the time left
        font.render("font", True, BLACK).get_rect(topleft=pos)
        text_surface = font.render("Wait " + str(max(((submit_delay//1000)-(pygame.time.get_ticks()-last_submit_time+submit_delay) // 1000), 0)) + " sec", True, BLACK)
        screen.blit(text_surface, (252, 942))

        pygame.display.update()

if __name__ == "__main__":
    main()