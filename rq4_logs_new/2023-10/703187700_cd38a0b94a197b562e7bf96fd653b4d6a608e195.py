import pygame
import math

# Initialize Pygame
pygame.init()

# Set up display
width, height = 800, 600
screen = pygame.display.set_mode((width, height))
pygame.display.set_caption("Parabolic Trajectories Game")

# Colors
WHITE = (255, 255, 255)
BLUE = (0, 0, 255)
GREEN = (0, 255, 0)

# Define a function to calculate a parabolic trajectory
def parabolic_trajectory(start_pos, initial_velocity, angle, time):
    g = 9.81  # acceleration due to gravity
    x = start_pos[0] + initial_velocity * math.cos(math.radians(angle)) * time
    y = start_pos[1] - (initial_velocity * math.sin(math.radians(angle)) * time - 0.5 * g * time**2)
    return int(x), int(y)

# Define a function to calculate the tangent at a point on the trajectory
def tangent_at_point(start_pos, initial_velocity, angle, time):
    g = 9.81
    vx = initial_velocity * math.cos(math.radians(angle))
    vy = -initial_velocity * math.sin(math.radians(angle)) - g * time
    return math.atan2(vy, vx)

# Main game loop
running = True
while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    screen.fill(WHITE)

    # Draw the ground
    pygame.draw.rect(screen, GREEN, (0, height - 10, width, 10))

    # Draw trajectory
    start_pos = (100, height - 20)
    initial_velocity = 40
    angle = 45

    for t in range(20):
        time = t / 10
        pos = parabolic_trajectory(start_pos, initial_velocity, angle, time)
        pygame.draw.circle(screen, BLUE, pos, 5)

        tangent_angle = tangent_at_point(start_pos, initial_velocity, angle, time)
        end_x = pos[0] + 20 * math.cos(tangent_angle)
        end_y = pos[1] - 20 * math.sin(tangent_angle)
        pygame.draw.line(screen, BLUE, pos, (end_x, end_y))

    pygame.display.flip()

pygame.quit()