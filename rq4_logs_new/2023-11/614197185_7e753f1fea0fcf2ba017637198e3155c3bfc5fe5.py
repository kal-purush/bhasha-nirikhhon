import pygame
import time
from Game.Background import Background
from Game.Ball import Ball
from Game.Paddle import Paddle

screen = pygame.display.set_mode((1000, 600))


def collisions(ball, left_paddle, right_paddle, background):

    if ball.y + ball.radius >= 600:  # ball.y is center of ball so we add radius to get outer point of ball
        ball.y_speed *= -1  # Makes our speed negative, therefore bouncing off wall

    elif ball.y - ball.radius <= 0:
        ball.y_speed *= -1

    if ball.x + ball.radius == right_paddle.x and (right_paddle.y < ball.y - ball.radius < right_paddle.y + right_paddle.height or right_paddle.y < ball.y + ball.radius < right_paddle.y + right_paddle.height) and (ball.x_speed > 0):  # Last and fixes simultaneous collision with wall and paddle
        ball.x_speed *= -1

        middle_y = (right_paddle.y + right_paddle.height + right_paddle.y) / 2
        gap = middle_y - ball.y
        formula = (right_paddle.height / 2) / ball.max_speed
        ball.y_speed = gap / formula

        background.right_hits += 1

    elif ball.x - ball.radius == left_paddle.x + left_paddle.width and (left_paddle.y < ball.y + ball.radius < left_paddle.y + left_paddle.height or left_paddle.y < ball.y - ball.radius < left_paddle.y + left_paddle.height) and (ball.x_speed < 0):  # Last and fixes simultaneous collision with wall and paddle
        ball.x_speed *= -1

        middle_y = (left_paddle.y + left_paddle.height + left_paddle.y) / 2
        gap = middle_y - ball.y
        formula = (left_paddle.height / 2) / ball.max_speed
        ball.y_speed = gap / formula

        background.left_hits += 1

    if ball.x + ball.radius >= 1000 and (ball.y + ball.radius < 100 or ball.y - ball.radius > 500) and (ball.y - ball.radius < right_paddle.y or ball.y + ball.radius > right_paddle.y + right_paddle.height) and (ball.x_speed > 0):
        ball.x_speed *= -1

    elif ball.x - ball.radius <= 0 and (ball.y + ball.radius < 100 or ball.y - ball.radius > 500) and (ball.y - ball.radius < left_paddle.y or ball.y + ball.radius > left_paddle.y + left_paddle.height) and (ball.x_speed < 0):
        ball.x_speed *= -1


def restart(ball, left_paddle, right_paddle, background):
    if ball.x < left_paddle.x and (100 <= ball.y - ball.radius and ball.y + ball.radius <= 500):
        background.right_score += 1
        ball.x = 500
        time.sleep(0.5)
        left_paddle.y = left_paddle.start_position
        right_paddle.y = right_paddle.start_position
        ball.y_speed = 0
        ball.y = ball.y_start

    elif ball.x > right_paddle.x + right_paddle.width and (100 <= ball.y - ball.radius and ball.y + ball.radius <= 500):
        background.left_score += 1
        ball.x = 500
        time.sleep(0.5)
        left_paddle.y = left_paddle.start_position
        right_paddle.y = right_paddle.start_position
        ball.y_speed = 0
        ball.y = ball.y_start


right_paddle = Paddle(980, 250, 20, 100, screen)  
left_paddle = Paddle(0, 250, 20, 100, screen)  

background = Background(5, 0, screen)

ball = Ball(500, 300, 15, 5, screen)


def main():
    pygame.init()

    clock = pygame.time.Clock()
    pygame.display.set_caption('Pong')

    game_active = True
    run = True

    while run:
        quit_game = pygame.key.get_pressed()
        for event in pygame.event.get():
            if quit_game[pygame.K_ESCAPE]:
                run = False

        if game_active:
            screen.fill('black')

            background.update()

            right_paddle.update()
            left_paddle.update()

            ball.update()

            collisions(ball, left_paddle, right_paddle, background)
            restart(ball, left_paddle, right_paddle, background)

        if background.right_score == 5:
            game_active = False

            screen.fill((8, 8, 8))
            background.draw_background()
            right_paddle.draw_paddle()
            left_paddle.draw_paddle()

            winner = pygame.font.Font(None, 60)
            winner = winner.render('Right    Wins', False, (255, 250, 250))
            screen.blit(winner, (400, 50))

            restart_button = pygame.font.Font(None, 50)
            restart_button = restart_button.render('Click R  to restart', False, (0, 128, 128))
            screen.blit(restart_button, (375, 300))

            keys = pygame.key.get_pressed()
            if keys[pygame.K_r]:
                background.right_score = 0
                background.left_score = 0
                game_active = True

        elif background.left_score == 5:
            game_active = False

            screen.fill((8, 8, 8))
            background.draw_background()
            right_paddle.draw_paddle()
            left_paddle.draw_paddle()

            winner = pygame.font.Font(None, 60)
            winner = winner.render('Left    Wins', False, (255, 250, 250))
            screen.blit(winner, (400, 50))

            restart_button = pygame.font.Font(None, 50)
            restart_button = restart_button.render('Click R  to restart', False, (0, 128, 128))
            screen.blit(restart_button, (375, 300))

            keys = pygame.key.get_pressed()
            if keys[pygame.K_r]:
                background.left_score = 0
                background.right_score = 0
                game_active = True

        pygame.display.update()
        clock.tick(60)


if __name__ == '__main__':
    main()  # DRAW A CIRCLE AROUND GOAL