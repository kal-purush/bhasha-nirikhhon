import random

import pygame

pygame.init()

white = (255, 255, 255)
black = (0, 0, 0)
red = (255, 0, 0)
green = (0, 255, 0)
orange = (255, 165, 0)

width, height = 600, 400

game_display = pygame.display.set_mode((width, height))
pygame.display.set_caption("Snake Game")

clock = pygame.time.Clock()

snake_size = 10
snake_speed = 15

message_font = pygame.font.SysFont('mac', 30)
score_font = pygame.font.SysFont('mac', 25)


def print_score(score):
	text = score_font.render("Score: " + str(score), True, orange)
	game_display.blit(text, [0, 0])


def update_high_score(score):
	text = score_font.render("High Score: " + str(score), True, orange)
	game_display.blit(text, [width - text.get_width(), 0])


def draw_snake(snake_size, snake_pixels):
	for pixel in snake_pixels:
		pygame.draw.rect(game_display, green, [pixel[0], pixel[1], snake_size, snake_size])


def spawn_food(width, height, snake_size):
	food_x = round(random.randrange(0, width - snake_size) / 10.0) * 10.0
	food_y = round(random.randrange(0, height - snake_size) / 10.0) * 10.0
	return food_x, food_y


def run_game(high_score):
	game_over = False
	game_close = False

	x = width / 2
	y = height / 2

	x_speed = 0
	y_speed = 0

	snake_pixels = []
	snake_length = 1

	food_x, food_y = spawn_food(width, height, snake_size)

	last_pressed_key = None

	while not game_over:
		while game_close:
			game_display.fill(black)
			game_over_message = message_font.render('Game Over!', True, red)
			game_display.blit(game_over_message, [(width - game_over_message.get_width()) / 2, height / 2])
			print_score(snake_length - 1)
			update_high_score(high_score)
			pygame.display.update()

			for event in pygame.event.get():
				if event.type == pygame.KEYDOWN:
					if event.key == pygame.K_1:
						game_over = True
						game_close = False
					elif event.key == pygame.K_2:
						run_game(high_score)
				if event.type == pygame.QUIT:
					game_over = True
					game_close = False

		for event in pygame.event.get():
			if event.type == pygame.QUIT:
				game_over = True
			elif event.type == pygame.KEYDOWN:
				if event.key == pygame.K_LEFT and last_pressed_key != pygame.K_RIGHT:
					x_speed = -snake_size
					y_speed = 0
					last_pressed_key = pygame.K_LEFT
				elif event.key == pygame.K_RIGHT and last_pressed_key != pygame.K_LEFT:
					x_speed = snake_size
					y_speed = 0
					last_pressed_key = pygame.K_RIGHT
				elif event.key == pygame.K_UP and last_pressed_key != pygame.K_DOWN:
					x_speed = 0
					y_speed = -snake_size
					last_pressed_key = pygame.K_UP
				elif event.key == pygame.K_DOWN and last_pressed_key != pygame.K_UP:
					x_speed = 0
					y_speed = snake_size
					last_pressed_key = pygame.K_DOWN
		if x >= width or x < 0 or y >= height or y < 0:
			game_close = True

		x += x_speed
		y += y_speed

		game_display.fill(black)
		pygame.draw.rect(game_display, red, [food_x, food_y, snake_size, snake_size])

		snake_pixels.append([x, y])

		if len(snake_pixels) > snake_length:
			del snake_pixels[0]

		for pixel in snake_pixels[:-1]:
			if pixel == [x, y]:
				game_close = True

		draw_snake(snake_size, snake_pixels)
		print_score(snake_length - 1)
		update_high_score(high_score)

		if snake_length - 1 > high_score:
			high_score += 1

		pygame.display.update()

		if x == food_x and y == food_y:
			food_x, food_y = spawn_food(width, height, snake_size)
			snake_length += 1

		clock.tick(snake_speed)

	pygame.quit()


run_game(0)