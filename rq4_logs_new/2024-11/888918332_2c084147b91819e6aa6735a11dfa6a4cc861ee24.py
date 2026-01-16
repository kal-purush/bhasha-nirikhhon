import pygame, sys
from pygame.locals import *

pygame.init()

width = 700
height = 500
screen = pygame.display.set_mode((width, height))

# 배경
bgImage = pygame.image.load("background.jpg")
bgImage = pygame.transform.scale(bgImage, (width, height))
backX = 0
backX2 = width - 10

# 주인공
img = pygame.image.load("spaceship.png")
spaceship = {
  "rect": pygame.Rect(30, 215, 70, 70),
  "image": pygame.transform.scale(img, (70, 70)),
}

# 총알
bullets = []
bulletImage = pygame.image.load("bullet1.png")
bulletImage = pygame.transform.scale(bulletImage, (20, 10))

while True:
  for event in pygame.event.get():
    if event.type == QUIT:
      pygame.quit()
      sys.exit()
    if event.type == KEYDOWN and event.key == K_SPACE:
      bullet = pygame.Rect(
        spaceship["rect"].centerx - 10, spaceship["rect"].centery - 5, 20, 10
      )
      bullets.append(bullet)

  # 키보드 이벤트
  keyInput = pygame.key.get_pressed()
  if keyInput[K_RIGHT]:
    backX -= 3
    backX2 -= 3

  if keyInput[K_UP] and spaceship["rect"].top > 0:
    spaceship["rect"].top -= 3
  elif keyInput[K_DOWN] and spaceship["rect"].bottom < height:
    spaceship["rect"].bottom += 3

  # 배경 움직임
  if backX <= width * -1:
    backX = width - 10

  if backX2 <= width * -1:
    backX2 = width - 10

  if backX >= 0:
    backX -= width - 10
    backX2 -= width - 10

  screen.blit(bgImage, (backX, 0))
  screen.blit(bgImage, (backX2, 0))

  for bullet in bullets:
    bullet.x += 5
    screen.blit(bulletImage, bullet)
    if bullet.right >= width:
      bullets.remove(bullet)

  screen.blit(spaceship["image"], spaceship["rect"])
  pygame.display.update()