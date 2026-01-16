from constants import *

class Ball():

    def __init__(self, image, speed):
        self.image = image
        self.speed = speed
        self.x_dir = 1
        self.y_dir = 1
        self.rect = image.get_rect().move(0, screen_rect.height)
        self.fastspeed = False

    def move(self, paddle, brickDict, background):
        # if hit paddle, change direction based on where on paddle hit
        if self.rect.colliderect(paddle.rect):
            if self.rect.centerx < paddle.rect.left:
                self.rect.right = paddle.rect.left
                self.x_dir *= -1
            if self.rect.centerx > paddle.rect.right:
                self.rect.left = paddle.rect.right
                self.x_dir *= -1
            else:
                self.y_dir *= -1
                ball_x = self.rect.centerx
                paddle_x = paddle.rect.centerx
                self.x_dir = (ball_x - paddle_x) / (paddle.rect.width / 2.0)
                if self.x_dir > 1.0: self.x_dir = 1.0
                if self.x_dir < -1.0: self.x_dir = -1.0
            print self.x_dir


        # if hit sides of screen, change x direction
        if self.rect.left < screen_rect.left:
            self.x_dir *= -1
            self.rect.left = screen_rect.left
        if self.rect.right > screen_rect.right:
            self.rect.right = screen_rect.right
            self.x_dir *= -1

        # if hit top of screen, change y direction and increase speed
        if self.rect.top < screen_rect.top:
            self.rect.top = screen_rect.top
            self.y_dir *= -1
            if not self.fastspeed:
                self.speed *= 1.4
                self.fastspeed = True

        ###
        ### CHANGE to reset ball and decrement life counter
        ###
                
        # if hit bottom, change x direction
        if self.rect.bottom > screen_rect.bottom:
            self.rect.bottom = screen_rect.bottom
            self.y_dir *= -1
            
        # if hit brick, remove brick and reverse x direction
        brick = False
        for i in brickDict.keys():
            if self.rect.colliderect(brickDict[i].rect):
                brick = brickDict[i]
                bricknum = i
                
        # move and blit ball
        screen.blit(background, self.rect, self.rect)

        self.move_one_axis(0, self.y_dir*self.speed, brick)
        self.move_one_axis(self.x_dir*self.speed, 0, brick)
        
        # if brick collision, remove brick from dictionary
        if brick:
            screen.blit(background, brick.rect, brick.rect)
            brickDict.pop(bricknum)
        
        screen.blit(self.image, self.rect)
        pygame.display.update()

    def move_one_axis(self, dx, dy, brick):
        self.rect.x += dx
        self.rect.y += dy

        if brick:
            if self.rect.colliderect(brick.rect):
                print self.rect.x
                if dx > 0:
                    self.rect.right = brick.rect.left
                    self.x_dir *= -1
                if dx < 0:
                    self.rect.left = brick.rect.right
                    self.x_dir *= -1
                if dy > 0:
                    self.rect.bottom = brick.rect.top
                    self.y_dir *= -1
                if dy < 0:
                    self.rect.top = brick.rect.bottom
                    self.y_dir *= -1