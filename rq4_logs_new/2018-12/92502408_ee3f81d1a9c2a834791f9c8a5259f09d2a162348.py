#for all the in game objects


import pygame
from settings import *
from os import path
from random import choice,randrange

vector=pygame.math.Vector2

class Player(pygame.sprite.Sprite):
	def __init__(self,game):
		self._layer=PLAYER_LAYER
		self.groups = game.all_sprites
		pygame.sprite.Sprite.__init__(self,self.groups)
		self.game=game
		self.walking=False
		self.jumping=False
		self.current_frame=0
		self.last_update=0 		#we only make changes sometimes, not every frame
		self.load_images()
		self.image = self.standing_frame
		self.rect=self.image.get_rect()
		self.rect.center=(WIDTH/2,HEIGHT/2)
		self.pos=vector(WIDTH / 2-20, HEIGHT - 80)
		self.vel=vector(0,0)
		self.acc=vector(0,0)
	
	
	
	def load_images(self):
		#loads the images so that we can use to animate
		self.standing_frame=self.game.shrink_image(pygame.image.load(path.join(self.game.image_dir, 'player_idle.png')),0.8)
		
		self.standing_frame.set_colorkey(BLACK)
		self.walking_frames_r=[pygame.image.load(path.join(self.game.image_dir, 'player_walk1.png')),
						     pygame.image.load(path.join(self.game.image_dir, 'player_walk2.png'))]
		
		for i in range(len(self.walking_frames_r)): #shrinking every image in list
			self.walking_frames_r[i]=self.game.shrink_image(self.walking_frames_r[i],0.8)
		self.walking_frames_l=[]
		
		for frame in self.walking_frames_r:
			self.walking_frames_l.append(pygame.transform.flip(frame, True, False))    #flipping the right side walking frames
			frame.set_colorkey(BLACK)
		self.jump_frame=self.game.shrink_image(pygame.image.load(path.join(self.game.image_dir, 'player_jump.png')),0.8)
		self.jump_frame.set_colorkey(BLACK)
		
		
	
	def jump(self):
		self.rect.y+=1
		hits=pygame.sprite.spritecollide(self,self.game.platforms,False)
		self.rect.y-=1
		if hits and not self.jumping:
			self.game.jump_sound.play()
			self.jumping=True
			self.vel.y=-player_jump	
		
	def low_jump(self): #if we dont press space properly then low jump
		if self.jumping:
			if self.vel.y<-10:  #we disallow from going upwardsimage_
				self.vel.y=-10
	
	
	def animate(self):
		now=pygame.time.get_ticks()
		if self.vel.x != 0:
			self.walking=True
		else:
			self.walking=False
			
		#animation of walking
		if self.walking:
			if now-self.last_update> 200: #time since last update
				self.last_update=now #resetting the time of last update
				self.current_frame=(self.current_frame+1)%len(self.walking_frames_r)
				bottom=self.rect.bottom
				if self.vel.x>0:
					self.image=self.walking_frames_r[self.current_frame]   #walking right
				else:
					self.image=self.walking_frames_l[self.current_frame]   #walking left
				self.rect.bottom=bottom
		
		#standing still animation
		if not self.walking and not self.jumping:
			self.image=self.standing_frame 
			
		if self.jumping and not self.vel.x<0:
			self.image=self.jump_frame
		elif self.jumping and self.vel.x<0:
			self.image=pygame.transform.flip(self.jump_frame, True, False)
		self.mask=pygame.mask.from_surface(self.image)
			
				
	
	def update(self):
		self.animate()
		self.acc=vector(0,player_gravity)
		keypress=pygame.key.get_pressed()
		#we change accleration if we press key
		if keypress[pygame.K_LEFT]:
			self.acc.x= -player_acc
		if keypress[pygame.K_RIGHT]:
			self.acc.x= player_acc
		
		#wrap walls
		if self.rect.left > WIDTH:
			self.pos.x = 0
		if self.rect.right < 0:
			self.pos.x = WIDTH
		
		#applying the friction to avoid endless movement
		self.acc.x += self.vel.x * player_friction
		
		self.vel+=self.acc
		#the velocity keeps decreasing but doesnt become 0 , so we make it 0 when it is very small
		if abs(self.vel.x)<0.5:
			self.vel.x=0
		
		self.pos+=self.vel+0.5*self.acc
		
		self.rect.midbottom=self.pos
		
		

class Platform(pygame.sprite.Sprite):
	def __init__(self,game,x,y):
		self._layer=PLATFORM_LAYER
		self.groups = game.all_sprites, game.platforms
		pygame.sprite.Sprite.__init__(self,self.groups)
		self.game=game
		images=[self.game.shrink_image(pygame.image.load(path.join(self.game.image_dir, 'ground_snow_broken.png')),0.5),
		        self.game.shrink_image(pygame.image.load(path.join(self.game.image_dir, 'ground_stone_broken.png')),0.5),
				self.game.shrink_image(pygame.image.load(path.join(self.game.image_dir, 'ground_stone_small.png')),0.5),
				self.game.shrink_image(pygame.image.load(path.join(self.game.image_dir, 'ground_snow_small.png')),0.5)]
		self.image=choice(images)
		self.image.set_colorkey(BLACK)
		self.rect=self.image.get_rect()
		self.rect.x=x
		self.rect.y=y
		if randrange(100)<powerup_freq:
			Powerup(self.game,self)
	

class Powerup(pygame.sprite.Sprite):
	def __init__(self,game,plat):
		self._layer=POWERUP_LAYER
		self.groups = game.all_sprites, game.powerups
		pygame.sprite.Sprite.__init__(self,self.groups)
		self.game=game
		self.plat=plat
		self.type=choice(['boost']) #list of diff powerups
		self.image=self.game.shrink_image(pygame.image.load(path.join(self.game.image_dir, 'powerup_jetpack.png')),0.5)
		self.image.set_colorkey(BLACK)
		self.rect=self.image.get_rect()
		self.rect.centerx=self.plat.rect.centerx
		self.rect.bottom=self.plat.rect.top-5  #right above the platform
	
	def update(self):
		self.rect.bottom=self.plat.rect.top-5
		if not self.game.platforms.has(self.plat):
			self.kill()
	
			

class Mob(pygame.sprite.Sprite):
	def __init__(self,game):
		self._layer=MOB_LAYER
		self.groups = game.all_sprites, game.mobs
		pygame.sprite.Sprite.__init__(self,self.groups)
		self.game=game
		self.image_up=self.game.shrink_image(pygame.image.load(path.join(self.game.image_dir, 'flyMan_still_fly.png')),0.4)
		self.image_up.set_colorkey(BLACK)
		self.image_down=self.game.shrink_image(pygame.image.load(path.join(self.game.image_dir, 'flyMan_still_jump.png')),0.4)
		self.image_down.set_colorkey(BLACK)
		self.image=self.image_up
		self.rect = self.image.get_rect()
		self.rect.centerx = choice([-100, WIDTH + 100]) #spawn position
		self.vx = randrange(2, 5) #velocity
		if self.rect.centerx > WIDTH:  #reversing velocity
			self.vx *= -1
		self.rect.y = randrange(HEIGHT / 2)  #top half of the screen only
		self.vy = 0
		self.dy = 0.5
		
	
	def update(self):
		self.rect.x += self.vx #speed
		self.vy += self.dy #upward motion
		if self.vy > 3 or self.vy < -3:  #limitting upward motion
			self.dy *= -1
		center = self.rect.center
		
		if self.dy < 0:  #animation
			self.image = self.image_up
		else:
			self.image = self.image_down
		self.mask=pygame.mask.from_surface(self.image)
		self.rect = self.image.get_rect()
		
		self.rect.center = center
		
		self.rect.y += self.vy
		if self.rect.left > WIDTH + 100 or self.rect.right < -100:  #delete if gone
			self.kill()
		
class Cloud(pygame.sprite.Sprite):
	def __init__(self,game):
		self._layer=CLOUD_LAYER
		self.groups = game.all_sprites, game.clouds
		pygame.sprite.Sprite.__init__(self,self.groups)
		self.game=game
		self.image=choice(self.game.cloud_images)
		self.image.set_colorkey(BLACK)
		self.rect = self.image.get_rect()
		scale = randrange(50, 101) / 100
		self.image = pygame.transform.scale(self.image, (int(self.rect.width * scale),
													 int(self.rect.height * scale)))
		self.rect.x = randrange(WIDTH - self.rect.width)
		self.rect.y = randrange(-500, -50)

	def update(self):
		if self.rect.top > HEIGHT * 2:
			self.kill()