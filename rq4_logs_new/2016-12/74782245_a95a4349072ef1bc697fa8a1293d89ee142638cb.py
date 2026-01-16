import pygame

ANCHO=1200
ALTO=700

VERDE=(0,255,0)
BLANCO=(255,255,255)
NEGRO=(0,0,0)
GRIS=(100,100,100)
AZUL=(0,0,255)

class Jugador(pygame.sprite.Sprite):
	lb=None
	lv=None

	def __init__(self,px,py):
		pygame.sprite.Sprite.__init__(self)
		self.image=pygame.Surface([20,50])
		self.image.fill(GRIS)
		self.rect=self.image.get_rect()
		self.var_x=0
		self.var_y=0
		self.rect.x=px
		self.rect.y=py

	def gravedad(self):
		if self.var_y==0:
			self.var_y=0.6
		else:
			self.var_y+=0.35

		if self.rect.y >= ALTO-self.rect.height:
			self.var_y=0
			self.rect.y= ALTO-self.rect.height

	def update(self):
		self.gravedad()
		self.rect.x+=self.var_x
		lc=pygame.sprite.spritecollide(self,self.lb,False)
		

		for b in lc:
			if self.var_x>0:
				self.rect.right=b.rect.left
			else:
				self.rect.left=b.rect.right

		self.rect.y+=self.var_y
		lc=pygame.sprite.spritecollide(self,self.lb,False)
		for b in lc:
			if self.var_y>0:
				self.rect.bottom=b.rect.top
			else:
				self.rect.top=b.rect.bottom
			self.var_y=0

		lc2=pygame.sprite.spritecollide(self,self.lv,True)		
		for j in lc2:
			if self.var_x>0:
				self.rect.right=j.rect.left
			else:
				self.rect.left=j.rect.right

		self.rect.y+=self.var_y
		lc2=pygame.sprite.spritecollide(self,self.lv,False)
		for j in lc2:
			if self.var_y>0:
				self.rect.bottom=j.rect.top
			else:
				self.rect.top=j.rect.bottom
			self.var_y=0
		

class Bloque(pygame.sprite.Sprite):
	def __init__(self,an,al,px,py):
		pygame.sprite.Sprite.__init__(self)
		self.image=pygame.Surface([an,al])
		self.image.fill(VERDE)
		self.rect=self.image.get_rect()
		self.rect.x=px
		self.rect.y=py

class ventana(pygame.sprite.Sprite):
	def __init__(self,an,al,px,py):
		pygame.sprite.Sprite.__init__(self)
		self.image=pygame.Surface([an,al])
		self.image.fill(AZUL)
		self.rect=self.image.get_rect()
		self.rect.x=px
		self.rect.y=py
		

if __name__ == '__main__':
	pygame.init()
	pantalla=pygame.display.set_mode([ANCHO,ALTO])
	todos=pygame.sprite.Group()
	bloques=pygame.sprite.Group()
	victoria=pygame.sprite.Group()

	jp=Jugador(10,120)
	todos.add(jp)

	vn=ventana(50,100,585,0)
	todos.add(vn)

	victoria.add(vn)


	b1=Bloque(80,40,650,500)
	b2=Bloque(100,40,600,430)
	b3=Bloque(100,40,0,300)


	bor3=Bloque(10,700,0,0)#BORDE IZQUIERDO
	bor4=Bloque(10,700,1190,0)#BORDE DERECHO
	bor5=Bloque(1200,10,0,0)#BORDE SUPERIOR
	bor6=Bloque(1200,10,0,690)#BORDE INFERIOR


	todos.add(b1)
	todos.add(b2)
	todos.add(b3)

	todos.add(bor3)
	todos.add(bor4)
	todos.add(bor5)
	todos.add(bor6)

	bloques.add(b1)
	bloques.add(b2)
	bloques.add(b3)

	bloques.add(bor3)
	bloques.add(bor4)
	bloques.add(bor5)
	bloques.add(bor4)


	jp.lb=bloques
	jp.lv=victoria
	reloj=pygame.time.Clock()
	fin=False

	while not fin:
		for event in pygame.event.get():
			if event.type == pygame.QUIT:
				fin=True

			if event.type == pygame.KEYDOWN:
				if event.key == pygame.K_LEFT:
					jp.var_x=-5
					#jp.var_x=0
					

				if event.key == pygame.K_RIGHT:
					jp.var_x=+5
					#jp.var_y=0
					

				if event.key == pygame.K_UP:
					#jp.var_x=0
					jp.rect.y += -0.5
					jp.var_y=-4
					#jp.var_y=0

				#if event.key == pygame.K_DOWN:
				#	jp.var_x=0
				#	jp.var_y=5

				
				if event.key == pygame.K_SPACE:
					jp.var_x=0
					jp.var_y=0

			if jp.rect.x >= ANCHO:
				#jp.rect.x=0
				print "si"
				jp.var_x=0
				jp.rect.x=ANCHO-jp.rect.width

			if jp.rect.y <= 15: #revisar
				#jp.rect.x=0
				print "si"
				jp.var_y=0
				jp.rect.y=12

			if jp.rect.y<15:
				jp.var_y=30


		pantalla.fill(NEGRO)
		todos.update()
		todos.draw(pantalla)
		pygame.display.flip()
		reloj.tick(60)