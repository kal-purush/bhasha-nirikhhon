import pygame 

class Fusee():
    # cette class gere la fusee
    def __init__(self, pos):
        self.pos = [pos[0], pos[1]]
        self.sprite = pygame.image.load("image/fusee.png") # sprite de la fusee 
        self.sprite.convert_alpha()


    def up(self, nb):
        # deplacement de la fusee vers le haut
        self.pos[1] -= nb
    
    def down(self, nb):
        # deplacement de la fusee vers le bas
        self.pos[1] += nb
    
    def right(self, nb):
        # deplacement de la fusee vers la droite
        self.pos[0] += nb
    
    def left(self, nb):
        # deplacement de la fusee vers la gauche
        self.pos[0] -= nb

    def afficher(self,screen):
        # affichage de la fusee
        screen.blit(self.sprite,(self.pos[0],self.pos[1]))