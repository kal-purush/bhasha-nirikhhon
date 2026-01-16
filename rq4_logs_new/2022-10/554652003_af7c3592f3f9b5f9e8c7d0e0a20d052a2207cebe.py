import pygame
import ctypes 
user32 = ctypes.windll.user32
screensize = (user32.GetSystemMetrics(0), user32.GetSystemMetrics(1))

class Menu():
    # gere le menu
    def __init__(self, screen_size, game):
        self.screen_size = screen_size
    
        # texte du score
        pygame.font.init()                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
        self.police = pygame.font.SysFont("font/font.ttf", 64) # police d'ecriture

        #differente taille d'ecran a afficher dans le menu
        self.taille = [(640,480),
                        (1028,768),
                        (1366,768),
                        (1280,1024),       
                        (1600,900),
                        (1920,1080)]

        self.buttons = [Button(screensize, (screensize[0]/2,screensize[1]/2), game, self.screen_size)]

        # est-ce que les tailles proposees sont plus grandes que l'ecran
        for i in range(len(self.taille)):
            if screensize[0] * screensize[1] > self.taille[i][0] * self.taille[i][1]:
                self.buttons.append(Button((self.taille[i][0], self.taille[i][1]), (screensize[0]/(i+1),screensize[1]/(i+1)), game, self.screen_size))

    def click_on_button(self):
        for button in self.buttons:
            if button.on():
                button.click()

    def afficher(self, screen):
        self.image = self.police.render("Menu", True, (255, 255, 255))
        screen.blit(self.image,(self.screen_size[0] / 2  - self.image.get_size()[0], self.screen_size[1]/40))
        for button in self.buttons:
            button.afficher(screen)

class Button():
    def __init__(self, taille, pos, game, screen_size):
        self.game = game
        self.pos = pos
        self.screen_size = screen_size
        self.taille = taille

        # texte du score
        pygame.font.init()
        self.police = pygame.font.SysFont("font/font.ttf", 52) # police d'ecriture

    def on(self):
        return pygame.Rect.collidepoint(self.rect, (pygame.mouse.get_pos()[0], pygame.mouse.get_pos()[1]))

    def click(self):
        if self.screen_size[0] != self.taille[0]:
            self.game.new_screen(self.taille)

    def afficher(self, screen):
        if screensize[0] == self.taille[0] and screensize[1] == self.taille[1]:
            self.image_text = self.police.render( f"Fullscreen", True , (0,0,0) ) # image du score ("texte a afficher", couleur?, couleur)
        else: self.image_text = self.police.render( f"{self.taille[0]} X {self.taille[1]}", True , (0,0,0) ) # image du score ("texte a afficher", couleur?, couleur)
        
        self.box = pygame.image.load("image/score_box.png")
        self.box = pygame.transform.scale(self.box, (self.image_text.get_size()[0] * 1.2, self.image_text.get_size()[1] * 2))

        self.rect = pygame.Rect(self.pos[0], self.pos[1],self.image_text.get_size()[0] * 1.2, self.image_text.get_size()[1] * 2)
        
        screen.blit(self.box, (self.pos[0] - self.image_text.get_size()[0] - (self.box.get_size()[0] - self.image_text.get_size()[0])/2, self.pos[1] - self.image_text.get_size()[1]/2))
        screen.blit(self.image_text,(self.pos[0] - self.image_text.get_size()[0], self.pos[1]))