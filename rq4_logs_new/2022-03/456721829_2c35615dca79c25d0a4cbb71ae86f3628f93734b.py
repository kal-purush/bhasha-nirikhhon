import pygame
import Settings

class Level():

    def __init__(self, level_data, tile_size, window):
        self.level_data = level_data
        self.tile_size = tile_size
        self.window = window
        self.tile_list = []

        # load images (hard-coded for now but will change as we update the game)
        dirt_img = pygame.image.load('GameFiles/Assets/grassCenter.png')
        grass_img = pygame.image.load('GameFiles/Assets/grassMid.png')

        # read level_data and put correct image and coordinates into a tile (temporarily hard-coded)
        row_num = 0
        for row in level_data:
            col_num = 0
            for tile in row:
                if tile == 1:
                    img = pygame.transform.scale(dirt_img, (tile_size, tile_size))
                    img_rect = img.get_rect()
                    img_rect.x = col_num * tile_size
                    img_rect.y = row_num * tile_size
                    tile = (img, img_rect)
                    self.tile_list.append(tile)
                if tile == 2:
                    img = pygame.transform.scale(grass_img, (tile_size, tile_size))
                    img_rect = img.get_rect()
                    img_rect.x = col_num * tile_size
                    img_rect.y = row_num * tile_size
                    tile = (img, img_rect)
                    self.tile_list.append(tile)
                col_num += 1
            row_num += 1
            
    def draw_level(self):
        for tile in self.tile_list:
            self.window.blit(tile[0], tile[1])
    
    def get_TileList(self) -> list:
        list = []
        for tile in self.tile_list:
            list.append(tile[0], tile[1])
        return list