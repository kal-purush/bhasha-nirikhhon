import pygame
import time
import random
from config.Config import Config as cfg
import game_logic.board.Board

pygame.init()
gameDisplay = pygame.display.set_mode((cfg.display_width, cfg.display_height))
board = game_logic.board.Board.Board()

crash_sound = pygame.mixer.Sound(cfg.crash_sound_path)

pygame.display.set_caption('Chess-Engine')
clock = pygame.time.Clock()

gameIcon = pygame.image.load('media/icons/car_icon.png')

pygame.display.set_icon(gameIcon)



def quitgame():
    pygame.quit()
    quit()


def game_loop():
    ############
    pygame.mixer.music.load('media/sounds/music.wav')
    pygame.mixer.music.play(-1)
    ############

    gameDisplay.blit(board.image, (0, 0))

    for piece in board.white_pieces + board.black_pieces:
        gameDisplay.blit(piece.image,
                         (piece.x_file * int(cfg.display_width / 8), piece.y_file * int(cfg.display_height / 8)))

    pygame.display.update()
    gameExit = False

    has_selected_piece = False

    while not gameExit:

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                quit()

            if event.type == pygame.MOUSEBUTTONDOWN:
                pos = pygame.mouse.get_pos()
                selected_rect = [int(pos[0] / (cfg.display_width / 8)), int(pos[1] / (cfg.display_height / 8))]
                if board.has_selected_piece:
                    board.move_piece(selected_rect[0], selected_rect[1])
                else:
                    board.select_piece(selected_rect[0], selected_rect[1])

                gameDisplay.blit(board.image, (0, 0))

                for piece in board.white_pieces + board.black_pieces:
                    gameDisplay.blit(piece.image, (piece.x_file * int(cfg.display_width / 8), piece.y_file * int(cfg.display_height / 8)))
                    if piece.is_selected:
                        pygame.draw.circle(gameDisplay, (0,0,255), (piece.x_file * int(cfg.display_width / 8) + 35, piece.y_file * int(cfg.display_height / 8) + 35), 10)
                    if piece.TYPE == "K" and board.is_checked[0] == True and piece.color == "white":
                        pygame.draw.circle(gameDisplay, (0, 255, 0), (piece.x_file * int(cfg.display_width / 8) + 35, piece.y_file * int(cfg.display_height / 8) + 35), 10)
                    if piece.TYPE == "K" and board.is_checked[1] == True and piece.color == "black":
                        pygame.draw.circle(gameDisplay, (0, 255, 0), (piece.x_file * int(cfg.display_width / 8) + 35, piece.y_file * int(cfg.display_height / 8) + 35), 10)

                pygame.display.update()


game_loop()
pygame.quit()
quit()