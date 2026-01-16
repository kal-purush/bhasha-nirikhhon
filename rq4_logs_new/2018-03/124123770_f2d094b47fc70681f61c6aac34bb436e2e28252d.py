import sys
import pygame
import random
import os

def Player_vs_AI():
    # global instances
    RUNNING = True
    TURN = 1
    AREA_NUM = -1
    WIDTH = 800
    HEIGHT = 600
    WHITE = (255, 255, 255)
    BLACK = (0, 0, 0)
    BLUE = (0, 0, 255)
    RED = (255, 0, 0)
    SCORE_PLAYER = 0
    SCORE_AI = 0
    CARD_LEFT = 16
    WAITING_TIME = 600
    COMP_LIST = []
    CARD_LIST = []
    CARD_LIST_COPY = []
    # assign card value in order
    VALUE_IN_AREA = [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8]
    # coordinate for each card area
    AREA_LIST = [(210, 45), (310, 45), (410, 45), (510, 45),
                 (210, 175), (310, 175), (410, 175), (510, 175),
                 (210, 305), (310, 305), (410, 305), (510, 305),
                 (210, 435), (310, 435), (410, 435), (510, 435)]
    # load game folder path
    GAME_FOLDER = os.path.dirname(__file__)
    IMG_FOLDER = os.path.join(GAME_FOLDER, "img")

    # function that find the card area which mouse chooses
    def find_area_num(x, y):
        if x >= 210 and x <= 290:
            if y >= 45 and y <= 165:
                return 1
            if y >= 175 and y <= 295:
                return 5
            if y >= 305 and y <= 425:
                return 9
            if y >= 435 and y <= 555:
                return 13
        if x >= 310 and x <= 390:
            if y >= 45 and y <= 165:
                return 2
            if y >= 175 and y <= 295:
                return 6
            if y >= 305 and y <= 425:
                return 10
            if y >= 435 and y <= 555:
                return 14
        if x >= 410 and x <= 490:
            if y >= 45 and y <= 165:
                return 3
            if y >= 175 and y <= 295:
                return 7
            if y >= 305 and y <= 425:
                return 11
            if y >= 435 and y <= 555:
                return 15
        if x >= 510 and x <= 590:
            if y >= 45 and y <= 165:
                return 4
            if y >= 175 and y <= 295:
                return 8
            if y >= 305 and y <= 425:
                return 12
            if y >= 435 and y <= 555:
                return 16

    # initialize game
    pygame.init()
    screen = pygame.display.set_mode((WIDTH, HEIGHT))
    pygame.display.set_caption("My game")
    pygame.event.set_blocked(pygame.MOUSEMOTION)

    # load game images
    EGG1 = pygame.image.load(os.path.join(IMG_FOLDER, "egg1.png")).convert()
    EGG1 = pygame.transform.scale(EGG1, (80, 120))
    EGG2 = pygame.image.load(os.path.join(IMG_FOLDER, "egg2.png")).convert()
    EGG2 = pygame.transform.scale(EGG2, (80, 120))
    EGG3 = pygame.image.load(os.path.join(IMG_FOLDER, "egg3.png")).convert()
    EGG3 = pygame.transform.scale(EGG3, (80, 120))
    EGG4 = pygame.image.load(os.path.join(IMG_FOLDER, "egg4.png")).convert()
    EGG4 = pygame.transform.scale(EGG4, (80, 120))
    EGG5 = pygame.image.load(os.path.join(IMG_FOLDER, "egg5.png")).convert()
    EGG5 = pygame.transform.scale(EGG5, (80, 120))
    EGG6 = pygame.image.load(os.path.join(IMG_FOLDER, "egg6.png")).convert()
    EGG6 = pygame.transform.scale(EGG6, (80, 120))
    EGG7 = pygame.image.load(os.path.join(IMG_FOLDER, "egg7.png")).convert()
    EGG7 = pygame.transform.scale(EGG7, (80, 120))
    BUNNY = pygame.image.load(os.path.join(IMG_FOLDER, "bunny.png")).convert()
    BUNNY = pygame.transform.scale(BUNNY, (80, 120))
    CARD_IMAGE = pygame.image.load(os.path.join(IMG_FOLDER, "card.png")).convert()
    CARD_IMAGE = pygame.transform.scale(CARD_IMAGE, (80, 120))

    # load texts and scores on the screen
    FONT_NAME = pygame.font.SysFont("Arial", 40, True, False)
    TEXT_PLAYER_NAME = FONT_NAME.render("YOU", False, BLACK)
    TEXT_AI_NAME = FONT_NAME.render("AI", False, BLACK)
    FONT_SCORE = pygame.font.SysFont("Arial", 30, True, False)
    TEXT_PLAYER = FONT_SCORE.render(str(SCORE_PLAYER), False, BLACK)
    TEXT_AI = FONT_SCORE.render(str(SCORE_AI), False, BLACK)
    TEXT_COVER = pygame.Surface((100, 50))
    TEXT_COVER.fill(WHITE)
    FONT_WIN = pygame.font.SysFont("Arial", 100, True, False)
    FONT_DRAW = pygame.font.SysFont("Arial", 100, True, False)
    FONT_LOSE = pygame.font.SysFont("Arial", 100, True, False)

    # shuffle the cards
    random.shuffle(VALUE_IN_AREA)

    # class for card object
    class Card:
        def __init__(self, value, area):
            self.card_value = value
            self.card_area = area
            self.shown = False
            if self.card_value == 1:
                self.image = EGG1
            if self.card_value == 2:
                self.image = EGG2
            if self.card_value == 3:
                self.image = EGG3
            if self.card_value == 4:
                self.image = EGG4
            if self.card_value == 5:
                self.image = EGG5
            if self.card_value == 6:
                self.image = EGG6
            if self.card_value == 7:
                self.image = EGG7
            if self.card_value == 8:
                self.image = BUNNY

    # add cards to a card list
    for i in range(16):
        card = Card(VALUE_IN_AREA[i], AREA_LIST[i])
        CARD_LIST.append(card)
    # copy the card list
    CARD_LIST_COPY = list(CARD_LIST)

    # setup board with 16 cards face down, and texts
    screen.fill(WHITE)
    for coor in AREA_LIST:
        screen.blit(CARD_IMAGE, coor)
    screen.blit(TEXT_PLAYER_NAME, (60, 100))
    screen.blit(TEXT_AI_NAME, (670, 100))
    screen.blit(TEXT_PLAYER, (90, 150))
    screen.blit(TEXT_AI, (680, 150))

    # game loop
    while RUNNING:
        # pygame.display.update()
        # process input (events)
        for event in pygame.event.get():
            # check for closing the window
            if event.type == pygame.QUIT:
                RUNNING = False
            else:
                windows_focus = pygame.mouse.get_focused()
                if windows_focus == 1:
                    # player's turn
                    if TURN == 1:
                        print("player's turn")
                        # if the player has selected two cards:
                        if COMP_LIST.__len__() == 2:
                            print("comparing 2 cards now")
                            # compare two selected cards
                            if COMP_LIST[0].card_value == COMP_LIST[1].card_value:
                                print("matched")
                                SCORE_PLAYER = SCORE_PLAYER + 1
                                # update the score
                                screen.blit(TEXT_COVER, (90, 150))
                                TEXT_PLAYER = FONT_SCORE.render(str(SCORE_PLAYER), False, BLACK)
                                screen.blit(TEXT_PLAYER, (90, 150))
                                CARD_LEFT = CARD_LEFT - 2
                                # remove paired cards from the copy list
                                print("copy list len:", CARD_LIST_COPY.__len__())
                                CARD_LIST_COPY.remove(COMP_LIST[0])
                                CARD_LIST_COPY.remove(COMP_LIST[1])
                                print("copy list len:", CARD_LIST_COPY.__len__())
                                # if bunny is found
                                if COMP_LIST[0].card_value == 8:
                                    print("YOU FOUND BUNNY!")
                                    print("player's score: ", SCORE_PLAYER)
                                else:
                                    TURN = 2
                                    print("player's score: ", SCORE_PLAYER)
                            else:
                                print("missed")
                                COMP_LIST[0].shown = False
                                screen.blit(CARD_IMAGE, COMP_LIST[0].card_area)
                                COMP_LIST[1].shown = False
                                screen.blit(CARD_IMAGE, COMP_LIST[1].card_area)
                                pygame.time.wait(WAITING_TIME)
                                # pygame.display.flip()
                                TURN = 2
                                print("player's score: ", SCORE_PLAYER)
                            COMP_LIST.clear()
                        # if the player has NOT selected two cards:
                        else:
                            if event.type == pygame.MOUSEBUTTONDOWN:
                                x, y = pygame.mouse.get_pos()
                                AREA_NUM = find_area_num(x, y)
                                if AREA_NUM:
                                    print("area: ", AREA_NUM)
                                    if AREA_NUM == 1:
                                        if not CARD_LIST[0].shown:
                                            print("card 1 is selected now")
                                            screen.blit(CARD_LIST[0].image, CARD_LIST[0].card_area)
                                            CARD_LIST[0].shown = True
                                            COMP_LIST.append(CARD_LIST[0])
                                            print("card 1 is waiting for matching")
                                        else:
                                            print("card 1 is already used")
                                    if AREA_NUM == 2:
                                        if not CARD_LIST[1].shown:
                                            print("card 2 is selected now")
                                            screen.blit(CARD_LIST[1].image, CARD_LIST[1].card_area)
                                            CARD_LIST[1].shown = True
                                            COMP_LIST.append(CARD_LIST[1])
                                            print("card 2 is waiting for matching")
                                        else:
                                            print("card 2 is already used")
                                    if AREA_NUM == 3:
                                        if not CARD_LIST[2].shown:
                                            print("card 3 is selected now")
                                            screen.blit(CARD_LIST[2].image, CARD_LIST[2].card_area)
                                            CARD_LIST[2].shown = True
                                            COMP_LIST.append(CARD_LIST[2])
                                            print("card 3 is waiting for matching")
                                        else:
                                            print("card 3 is already used")
                                    if AREA_NUM == 4:
                                        if not CARD_LIST[3].shown:
                                            print("card 4 is selected now")
                                            screen.blit(CARD_LIST[3].image, CARD_LIST[3].card_area)
                                            CARD_LIST[3].shown = True
                                            COMP_LIST.append(CARD_LIST[3])
                                            print("card 4 is waiting for matching")
                                        else:
                                            print("card 4 is already used")
                                    if AREA_NUM == 5:
                                        if not CARD_LIST[4].shown:
                                            print("card 5 is selected now")
                                            screen.blit(CARD_LIST[4].image, CARD_LIST[4].card_area)
                                            CARD_LIST[4].shown = True
                                            COMP_LIST.append(CARD_LIST[4])
                                            print("card 5 is waiting for matching")
                                        else:
                                            print("card 5 is already used")
                                    if AREA_NUM == 6:
                                        if not CARD_LIST[5].shown:
                                            print("card 6 is selected now")
                                            screen.blit(CARD_LIST[5].image, CARD_LIST[5].card_area)
                                            CARD_LIST[5].shown = True
                                            COMP_LIST.append(CARD_LIST[5])
                                            print("card 6 is waiting for matching")
                                        else:
                                            print("card 6 is already used")
                                    if AREA_NUM == 7:
                                        if not CARD_LIST[6].shown:
                                            print("card 7 is selected now")
                                            screen.blit(CARD_LIST[6].image, CARD_LIST[6].card_area)
                                            CARD_LIST[6].shown = True
                                            COMP_LIST.append(CARD_LIST[6])
                                            print("card 7 is waiting for matching")
                                        else:
                                            print("card 7 is already used")
                                    if AREA_NUM == 8:
                                        if not CARD_LIST[7].shown:
                                            print("card 8 is selected now")
                                            screen.blit(CARD_LIST[7].image, CARD_LIST[7].card_area)
                                            CARD_LIST[7].shown = True
                                            COMP_LIST.append(CARD_LIST[7])
                                            print("card 8 is waiting for matching")
                                        else:
                                            print("card 8 is already used")
                                    if AREA_NUM == 9:
                                        if not CARD_LIST[8].shown:
                                            print("card 9 is selected now")
                                            screen.blit(CARD_LIST[8].image, CARD_LIST[8].card_area)
                                            CARD_LIST[8].shown = True
                                            COMP_LIST.append(CARD_LIST[8])
                                            print("card 9 is waiting for matching")
                                        else:
                                            print("card 9 is already used")
                                    if AREA_NUM == 10:
                                        if not CARD_LIST[9].shown:
                                            print("card 10 is selected now")
                                            screen.blit(CARD_LIST[9].image, CARD_LIST[9].card_area)
                                            CARD_LIST[9].shown = True
                                            COMP_LIST.append(CARD_LIST[9])
                                            print("card 10 is waiting for matching")
                                        else:
                                            print("card 10 is already used")
                                    if AREA_NUM == 11:
                                        if not CARD_LIST[10].shown:
                                            print("card 11 is selected now")
                                            screen.blit(CARD_LIST[10].image, CARD_LIST[10].card_area)
                                            CARD_LIST[10].shown = True
                                            COMP_LIST.append(CARD_LIST[10])
                                            print("card 11 is waiting for matching")
                                        else:
                                            print("card 11 is already used")
                                    if AREA_NUM == 12:
                                        if not CARD_LIST[11].shown:
                                            print("card 12 is selected now")
                                            screen.blit(CARD_LIST[11].image, CARD_LIST[11].card_area)
                                            CARD_LIST[11].shown = True
                                            COMP_LIST.append(CARD_LIST[11])
                                            print("card 12 is waiting for matching")
                                        else:
                                            print("card 12 is already used")
                                    if AREA_NUM == 13:
                                        if not CARD_LIST[12].shown:
                                            print("card 13 is selected now")
                                            screen.blit(CARD_LIST[12].image, CARD_LIST[12].card_area)
                                            CARD_LIST[12].shown = True
                                            COMP_LIST.append(CARD_LIST[12])
                                            print("card 13 is waiting for matching")
                                        else:
                                            print("card 13 is already used")
                                    if AREA_NUM == 14:
                                        if not CARD_LIST[13].shown:
                                            print("card 14 is selected now")
                                            screen.blit(CARD_LIST[13].image, CARD_LIST[13].card_area)
                                            if COMP_LIST.__len__() != 2:
                                                CARD_LIST[13].shown = True
                                                COMP_LIST.append(CARD_LIST[13])
                                                print("card 14 is waiting for matching")
                                            else:
                                                print("COMP_LIST only can take 2 cards")
                                        else:
                                            print("card 14 is already used")
                                    if AREA_NUM == 15:
                                        if not CARD_LIST[14].shown:
                                            print("card 15 is selected now")
                                            screen.blit(CARD_LIST[14].image, CARD_LIST[14].card_area)
                                            CARD_LIST[14].shown = True
                                            COMP_LIST.append(CARD_LIST[14])
                                            print("card 15 is waiting for matching")
                                        else:
                                            print("card 15 is already used")
                                    if AREA_NUM == 16:
                                        if not CARD_LIST[15].shown:
                                            print("card 16 is selected now")
                                            screen.blit(CARD_LIST[15].image, CARD_LIST[15].card_area)
                                            CARD_LIST[15].shown = True
                                            COMP_LIST.append(CARD_LIST[15])
                                            print("card 16 is waiting for matching")
                                        else:
                                            print("card 16 is already used")
                                else:
                                    pass

                    # AI's turn
                    if TURN == 2:
                        print("AI's turn")
                        # if AI has selected two cards:
                        if COMP_LIST.__len__() == 2:
                            print("comparing 2 cards now")
                            # compare two selected cards
                            if COMP_LIST[0].card_value == COMP_LIST[1].card_value:
                                print("matched")
                                SCORE_AI = SCORE_AI + 1
                                screen.blit(TEXT_COVER, (680, 150))
                                TEXT_AI = FONT_SCORE.render(str(SCORE_AI), False, BLACK)
                                screen.blit(TEXT_AI, (680, 150))
                                CARD_LEFT = CARD_LEFT - 2
                                # remove paired cards from the copy list
                                print("copy list len:", CARD_LIST_COPY.__len__())
                                CARD_LIST_COPY.remove(COMP_LIST[0])
                                CARD_LIST_COPY.remove(COMP_LIST[1])
                                print("copy list len:", CARD_LIST_COPY.__len__())
                                if COMP_LIST[0].card_value == 8:
                                    print("AI FOUND BUNNY!")
                                    print("AI's score: ", SCORE_AI)
                                else:
                                    TURN = 1
                                    print("AI's score: ", SCORE_AI)
                            else:
                                print("missed")
                                COMP_LIST[0].shown = False
                                screen.blit(CARD_IMAGE, COMP_LIST[0].card_area)
                                COMP_LIST[1].shown = False
                                screen.blit(CARD_IMAGE, COMP_LIST[1].card_area)
                                # temp = pygame.event.Event(pygame.MOUSEBUTTONDOWN)
                                # pygame.event.post(temp)
                                pygame.time.wait(WAITING_TIME)
                                # pygame.display.flip()
                                TURN = 1
                                print("AI's score: ", SCORE_AI)
                            COMP_LIST.clear()
                        # if AI has NOT selected two cards:
                        else:
                            # randomly pick two cards
                            # level 0 AI (pick the two cards randomly)
                            # pick the first card
                            print("AI is choosing the first card")
                            pygame.time.wait(1500)
                            index1 = random.randint(0, CARD_LIST_COPY.__len__() - 2)
                            COMP_LIST.append(CARD_LIST_COPY[index1])
                            screen.blit(CARD_LIST_COPY[index1].image, CARD_LIST_COPY[index1].card_area)
                            CARD_LIST_COPY[index1].shown = True
                            pygame.display.flip()
                            # pick the second card
                            print("AI is choosing the second card")
                            pygame.time.wait(1500)
                            if CARD_LIST_COPY.__len__() == 1:
                                index2 = 0
                            else:
                                while True:
                                    index2 = random.randint(0, CARD_LIST_COPY.__len__() - 2)
                                    if index2 != index1:
                                        break
                            COMP_LIST.append(CARD_LIST_COPY[index2])
                            screen.blit(CARD_LIST_COPY[index2].image, CARD_LIST_COPY[index2].card_area)
                            CARD_LIST_COPY[index2].shown = True
                            # pygame.time.wait(2000)

        # exit game when all cards are flipped
        if CARD_LEFT == 0:
            if SCORE_PLAYER > SCORE_AI:
                print("YOU WIN")
                WIN_WINDOW = FONT_WIN.render("YOU WIN", False, RED)
            elif SCORE_PLAYER == SCORE_AI:
                print("DRAW")
                WIN_WINDOW = FONT_DRAW.render("DRAW", False, BLUE)
            else:
                print("YOU LOSE")
                WIN_WINDOW = FONT_LOSE.render("YOU LOSE", False, BLACK)
            screen.blit(WIN_WINDOW, (200, 200))
            RUNNING = False

        pygame.display.flip()

    # end loop
    pygame.time.wait(WAITING_TIME)
    pygame.quit()
    sys.exit(0)


def main():
    mode = input("Pick a mode: "
                 "1. Player vs Player"
                 "2. Player vs AI")
    if mode == '1':
        print("mode 1")
    if mode == '2':
        Player_vs_AI()

if __name__ == "__main__":
    main()



