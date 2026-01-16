import pygame, sys, random, os
from pygame.locals import * # pygame에 있는 모든기능을 사용

# 2단점프, 좌우 움직일때 이미지 구분하기

def main():
    # 게임 초기화 정보
    pygame.init()
    screen_width = 1800
    screen_height = 900
    # screen 이란 객체를 생성
    screen = pygame.display.set_mode((screen_width,screen_height))
    # 제목 생성
    pygame.display.set_caption('cat game')
    bgImage = pygame.image.load('pictures/background.jpg')
    bgImage = pygame.transform.scale(bgImage, (screen_width, screen_height))

    # 플레이어 생성
    # 가로 105, 세로 120 객체를 생성, 이후 객체의 왼쪽상단점의 위치를 다음 좌표로 설정
    player = pygame.Rect( 10, 470, 105,120)
    player_img = pygame.image.load('pictures/cat.png') # 이미지 할당
    player_img = pygame.transform.scale(player_img, (105,120)) # 이미지 크기 조정

    # 플레이어 왼쪽 모습 생성
    player_img_left = pygame.image.load(os.path.join('pictures', 'cat_left.png'))
    player_img_left = pygame.transform.scale(player_img_left, (105, 120))

    # 플레이어 오른쪽 모습 생성
    player_img_right = pygame.image.load(os.path.join('pictures', 'cat_right.png'))
    player_img_right = pygame.transform.scale(player_img_right, (105, 120))

    # 플레이어 2단 점프 모습 생성
    player_img_jump = pygame.image.load(os.path.join('pictures', 'cat_jump.png'))
    player_img_jump = pygame.transform.scale(player_img_jump, (105, 120))

    # 발판 생성(추가)
    foothold = pygame.Rect((screen_width + 105) / 2, (screen_height) / 2, 345, 81)
    foothold_img = pygame.image.load(os.path.join('pictures', 'foothold.png'))
    foothold_img = pygame.transform.scale(foothold_img, (345, 81))  # 가로, 세로

    # 게임 속도
    clock = pygame.time.Clock()
    speed = 0.5

    # 중력 표현
    y_vel = 0

    # 속도
    speed = 0.5

    # 2단 점프 구현
    jump_count = 2  # 기본 2회 점프 가능 (더블 점프)
    is_jumping = False  # 점프 상태 플래그
    jump_timer = 0 # 2단 점프시 이미지 길게 보이게 하기 위함

    # 방향에 따른 이미지 표현
    va = 0

    # 2단 점프 소리 구현
    dbjump_sound = pygame.mixer.Sound(os.path.join('pictures', 'one.mp3'))
    dbjump_sound.set_volume(0.5) # 0.0 ~ 1.0

    # 게임 실행에대해 처리되는 코드
    while True:
        dt = clock.tick(60) # 1초에 60번(hz) 업데이트
        # 기능구현 1단계 : x 표시 누르면 시스템 종료하기
        for event in pygame.event.get():
            if event.type == QUIT:
                pygame.quit()
                sys.exit()

        screen.blit(bgImage, (0,0))
        # 발판 이미지 (추가)
        screen.blit(foothold_img, foothold)

        # 키보드로 플레이어 조종
        key = pygame.key.get_pressed()

        # 플레이어 좌우로 움직이기
        if key[K_LEFT] and player.left >= 0 :
            player.left = player.left - speed * dt
            va = 1 # 왼쪽 상태값
        elif key[K_RIGHT] and player.right <= screen_width:
            player.right = player.right + speed * dt
            va = 2 # 오른쪽 상태값
        else :
            va = 0 # 일반값

        # 점프 구현
        player.top = player.top + y_vel
        y_vel += 1
        if player.bottom >= 765:
            player.bottom = 765
            y_vel = 0
            jump_count = 2

        # 발판 바닥 위로 못뛰게
        elif player.colliderect(foothold) and y_vel > 0:
            player.bottom = foothold.top
            y_vel = 0
            jump_count = 2
            jump_timer = 0

        # 발판 위로 착지
        elif player.colliderect(foothold) and y_vel < 0: # 점프할때
            player.top = foothold.bottom
            y_vel = 1

        # 2단 점프 구현
        # is_jumping의 초기값은 False 입니다.
        # 스페이스바를 누르지않으면 False가 됩니다.
        #
        # key[K_SPACE] and jump_count > 0 and not is_jumping 에서 not is_jumping은 not False이니 True가 됩니다.
        # 한번 점프가 발생하고 is_jumping이 True가 됩니다. 다음 프레임(hz)에서 key[K_SPACE] and jump_count > 0 and not is_jumping 은 실행되지않습니다.
        # is_jumping이 not True로 False가 되기 때문입니다. 그리고 스페이스바를 떼면 if not key[k_space]에 의해 is_jumping은 False가 되서 이후 프레임(hz)에서 부터는
        # key[K_SPACE] and jump_count > 0 and not is_jumping 은 성립되게 됩니다.
        # 즉, is_jumping으로 프레임(hz)마다 점프가 실행되는 문제를 해결할수있습니다.
        if key[K_SPACE] and jump_count > 0 and not is_jumping :
            if jump_count == 2:
                y_vel = -18  # 첫 점프
            elif jump_count == 1:
                dbjump_sound.play() # 2단 점프시 사운드
                y_vel = -20  # 두 번째 점프
                jump_timer = 50 # 2단 점프 이미지 길게

            jump_count -= 1  # 점프 횟수 감소
            is_jumping = True  # 점프 상태 시작

        # 점프 키에서 손을 떼면 is_jumping 초기화

        if not key[K_SPACE]:
            is_jumping = False

        # 2단 점프시 타이머 작동 및 이미지 구현
        if jump_timer > 0:
            va = 3
            jump_timer -= 1

        # va 별 이미지 구현
        if va == 1 :
            screen.blit(player_img_left, player)  # 이미지를 플레이어 객체에 그리기, 어려운말로 랜더링(묘사하다라는 사전적 의미)
        elif va == 2 :
            screen.blit(player_img_right, player)
        elif va == 3 :
            screen.blit(player_img_jump, player)
        else :
            screen.blit(player_img, player)

        # 플레이어의 행동에대해 결과를 화면에 업데이트 하기위해 선언
        pygame.display.update()
main()