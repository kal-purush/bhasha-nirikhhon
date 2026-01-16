import game_framework
from pico2d import *
import main_state
import title_state
import game_world
import map

name = "howtoplay"
image = None


def enter():
    global image
    global tile
    global font
    image = load_image('Images//chrome.png')
    tile = load_image('Images//tiles0.png')
    font = load_font('fonts.ttf')


def exit():
    global image
    del (image)


def handle_events():
    events = get_events()
    for event in events:
        if event.type == SDL_QUIT:
            game_framework.quit()
        else:
            if event.type == SDL_KEYDOWN and event.key == SDLK_ESCAPE:
                game_framework.change_state(title_state)
            elif event.type == SDL_KEYDOWN and event.key == SDLK_h:
                game_framework.change_state(title_state)



def draw():
    clear_canvas()
    for n in range(50):
        if n % 2 == 0:
            tile.clip_draw(16, 48, 16, 16,
                           map.fixsize + (map.fixsize * 2) * (n % (map.windsizX / (map.fixsize * 2))),
                           map.fixsize + (map.fixsize * 2) * (n // (map.windsizX / (map.fixsize * 2))),
                           map.fixsize * 2, map.fixsize * 2)
        else:
            tile.clip_draw(144, 16, 16, 16,
                           map.fixsize + (map.fixsize * 2) * (n % (map.windsizX / (map.fixsize * 2))),
                           map.fixsize + (map.fixsize * 2) * (n // (map.windsizX / (map.fixsize * 2))),
                           map.fixsize * 2, map.fixsize * 2)
    image.clip_draw_to_origin(30, 0, 34, 32, 0, 150, 300, 300)
    font.draw(80, 405, 'HOW TO PLAY', (0, 0, 0))

    font.draw(80, 330, 'MOVE : ARROW KEY', (0, 0, 0))
    font.draw(80, 310, 'K : SKIP TURN', (0, 0, 0))
    font.draw(80, 290, 'P : VIEW STATUS', (0, 0, 0))
    font.draw(75, 250, '    ESC TO TITLE', (0, 0, 0))
    update_canvas()


def update():
    pass


def pause():
    pass


def resume():
    pass