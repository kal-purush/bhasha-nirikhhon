from poormishaengine.poorengine import *
from poormishaengine.enginetypes import *

import pyray as rl
from poormishaengine import poorengine

engine = poorengine.PoorEngine(800, 600, "PoorMishaEngine")
actions_input = poorengine.InputActions()

actions_input.bind_action("drag", [rl.KeyboardKey.KEY_SPACE])
actions_input.bind_action("left", [rl.KeyboardKey.KEY_LEFT])
actions_input.bind_action("right", [rl.KeyboardKey.KEY_RIGHT])
actions_input.bind_action("jump", [rl.KeyboardKey.KEY_UP])

bounce_factor = 0.5
position = Vector2(100, 0)
velocity = Vector2(0, 0)
rotation_degress: float = 90.0
gravity = 0.5

SCALE_X = 20
SCALE_Y = 20


# Платформи
platforms = [
    rl.Rectangle(0, 240, 1000, 500),
    rl.Rectangle(500, 140, 1000, 500),
    rl.Rectangle(100, 10, 300, 200),
]

def rect_vs_rect(a: rl.Rectangle, b: rl.Rectangle):
    return (
        a.x < b.x + b.width and
        a.x + a.width > b.x and
        a.y < b.y + b.height and
        a.y + a.height > b.y
    )


def is_on_floor(player_rect):
    if is_on_wall(player_rect=player_rect):
        return False
    for platform in platforms:
        if (
            player_rect.y + player_rect.height >= platform.y and
            player_rect.y + player_rect.height <= platform.y + 5 and
            player_rect.x + player_rect.width > platform.x and
            player_rect.x < platform.x + platform.width
        ):
            return True
    return False
def is_on_wall(player_rect):
    for platform in platforms:
        touching_left = (
            player_rect.x <= platform.x + platform.width and
            player_rect.x >= platform.x + platform.width - 5
        )
        touching_right = (
            player_rect.x + player_rect.width >= platform.x and
            player_rect.x + player_rect.width <= platform.x + 5
        )
        vertical_overlap = (
            player_rect.y + player_rect.height > platform.y and
            player_rect.y < platform.y + platform.height
        )
        if vertical_overlap and (touching_left or touching_right):
            return True
    return False


last_mouse_pos = rl.get_mouse_position()
def lerp(a, b, t):
    return a + (b - a) * t
last_frame_is_on_floor = False

def snap(value, step):
    return round(value / step) * step
visual_scale_x = SCALE_X
visual_scale_y = SCALE_Y
@engine.process("phyc")
def phyc():
    global position, velocity, last_mouse_pos, rotation_degress,last_frame_is_on_floor, visual_scale_x,visual_scale_y

    visual_scale_x = SCALE_X
    visual_scale_y = lerp(visual_scale_y,SCALE_Y,0.4)

    engine.rendering_engine.draw_text(f"{velocity.x:.2f} і {velocity.y:.2f} \n rotation_degress {rotation_degress}", Vector2(0, 0), 30, rl.RED)

    dragging = actions_input.is_action_pressed("drag")
    mouse_pos = rl.get_mouse_position()

    player_rect = rl.Rectangle(position.x, position.y, SCALE_X, SCALE_Y)

    if dragging:
        velocity = Vector2(mouse_pos.x - last_mouse_pos.x, mouse_pos.y - last_mouse_pos.y)
        position = mouse_pos
    else:
        
        velocity.y += gravity

        
        if actions_input.is_action_pressed("left"):
            velocity.x = -4
        elif actions_input.is_action_pressed("right"):
            velocity.x = 4
        elif actions_input.is_action_pressed("jump") and is_on_floor(player_rect):
            velocity.y -= 10
        

        velocity.x = lerp(velocity.x, 0, 0.1)
        
        if not is_on_floor(player_rect):
            rotation_degress += velocity.x
        else:
            rotation_degress = lerp(rotation_degress, snap(rotation_degress,90), 0.5)
        position.x += velocity.x
        player_rect = rl.Rectangle(position.x, position.y, SCALE_X, SCALE_Y)

        for platform in platforms:
            if rect_vs_rect(player_rect, platform):
                if velocity.x > 0:
                    position.x = platform.x - SCALE_X
                elif velocity.x < 0:
                    position.x = platform.x + platform.width
                velocity.x = 0
                break

        
        position.y += velocity.y
        player_rect = rl.Rectangle(position.x, position.y, SCALE_X, SCALE_Y)

        for platform in platforms:
            if rect_vs_rect(player_rect, platform):
                if velocity.y > 0:
                    position.y = platform.y - SCALE_Y
                elif velocity.y < 0:
                    position.y = platform.y + platform.height

                if abs(velocity.y) > 1:  
                    visual_scale_y = 10
                    velocity.y = -velocity.y * bounce_factor
                else:
                    velocity.y = 0  
                break


    last_mouse_pos = mouse_pos

    
    origin = Vector2(visual_scale_x / 2, visual_scale_y / 2)
    draw_pos = Vector2(position.x + origin.x, position.y + origin.y)

    player_rect = rl.Rectangle(draw_pos.x, draw_pos.y, visual_scale_x, visual_scale_y)
    engine.rendering_engine.draw_rect_pro(player_rect, rl.GRAY,origin,rotation_degress)


    for platform in platforms:
        engine.rendering_engine.draw_rect(platform, rl.BLACK)
    

engine.run()