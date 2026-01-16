# Andrew Walsh, abw9yd, CS 1111, Fall 2016
import pygame
import gamebox
import random
screen_width = 1000
screen_length = 700
camera = gamebox.Camera(screen_width, screen_length)
counter_space = 0
counter_side = 0
counter_air = 0
life = 500
heal = 0
# Keeps character facing the same directions
orientation = 'R'
character = gamebox.from_color(screen_width/2, screen_length/2, 'blue', 10, 20)
character_speed = 0
enemies = [gamebox.from_color(random.randint(10, 990), 30, 'white', random.randint(10, 20), random.randint(20, 30))]
coins = []
coin_timer = []
coins_towards_reload_increase = 0
score = 0
difficulty_increasing = 0

bullets_r = []
bullets_l = []
shot_direction = 5


reload_time = 60
enemy_spawn_rate = (60)*7


reload = 60
velocity = 0
tic_cnt = [0,0,0,0]
# for_jump, for_sprite

curr_pos = 556
falling = False

def tick(keys):

    global bullets_l
    global bullets_r
    global character
    global character_speed
    global coins_towards_reload_increase
    global counter_space
    global counter_side
    global counter_air
    global curr_pos
    global difficulty_increasing
    global enemies
    global enemy_spawn_rate
    global falling
    global heal
    global life
    global orientation
    global reload
    global reload_time
    global tic_cnt
    global velocity
    global score
    global shot_direction
    camera.clear("black")

    for varb in range(len(tic_cnt)):
        tic_cnt[varb] += 1


#                            1000    //   700
#                            x center//y center//colour//width//height
    walls = [gamebox.from_color(500, 695, 'black', 1000, 10),
             gamebox.from_color(75, 600, 'blue', 200, 10),
             gamebox.from_color(400, 480, 'red', 50, 10),
             gamebox.from_color(290, 540, 'green', 50, 10),
             gamebox.from_color(600, 498, 'green', 200, 10),
             gamebox.from_color(900, 420, 'orange', 250, 10),
             gamebox.from_color(750, 660, 'red', 100, 10),
             gamebox.from_color(160, 440, 'pink', 200, 10),
             gamebox.from_color(825, 600, 'grey', 50, 10),
             gamebox.from_color(750, 560, 'cyan', 50, 10)
    ]
    sides = [
        gamebox.from_color(1, 350, 'black', 2, 700),
        gamebox.from_color(999, 350, 'black', 2, 700)
    ]

#    Variables

    invincibility = False
    coin_life_span = (60)*6
    character_speed = 4
    enemy_speed = 2
    heal_power = 20
    jump_power = -15
    sprite_speed = 8
# Number of seconds it takes for a new enemy to spawn
    coin_spawn_rate = (60)*5
# ####### Increasing Difficulty #########################

    coins_to_hasten_reload = 8
    reload_reduction = 4
    coins_to_increase_enemy_spawn_rate = 6
    amount_spawn_rate_increases = 30
    coins_to_heal = 10






# Sets the speed that the animation changes
    if tic_cnt[1] % sprite_speed == 0:
        tic_cnt[1] = 0
        counter_space += 1
    if tic_cnt[1] % sprite_speed == 0:
        tic_cnt[1] = 0
        counter_side += 1

    if counter_space == 6:
        counter_space = 0

    if counter_side == 4:
        counter_side = 0

# Creates 'Gravity'
    velocity += 1
    character.y += velocity

# **Sets boundaries of the screen

    if character.x > 1000:
        character.x = 1000
    if character.x < 0:
        character.x = 0
    if character.y < 30:
        character.y = 30
    if character.y > 690:
        character.y = 690

# **// Creates Jumping // Stops Sprite Overlapping
    for wall in walls:
        if character.bottom_touches(wall):
            velocity = 0
            if pygame.K_UP in keys:
                velocity = jump_power
        for koin in coins:
            if koin.bottom_touches(wall):
                koin.move_to_stop_overlapping(wall)
        if character.touches(wall):
            character.move_to_stop_overlapping(wall)
        camera.draw(wall)

# Test for falling
    if curr_pos != character.y:
        falling = True
    else:
        falling = False
    curr_pos = character.y

# Create Enemies

    tic_cnt[2] += 1
    if tic_cnt[2] >= enemy_spawn_rate:
        enemies.append(gamebox.from_color(random.randint(10, 990), 30, 'white', random.randint(10, 20), random.randint(20, 30)))
        tic_cnt[2] = 0


    # ############################ SHOOTING ###########################################################


    if reload > 0:
        reload -= 1
    if reload == 0:
        if pygame.K_SPACE in keys:
            if orientation == 'L':
                bullets_l.append(gamebox.from_color(character.x, character.y, 'yellow', 6, 4))
                reload = reload_time
            else:
                bullets_r.append(gamebox.from_color(character.x, character.y, 'yellow', 6, 4))
                reload = reload_time

    for c in range(len(bullets_l)):
        bullets_l[c].x -= 5
    for d in range(len(bullets_r)):
        bullets_r[d].x += 5

    for bullet_l in bullets_l:
        for side in sides:
            if bullet_l.touches(side):
                bullets_l.remove(bullet_l)
        for wall in walls:
            if bullet_l.touches(wall):
                bullets_l.remove(bullet_l)
        for enemy in enemies:
            if bullet_l.touches(enemy):
                enemies.remove(enemy)
                if bullet_l in bullets_l:
                    bullets_l.remove(bullet_l)
        camera.draw(bullet_l)

    for bullet_r in bullets_r:
        for side in sides:
            if bullet_r.touches(side):
                bullets_r.remove(bullet_r)
        for wall in walls:
            if bullet_r.touches(wall):
                bullets_r.remove(bullet_r)
        for enemy in enemies:
            if bullet_r.touches(enemy):
                enemies.remove(enemy)
                if bullet_r in bullets_r:
                    bullets_r.remove(bullet_r)
        camera.draw(bullet_r)


    if orientation == 'L':
        shot_direction = -5
    else:
        shot_direction = 5

# ############################ SHOOTING ###########################################################

    if pygame.K_RIGHT in keys:
        if character.x <= 988:
            character.x += 1
            if pygame.K_SPACE not in keys or falling:
                character.x += character_speed
        orientation = 'R'

    if pygame.K_LEFT in keys:
        if character.x >= 12:
            character.x -= 1
            if pygame.K_SPACE not in keys or falling:
                character.x -= character_speed
        orientation = 'L'


# ############# Health ################################################
    health = gamebox.from_color(500, 15, 'red', life, 30)
    for hurt in enemies:
        if character.touches(hurt):
            if life > 0:
                life -= 5
    if life == 0:
        score_display = gamebox.from_text(500, 350, 'Your final score is: '+str(score), 'Arial', 90, 'yellow', italic=True, bold=True)
        camera.draw(score_display)
        if invincibility == False:
            gamebox.pause()

    if heal == coins_to_heal and life < 500:
        heal = 0
        life += heal_power
# ############# Health ################################################

# ############# ENEMY #################################################

    for enemy in enemies:
        if enemy.x < character.x:
            enemy.x += enemy_speed
        elif enemy.x > character.x:
            enemy.x -= enemy_speed
        if enemy.y < character.y:
            enemy.y += enemy_speed
        elif enemy.y > character.y:
            enemy.y -= enemy_speed
        # if enemy.touches(enemies[enemies.index(enemy)-1]):
        #     enemy.move_to_stop_overlapping(enemies[enemies.index(enemy)-1])


# ############# ENEMY #################################################

# ############ Coins ##################################################

    tic_cnt[3] += 1
    if tic_cnt[3] == coin_spawn_rate:
        coins.append(gamebox.from_color(random.randint(15, 985), 15, 'yellow', 15, 15))
        coin_timer.append(coin_life_span)
        tic_cnt[3] = 0
    for coin in coins:
        coin.y += 5
        coin_timer[coins.index(coin)] -= 1
        if coin.touches(character):
            score += 1
            heal += 1
            coin_timer.remove(coin_timer[coins.index(coin)])
            coins.remove(coin)
            coins_towards_reload_increase += 1
            difficulty_increasing += 1
        for time_left in coin_timer:
            if time_left == 0:
                coins.remove(coins[coin_timer.index(time_left)])
                coin_timer.remove(time_left)
        camera.draw(coin)
    score_display = gamebox.from_text(15, 15, str(score), 'Arial', 20, 'yellow', italic=True, bold=True)

    if coins_towards_reload_increase == coins_to_hasten_reload and (reload_time-reload_reduction) >= 0:
        reload_time -= reload_reduction
        coins_towards_reload_increase = 0



    if difficulty_increasing == coins_to_increase_enemy_spawn_rate and enemy_spawn_rate > amount_spawn_rate_increases:
        enemy_spawn_rate -= amount_spawn_rate_increases
        difficulty_increasing = 0



# ############ Coins ##################################################

   # character = gamebox.from_color(character.x, character.y, 'blue', life//50, life//25)
# General Drawing area
    camera.draw(score_display)
    camera.draw(character)
    camera.draw(health)
    for evil in enemies:
        camera.draw(evil)
    for z in range(len(sides)):
        camera.draw(sides[z])




    # to check the mouse, use camera.mouse (for position) or camera.mouseclick (for buttons)

    # typically here you update the position of the characters
    # then you call camera.draw(box) for each GameBox you made

    # usually camera.display() should be the last line of the tick method




    camera.display()

ticks_per_second = 60
gamebox.timer_loop(ticks_per_second, tick)