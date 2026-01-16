import pygame.mixer
from pygame.mixer import Sound
from support import load_audio


pygame.mixer.init()

DIAMOND_SOUND = Sound("../sounds/diamond.wav")
PIG_DIE_SOUND = Sound("../sounds/pig/die.mp3")
PIG_SAY_SOUND = load_audio("../sounds/pig/say/")
CANNON_SHOT_SOUND = Sound("../sounds/cannon_shot.wav")
BOMB_BOOM_SOUND = Sound("../sounds/bomb_boom_sound.wav")
BOMB_BOOM_SOUND.set_volume(0.5)

JUMP_SOUND = Sound("../sounds/hero/jump.mp3")
HERO_DAMAGE_SOUND = Sound("../sounds/hero/get_damage.mp3")
ATTACK_SOUND = Sound("../sounds/hero/attack.mp3")

MAIN_MENU_MUSIC = Sound("../sounds/main_menu.mp3")
WIN_SOUND = Sound("../sounds/win.wav")
LOSE_SOUND = Sound("../sounds/lose.mp3")