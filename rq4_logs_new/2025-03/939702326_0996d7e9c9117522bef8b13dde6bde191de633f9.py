import platform
import random
from PIL import ImageFont

def random_color():
    return (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))

def get_font():
    font_size = 20
    if platform.system() == 'Darwin':
        font = ImageFont.truetype('AppleGothic.ttf', size=font_size)
    elif platform.system() == 'Windows':
        font = ImageFont.truetype('malgun.ttf', size=font_size)
    else:
        font = ImageFont.load_default(size=font_size)
    
    return font