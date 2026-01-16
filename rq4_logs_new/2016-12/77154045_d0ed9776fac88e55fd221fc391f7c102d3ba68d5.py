import random

def screen_width():
    return 320
def screen_height():
    return 240
    
def get_random_named_color():
    namedcolors = ['navy','blue','aqua','teal','olive','green','lime','yellow','orange','red','maroon','fuchsia','purple','black','gray','silver','white']
    return random.choice(namedcolors)
    
def draw_menu(screen,menuitems,textcolor, backgroundcolor):
    screen.rectangle(xy=(0,0), size=(screen_width(),25), color=backgroundcolor)
    for index in range(len(menuitems)):
        screen.text(menuitems[index],xy=(index*(screen_width()/4),0),font_size=20,color=textcolor,align='topleft')