import tkinter
from PIL import Image, ImageTk
import chap7fight

#drawingmap
def draw_map():
    for y in range(0, MAX_HEIGHT):
        for x in range(0, MAX_WIDTH):
            p = map_data[y][x]
            canvas.create_image(x*62+31, y*62+31, image=images[p])
    #display of brave
    canvas.create_image(brave_x*62+31, brave_y*62+31, image=images[4], tag="brave")

#check the destination
def check_move(x, y):
    global brave_x, brave_y, flag_key
    if x>=0 and x<MAX_WIDTH and y>=0 and y<MAX_HEIGHT:
        p = map_data[y][x]
        if p == 1:
            return
        elif p == 3:
            flag_key == Tru
            map_data[y][x] = 0
            canvas.delete("all")
            draw_map()
        elif p == 2:
            if flag_key == True:
                ending()
            else:
                return
        brave_x = x
        brave_y = y
        canvas.coords("brave", brave_x*62+31, brave_y*62+31)


#push buttons up
def click_button_up():
    check_move(brave_x, brave_y-1)

#push buttons down
def click_button_down():
    check_move(brave_x, brave_y+1)

#push buttons left
def click_button_left():
    check_move(brave_x-1, brave_y)

#push buttons right
def click_button_right():
    check_move(brave_x+1, brave_y)

#display the ending
def ending():
    canvas.delete("all")
    canvas.create_rectangle(0, 0, 620, 434, fill="black")
    canvas.create_text(300, 200, fill="white", font=("MS ゴシック", 15), text="""ゴールおめでとう。
    だが君の戦いはまだ始まったばかりだ。
    ・・・・つづく？""")

#comment

    #invalidate button
    button_up["state"] = "disabled"
    button_down["state"] = "disabled"
    button_left["state"] = "disabled"
    button_right["state"] = "disabled"


#window
root = tkinter.Tk()
root.title("ダンジョン")
root.minsize(840, 454)
root.option_add("*font", ["メイリオ", 14])

#canvas
canvas = tkinter.Canvas(width=620, height=434)
canvas.place(x=10, y=10)
canvas.create_rectangle(0, 0, 620, 434, fill="gray", tag="drawField")

#button
button_up = tkinter.Button(text="↑")
button_up.place(x=720, y=150)
button_up["command"] = click_button_up
button_down = tkinter.Button(text="↓")
button_down.place(x=720, y=210)
button_down["command"] = click_button_down
button_left = tkinter.Button(text="←")
button_left.place(x=660, y=180)
button_left["command"] = click_button_left
button_right = tkinter.Button(text="→")
button_right.place(x=780, y=180)
button_right["command"] = click_button_right

#scan
images = [ImageTk.PhotoImage(file='img6/chap6-mapfield.png'),
ImageTk.PhotoImage(file='img6/chap6-mapwall.png'),
ImageTk.PhotoImage(file='img6/chap6-mapgoal.png'),
ImageTk.PhotoImage(file='img6/chap6-mapkey.png'),
ImageTk.PhotoImage(file='img6/chap6-mapman.png')]


#mapdata
MAX_WIDTH = 10
MAX_HEIGHT = 7
map_data = [[1, 0, 1, 1, 1, 1, 1, 1, 1, 1],
[1, 0, 0, 1, 2, 0, 0, 1, 3, 1],
[1, 1, 0, 1, 1, 1, 0, 1, 0, 1],
[1, 0, 0, 0, 0, 0, 0, 1, 0, 1],
[1, 0, 1, 1, 1, 1, 1, 1, 0, 1],
[1, 0, 0, 0, 0, 0, 0, 0, 0, 1],
[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]]

#position of brave
brave_x = 1
brave_y = 0

#flag key
flag_key = False

#preparation for battle
fightmanager = chap7fight.FightManager()

draw_map()
root.mainloop()