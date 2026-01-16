from tkinter import *
import time

import random
WIDTH = 800
HEIGHT = 400

class Ball:
    def __init__(self,canvas, color,size,x,y,xspeed,yspeed):
        self.canvas = canvas # 캔버스 객체
        self.color = color # Ball의 색상
        self.size = size # Ball의 크기
        self.x = x # Ball의 x좌표
        self.y = y # Ball의 y좌표
        self.xspeed = xspeed # Ball의 수평 방향 속도
        self.yspeed = yspeed # Ball의 수직 방향 속도
        self.id = canvas.create_oval(x,y,x+size,y+size, fill=color)
        def move(self): # Ball을 이동시키는 함수
            self.canvas.move(self.id, self.xspeed, self.yspeed)
            (x1,y1,x2,y2) = self.canvas.coords(self.id) # 공의 현재 위치를 얻는다.
            (self.x, self.y) = (x1,y1)
            if x1 <= 0 or x2 >= WIDTH: # 공의 x좌표가 음수이거나, x좌표가 오른쪽 경계를 넘으면
                self.xspeed = -self.xspeed # 속도의 부호를 반전시킨다.
                if y1 <= 0 or y2 >= HEIGHT: # 공의 y좌표가 음수이거나, y 좌표가 오른쪽 경계를 넘으면
                    self.yspeed = -self.yspeed # 속도의 부호를 반전시킨다.   

bullets = []       

def fire(event):
    bullets.append(Ball(canvas, "red",10,150,spaceship.y+50,10,0))   

def getScore(score):
    score = score + 1
    return score      

def keyPress_UP(event):
    spaceship.yspeed = -3
# 키보드 아래쪽방향키 눌림 이벤트에 함수를 연결한다. 
def keyPress_Down(event):
    spaceship.yspeed = +3
# 키보드를 풀어줬을 때 이벤트처리
def keyRelease(event):
    spaceship.yspeed =0     
                
window = Tk()
canvas = Canvas(window, width=WIDTH,height=HEIGHT)
canvas.pack()
canvas.bind("<Button-1>",fire) # 마우스 버튼 클릭 이벤트에 함수를 연결한다. 
canvas.bind("<KeyPress-Up>",keyPress_UP) # 키보드 위쪽방향키 눌림 이벤트에 함수를 연결한다. 
canvas.bind("<KeyPress-Down>",keyPress_Down) # 키보드 아래쪽방향키 눌림 이벤트에 함수를 연결한다. 
canvas.bind("<KeyRelease>",keyRelease) # 키보드를 풀어줬을 때 이벤트 처
# 키보드 포커스를 갖게 한다
canvas.focus_set()  

score = 0
var = StringVar()
label = Label(window, textvariable = var, relief = RAISED)                

spaceship = Ball(canvas,"green",100,100,200,0,0)
enemy = Ball(canvas,"red",100,500,200,0,5)

while True:
    print(spaceship.yspeed)
    var.set("score: "+str(score))
    label.pack()
    for bullet in bullets:
        bullet.move()
 # 포탄이 화면을 벗어나면 삭제한다.
        if (bullet.x + bullet.size) >= WIDTH:
            canvas.delete(bullet.id)     
            bullets.remove(bullet)
            if bullet.x > enemy.x and bullet.x < enemy.x+enemy.size and bullet.y > enemy.y and bullet.y < enemy.y+enemy.size:
                score = getScore(score)
                enemy.move()
                spaceship.move()
                print(spaceship.x,spaceship.y)
                window.update()
                time.sleep(0.03)
        
        
        