from tkinter import *
from random import *


def pong():
    def move_ball():
        global dx,dy
        canvas.move(ball,dx,dy)
        fenetre.after(30,move_ball)
        x0_ball,y0_ball,x1_ball,y1_ball = canvas.coords(ball)
        x0_1,y0_1,x1_1,y1_1 = canvas.coords(paddle1)
        x0_2,y0_2,x1_2,y1_2 = canvas.coords(paddle2)

        if x0_ball <= 0:
            canvas.pack_forget()
            canvas.coords(ball,338, 238, 362, 262)
            menu()
        elif x0_ball < x1_1 and y1_ball > y0_1 and y0_ball < y1_1:
            dx = dx * -1
        elif y0_ball < 0:
            dy = dy * -1

        if x1_ball >= 700:
            canvas.pack_forget()
            canvas.coords(ball,338, 238, 362, 262)
            menu()
        elif x1_ball > x0_2 and y1_ball > y0_2 and y0_ball < y1_2:
            dx = dx * -1
        elif y1_ball > 500:
            dy = dy * -1

    def paddle_mouvement(event):
        x0_1,y0_1,x1_1,y1_1 = canvas.coords(paddle1)
        x0_2,y0_2,x1_2,y1_2 = canvas.coords(paddle2)
        touche = event.keysym
        if touche == "Up" and y0_2 > 0:
            y0_2=y0_2-10
            y1_2=y1_2-10
            canvas.coords(paddle2,x0_2,y0_2,x1_2,y1_2)
        elif touche == "Down" and y1_2 < 500:
            y0_2=y0_2+10
            y1_2=y1_2+10
            canvas.coords(paddle2,x0_2,y0_2,x1_2,y1_2)
        
        if touche == "z" and y0_1 > 0:
            y0_1=y0_1-10
            y1_1=y1_1-10
            canvas.coords(paddle1,x0_1,y0_1,x1_1,y1_1)
        elif touche == "s" and y1_1 < 500:
            y0_1=y0_1+10
            y1_1=y1_1+10
            canvas.coords(paddle1,x0_1,y0_1,x1_1,y1_1)

    fenetre.title("Pong")
    canvas = Canvas(fenetre,width=700,height=500,bg='#000000',cursor="none")
    ball = canvas.create_rectangle((338, 238, 362, 262),fill='white')
    paddle1 = canvas.create_rectangle(5,200,15,300,fill='white')
    paddle2 = canvas.create_rectangle(685,200,695,300,fill='white')
    
    
    canvas.focus_set()
    canvas.bind("<Key>", paddle_mouvement)
    canvas.pack()
    move_ball()
    fenetre.mainloop()
    fenetre.destroy()
    
def menu():
    fenetre.title("Pong's menu")
    
    ButtonJouer = Button(fenetre, text = "   Play   ", command=lambda:[ButtonJouer.pack_forget(),ButtonQuitter.pack_forget(),pong()])
    ButtonJouer.pack(padx = 10, pady = 10)
    
    ButtonQuitter = Button(fenetre, text = "   Exit    ", command = fenetre.destroy)
    ButtonQuitter.pack(padx = 10, pady = 10)

    fenetre.mainloop()
    fenetre.destroy()
    
    

fenetre = Tk()
dx = randrange(-8,8)
dy = randrange(-8,8)
menu()