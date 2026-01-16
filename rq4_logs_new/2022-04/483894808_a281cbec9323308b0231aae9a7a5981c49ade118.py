from ctypes import c_int, CDLL
from time import sleep
from tkinter import *


g_logic = CDLL("g_logic.dll")
tk = Tk()
tk.title("Gomoku")
tk.wm_attributes("-topmost", 1)
tk.resizable(0, 0)
canvas = Canvas(tk, width=395, height=420, bd=0, highlightthickness=0)
canvas.pack()


class Cell:
	def __init__(self,canvas,x,y):
		self.canvas = canvas
		self.B = Button(tk, width=3, height=1, command=self.update_game_info)
		self.DEFAULT_BUTTON_COLOR = self.B['bg']
		self.x_pos, self.y_pos = x, y
		# 25 is padding size between buttons, whereas 20 is button size)
		# indent is (+10 -25 = -15)
		self.B.place(x=(x+1)*25-15, y=(y+1)*25-15, width=20, height=20)

	def update_game_info(self):
		global acting_side
		global field_info
		global last_move
		global last_player
		global YSIZE
		
		if winner==0 and self.B['bg']==self.DEFAULT_BUTTON_COLOR:
			if last_player==1:
				self.B.config(bg='green')
				field_info[self.y_pos*YSIZE + self.x_pos] = 1
				last_move = [self.x_pos, self.y_pos]
				last_player = 2
				acting_side.config(bg='red')

			elif last_player==2:
				self.B.config(bg='red')
				field_info[self.y_pos*YSIZE + self.x_pos] = 2
				last_move = [self.x_pos, self.y_pos]
				last_player = 1
				acting_side.config(bg='green')


winner = 0
last_player = 1
last_move = [-1,-1]
XSIZE, YSIZE = 15, 15
buttons = [Cell(canvas,x,y) for y in range(YSIZE) for x in range(XSIZE)]
field_size = XSIZE*YSIZE
field_info = [0 for x in range(field_size)]
acting_side = Button(tk, width=3, height=1, bg='green')
acting_side.place(x=12.5+25*(XSIZE-1), y=10+25*YSIZE, width=15, height=15)

while 1:
	# c_last_move = (c_int * 2)(*last_move)
	# c_field_info = (c_int * field_size)(*field_info)
	# winner = g_logic.find_winner(last_player, c_last_move, c_field_info)

	if winner==1:
		canvas.create_text(165, 400, text='Green Victory',font=('Courier',15))
	elif winner==2:
		canvas.create_text(165, 400, text='Red Victory',font=('Courier',15))
	elif (winner==0) and (field_info.count(0)==0):
		canvas.create_text(165, 400, text='Draw',font=('Courier',15))
		
	tk.update_idletasks()
	tk.update()
	sleep(0.01)