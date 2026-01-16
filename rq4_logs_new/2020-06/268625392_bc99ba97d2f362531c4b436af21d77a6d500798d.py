from tkinter import *

root = Tk()
root.title = "Ankit's Calculator"

ent = Entry(root, width = 39 ,borderwidth = 4)
ent.grid(row = 0, column = 0, ipady = 20, columnspan = 3)

global current_res
current_res = 0
global operation
operation = ''

def button_click(num):
	
	current = ent.get()
	ent.delete(0,END)
	ent.insert(0,str(current) + str(num))

def button_clear():
	ent.delete(0,END)
	current_res = 0
	operation = ''

def button_add():
	global current_res
	global operation
	operation = "Addition"
	curr = float(ent.get())
	current_res = current_res + curr
	ent.delete(0,END)


def button_subtract():
	global current_res
	global operation
	curr = float(ent.get())
	if operation == '':
		current_res = current_res+curr
	else:
		current_res = current_res-curr
	operation = "Subtraction"
	
	ent.delete(0,END)

def button_multiply():
	global current_res
	global operation
	curr = float(ent.get())

	if operation == '':
		current_res = current_res+curr
	else:
		current_res = current_res*curr	
	operation = "Multiplication"
	ent.delete(0,END)

def button_divide():
	global current_res
	global operation
	curr = float(ent.get())
	if operation == '':
		current_res = current_res+curr
	else:
		if curr != 0:
			current_res = current_res/curr
		else:
			current_res = "YOU DON'T KNOW DIVISION YOU FAGGOT!!!"
			
	operation = "Division"
	ent.delete(0,END)

def button_equal():
	global current_res
	global operation
	if ent.get() != '':
		num = float(ent.get())
	else:
		num = 0
	ent.delete(0,END)
	if operation == "Addition":
		ent.insert(0, current_res+num)
	elif operation == "Subtraction":
		ent.insert(0, current_res-num)
	elif operation == "Multiplication":
		ent.insert(0, num*current_res)
	elif operation == "Division" and num!=0:
		ent.insert(0, current_res/num)
	elif operation == "Division" and num==0:
		ent.insert(0, "YOU DON'T KNOW DIVISION YOU FAGGOT!!!")
	else:
		ent.insert(0,0)
	
	operation = ''
	current_res = 0


buttons = []
col = 0
row = 1

for i in range(1,10):
	
	buttons.append(Button(root,text=i,padx = 30, pady = 20,command=lambda i = i: button_click(i)))
	
	buttons[i-1].grid(row=row,column = col)
	#x.grid(row=row,column = col)
	col +=1
	if col == 3:
		col = 0
		row+=1

buttons.append(Button(root,text=0,padx = 30, pady = 20,command = lambda: button_click(0)))
buttons[9].grid(row=4,column = 0)
buttons.append(Button(root,text='+',padx = 30, pady = 20,command = button_add))
buttons[10].grid(row=4,column = 1)
buttons.append(Button(root,text='-',padx = 30, pady = 20,command = button_subtract))
buttons[11].grid(row=4,column = 2)
buttons.append(Button(root,text='*',padx = 30, pady = 20,command = button_multiply))
buttons[12].grid(row=5,column = 0)
buttons.append(Button(root,text='Clear',padx = 61, pady = 20,command = button_clear, bg = "grey",fg = "white"))
buttons[13].grid(row=5,column = 1,columnspan = 2)
buttons.append(Button(root,text='/',padx = 30, pady = 20,command = button_divide))
buttons[14].grid(row=6,column = 0)

buttons.append(Button(root,text='=',padx = 71, pady = 20,command = button_equal,bg = "light green"))
buttons[15].grid(row=6,column = 1,columnspan = 2)


#print(*buttons)




root.mainloop()