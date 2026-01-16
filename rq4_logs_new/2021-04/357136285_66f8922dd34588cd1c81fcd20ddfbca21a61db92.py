import turtle
from tkinter import *
from tkinter import messagebox as tmsg


root = Tk()

#p1 = PhotoImage(file = 'cube.png')
 
#root.iconphoto(False, p1)

root.title("Miller Indices Simulator")
root.geometry("400x300")
root.configure(bg = "aquamarine")
root.minsize(height = 2, width = 407)


#function


def abouter():
    print(tmsg.showinfo("College Project", "A simple simulator to have a better understanding about miller indices !"))

def close_info():
    a = tmsg.askyesno("Close","Are you sure ?")
    if a == True:
        return root.destroy()
    else :
        pass



wn = turtle.Screen()

def reset_instance():
	one.delete(0,"end")
	two.delete(0,"end")
	three.delete(0,"end")
	axis1 = turtle.Turtle()
	axis2 = turtle.Turtle()
	axis3 = turtle.Turtle()
	cube = turtle.Turtle()

	cod = turtle.Turtle()
	plane = turtle.Turtle()
	plane = turtle.Turtle()
	wn.reset()
	wn.update()


def getvisual():
	Label(root,bg = "aquamarine",text = f'Miller Indices = ({number1.get()}{number2.get()}{number3.get()})',font = 20).pack()


	global wn

	wn.title("Miller Indices")


	def draw_axes():
		axis1 = turtle.Turtle()
		axis1.penup()				
		axis1.goto(10,-40)
		axis1.pendown()
		axis1.forward(300)
		axis1.write("X")

		axis2 = turtle.Turtle()
		axis2.penup()
		axis2.goto(10,-40)
		axis2.left(90)
		axis2.pendown()
		axis2.forward(300)
		axis2.write("Y")

		axis3 = turtle.Turtle()
		axis3.penup()
		axis3.goto(10,-40)
		axis3.right(90+59)
		axis3.pendown()
		axis3.forward(250)
		axis3.right(31)
		axis3.forward(10)
		axis3.write("Z")


	draw_axes()

	

	cod = turtle.Turtle()

	def cube(n,m):
		cube = turtle.Turtle()
		cube.speed(0)
		def goto(x,y):
			cube.penup()
			cube.goto(x,y)
			cube.pendown()

		def square():
			for i in range(4):
				cube.forward(200)
				cube.right(90)

		goto(-90-n,100-m)
		square()

		goto(10-n,160-m)
		square()

		cube.goto(-90-n,100-m)
		cube.forward(200)
		cube.goto(210-n,160-m)

		cube.right(90)
		cube.forward(200)
		cube.goto(110-n,-100-m)
		cube.right(90)

		cube.forward(200)
		cube.goto(10-n,-40-m)
		cube.penup()
		cube.goto(10000,111000)


	cod.penup()
	cod.goto(10,-40)
	cod.pendown()
	cod.color("red","red")
	cod.begin_fill()


	global neg_int
	neg_int = []
	def draw_cod(x,y,z):
		
		if x== -1 :
			neg_int.append(200)
		elif x != -1 and z != -1 :
			neg_int.append(0)
		if y== -1 :
			neg_int.append(200)
		elif y != -1 and z != -1 :
			neg_int.append(0)
		if z == -1 :
			neg_int.extend([-100,-60])

		
		cube(neg_int[0],neg_int[1])
		global x_info,y_info,z_info
		if x != 0 and y != 0 and z != 0:
			#for x 
			cod.penup()
			cod.forward(195/x)
			cod.right(90)
			cod.pendown()
			cod.circle(5)
			x_info = cod.position()

			cod.penup()
			cod.goto(10,-40)
			cod.pendown()

			
			cod.right(180)

			#for y
			cod.penup()
			cod.forward(195/y)
			cod.right(90)
			cod.pendown()
			cod.circle(5)
			y_info = cod.position()

			cod.penup()
			cod.goto(10,-40)
			cod.pendown()

			#for z
			cod.penup()
			cod.right(59+90)
			cod.forward(111.2/z)
			cod.right(90)
			z_info = cod.position()

			cod.pendown()
			cod.circle(5)

			cod.end_fill()

			cod.penup()
			cod.goto(10,-40)

			draw_plane(x_info,y_info,z_info)

		elif (x==0 and y==0) or (y==0 and z==0) or (x==0 and z==0) :
			if x != 0 :
				cod.begin_fill()
				cod.penup()
				cod.forward(195/x)
				cod.right(90)
				cod.pendown()
				cod.circle(5)
				info_cod = cod.position()
				cod.color('green','green')
				cod.goto(info_cod[0],info_cod[1]+200)
				info_cod = cod.position()
				cod.color('red','red')
				cod.circle(5)
				cod.color('green','green')
				cod.goto(info_cod[0]-100,info_cod[1]-60)
				info_cod = cod.position()
				cod.color('red','red')
				cod.circle(5)
				cod.color('green','green')
				cod.goto(info_cod[0],info_cod[1]-200)
				info_cod = cod.position()
				cod.color('red','red')
				cod.circle(5)
				cod.color('green','green')
				cod.goto(info_cod[0]+100,info_cod[1]+60)
				info_cod = cod.position()
				cod.color('green','green')

				cod.end_fill()
				cod.penup()
				cod.goto(10,-40)
				cod.pendown()

			if y != 0 :
				cod.left(90)
				cod.begin_fill()
				cod.penup()
				cod.forward(195/y)
				cod.right(90)
				cod.pendown()
				cod.circle(5)
				info_cod = cod.position()
				cod.color('green','green')
				cod.goto(info_cod[0]-100,info_cod[1]-60)
				info_cod = cod.position()
				cod.color('red','red')
				cod.circle(5)
				cod.color('green','green')
				cod.goto(info_cod[0]+200,info_cod[1])
				info_cod = cod.position()
				cod.color('red','red')
				cod.circle(5)
				cod.color('green','green')
				cod.goto(info_cod[0]+100,info_cod[1]+60)
				info_cod = cod.position()
				cod.color('red','red')
				cod.circle(5)
				cod.color('green','green')
				cod.goto(info_cod[0]-200,info_cod[1])
				info_cod = cod.position()
				cod.color('green','green')

				cod.end_fill()
				cod.penup()
				cod.goto(10,-40)
				cod.pendown()

			if z != 0 :
				cod.right(90+59)
				cod.begin_fill()
				cod.penup()
				cod.forward(111.2/z)
				cod.pendown()
				cod.circle(5)
				info_cod = cod.position()
				cod.color('green','green')
				cod.goto(info_cod[0],info_cod[1]+200)
				info_cod = cod.position()
				cod.color('red','red')
				cod.circle(5)
				cod.color('green','green')
				cod.goto(info_cod[0]+200,info_cod[1])
				info_cod = cod.position()
				cod.color('red','red')
				cod.circle(5)
				cod.color('green','green')
				cod.goto(info_cod[0],info_cod[1]-200)
				info_cod = cod.position()
				cod.color('red','red')
				cod.circle(5)
				cod.color('green','green')
				cod.goto(info_cod[0]-200,info_cod[1])
				info_cod = cod.position()
				cod.color('green','green')

				cod.end_fill()
				cod.penup()
				cod.goto(10,-40)
				cod.pendown()



			print("yess")

		else :
			constraint = 0
			target = 0
			if x == 0 :
				target = 0
			elif y == 0 :
				target = 1
			elif z == 0 :
				constraint = 90
				target = 2


			coordinates = []
			#for x
			if x != 0:
				cod.penup()
				cod.forward(195/x)
				cod.right(90)
				cod.pendown()
				cod.circle(5)
				x_info = cod.position()

				coordinates.append(x_info)

				cod.penup()
				cod.goto(10,-40)
				cod.pendown()

			cod.left(90)	

			if y != 0:
				#for y
				cod.left(constraint)
				cod.penup()
				cod.forward(195/y)
				cod.right(90)
				cod.pendown()
				cod.circle(5)
				y_info = cod.position()

				coordinates.append(y_info)

				cod.penup()
				cod.goto(10,-40)
				cod.pendown()

			if z != 0:
				#for z
				cod.penup()
				cod.right(90+59)
				cod.forward(111.2/z)
				cod.right(90)
				z_info = cod.position()

				coordinates.append(z_info)

				cod.pendown()
				cod.circle(5)


						
			draw_plane1(coordinates[0],coordinates[1],target)

			pass






	def draw_plane(x,y,z):
		plane = turtle.Turtle()
		plane.color('green','green')
		plane.begin_fill()

		plane.penup()
		plane.goto(x[0],x[1])
		plane.pendown()
		plane.goto(y[0],y[1])
		plane.goto(z[0],z[1])
		plane.goto(x[0],x[1])

		plane.end_fill()



	def draw_plane1(x,y,target):
		list_cood = [[-200,0],[0,-200],[100,60]]
		plane = turtle.Turtle()
		plane.pensize(3)
		plane.color('green','green')
		plane.begin_fill()

		plane.penup()
		plane.goto(x[0],x[1])
		plane.pendown()
		plane.goto(y[0],y[1])
		#plane.right(90+59)
		plane.goto(y[0]-list_cood[target][0],y[1]-list_cood[target][1])
		plane.color('red','red')
		plane.circle(5)
		plane.color('green','green')
		plane.goto(x[0]-list_cood[target][0],x[1]-list_cood[target][1])
		plane.color('red','red')
		plane.circle(5)
		plane.color('green','green')
		plane.goto(x[0],x[1])

		plane.end_fill()




	draw_cod(int(number1.get()),int(number2.get()),int(number3.get()))

	cube(neg_int[0],neg_int[1])
	turtle.done()




#menu

close = Menu(root)

close.add_command(label = "About" ,command = abouter )
close.add_command(label = "Close" ,command = close_info )
root.config(menu = close )





frame_1 = Frame(root, bg = "red", borderwidth = 8 )

header = Label(frame_1 ,text = "Draw your own lattice planes !",font = "comicsans 15 bold",relief =
               GROOVE, padx = 5)

header.pack()
frame_1.pack()

Label(bg = "aquamarine").pack()

Label(text = "Enter Miller Indices",font = "comicsans 15 bold",relief =
               GROOVE, padx = 5).pack()

#entry
number1 = StringVar()  
number2 = StringVar()
number3 = StringVar()


one = Entry(root, relief = GROOVE, bg = "light grey" ,font = "fixedsys" ,textvariable = number1)
one.pack()
two = Entry(root, relief = GROOVE, bg = "light grey" ,font = "fixedsys" ,textvariable = number2)
two.pack()
three = Entry(root, relief = GROOVE, bg = "light grey",font = "fixedsys" ,textvariable = number3)
three.pack()


Label(bg = "aquamarine").pack()

#button
Button(text = "VIEW",command = getvisual).pack()
Button(text = "RESET",command = reset_instance).pack()



root.mainloop()