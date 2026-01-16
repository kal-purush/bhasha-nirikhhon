import numpy as np
import cv2
import sys,os
import Tkinter
from Tkinter import *
from PIL import Image, ImageTk
from ttk import Frame, Style
from spatial import East,North,Near,step2

 
def find_pc_dfs(x,y,pc,mark,oldrel):
	# implement floodfill algorithm
	trel = np.zeros((27, 3), bool)
	#left
	if x-1 > 0 and (x-1, y) not in mark:
		for i in xrange(27):
			trel[i] = [North(i, [x-1, y]), East(i, [x-1, y]), Near(i, [x-1, y])]
		mark.append((x-1,y))
		if np.all(trel == oldrel):
			pc.append((x-1,y))
			pc = find_pc_dfs(x-1,y,pc,mark,trel)

	#right
	if x+1 < image0.shape[1] and (x+1, y) not in mark:
		for i in xrange(27):
			trel[i] =  [North(i, [x+1, y]), East(i, [x+1, y]), Near(i, [x+1, y])]
		mark.append((x+1,y))
		if np.all(trel == oldrel):
			pc.append((x+1,y))
			pc = find_pc_dfs(x+1,y,pc,mark,trel)
	#up
	if y-1 > 0 and (x, y-1) not in mark:
		for i in xrange(27):
			trel[i] =  [North(i, [x, y-1]), East(i, [x, y-1]), Near(i, [x, y-1])]
		mark.append((x, y-1))
		if np.all(trel == oldrel):
			pc.append((x, y-1))
			pc = find_pc_dfs(x,y-1,pc,mark,trel)
	#down
	if y+1 < image0.shape[0] and (x,y+1) not in mark:
		for i in xrange(27):
			trel[i] =  [North(i, [x,y+1]), East(i, [x,y+1]), Near(i, [x,y+1])]
		mark.append((x,y+1))
		if np.all(trel == oldrel):
			pc.append((x,y+1))
			pc = find_pc_dfs(x,y+1,pc,mark,trel)

	return pc

def get_des(c, x, y):
	# print out description for point/building
	if c == 0:
		print 'Loading point cloud for starting point'
	else:
		print 'Loading point cloud for target point'
	bnum=0
	try:
		bnum = image0[y][x]
	except IndexError:
		print 'Click again!'
	if bnum != 0:
		for i in xrange(27):
			if i !=bnum:
				# print rel[i][bnum]
				print 'got it'
	return 'ok'

# def onObjectClick(event):
# 	print 'Got obj click', event.x, event.y, event.widget


def onCanvasClick(event):
	print 'Got canvas click', event.x, event.y, event.widget
	global count
	global image
	coor.append((event.x,event.y))
	if count == 0:
		color = (0,255,0)
		count = count + 1
		txt = get_des(count,event.x,event.y)
	elif count == 1:
		count = count + 1
		color = (255,0,0)
		txt = get_des(count,event.x,event.y)
	else:
		count = 0
		txt = 'please reset!'
	
	cur_rel = np.zeros((27, 3), bool)
	x = round(event.x)
	y = round(event.y)

	for i in range(27):
		cur_rel[i] = [North(i, [x, y]), East(i, [x, y]), Near(i, [x, y])]
	
	sys.setrecursionlimit(100000)
	point_cloud = find_pc_dfs(x,y,[(x,y)],[(x,y)],cur_rel)
	# change display
	for pix in point_cloud:
		# round to int and convert to int    
		xx = int(pix[0]) 
		yy = int(pix[1])
		tmp =  tuple(map(int, [xx,yy]))
		try:
			image.putpixel(tmp, color)
		except UnboundLocalError:
			print 'please reset!'

	img = ImageTk.PhotoImage(image)
	obj_img = Tkinter.Label(w, image=img)
	obj_img.place(x=0, y=0, width=image.size[0], height=image.size[1])
	w.mainloop()

# def onObjectClick(event):
# 	print 'Got object click', event.x, event.y, event.widget,
# 	print event.widget.find_closest(event.x, event.y) 

def step3():
	global coor
	global rel
	global MBR_list
	global COM_list
	global image0
	global labels
	global count
	global image
	global w
	count = 0
	coor = []
	filename='ass3-labeled.pgm'
	image0 = cv2.imread(filename,-1)
	
	rel,MBR_list,COM_list,labels = step2(image0)

	w = Tk()
	image = Image.open("campus.ppm")
	w.geometry('%dx%d' %(image.size[0] * 2,image.size[1]))

	# canvas = Canvas(w, width=image.size[0]*2, height=image.size[1])

	img = ImageTk.PhotoImage(image)
	obj_img = Tkinter.Label(w, image=img)
	obj_img.place(x=0, y=0, width=image.size[0], height=image.size[1])
	# obj_img = canvas.create_image(0,0, anchor=NW, image=img)
	# obj_text = canvas.create_text(image.size[0]+3, 10, anchor=W, font="Purisa", text="Welcome to CU Map")

	w.bind('<Double-1>', onCanvasClick)
	# canvas.tag_bind(obj_img, '<Double-1>', onObjectClick)



	# canvas.pack()
	w.mainloop()

def main():
	step3()


if __name__=='__main__':
	main()