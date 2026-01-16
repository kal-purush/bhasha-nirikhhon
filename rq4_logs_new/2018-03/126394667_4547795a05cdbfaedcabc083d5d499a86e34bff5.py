text_file = open("Z:/DGOUV022/Documents/test.txt","r")		
lines = []		
for line in text_file:		
	coord = line.split()	
	if len(coord) == 3:	
		d = ""
		coord.append(d)
		coord.append(d)
	coord_tup = tuple(coord)	
	lines.append(coord_tup)	
text_file.close()		
d = {}		
for cat,x,y,dx,dy in lines:		
	int_x = int(x)	
	int_y = int(y)	
	try:	
		int_dx=int(dx)
		int_dy=int(dy)
		co = [int_x,int_y,int_dx,int_dy]
	except ValueError:	
		co = [int_x,int_y]
	try:	
		d[cat].append(co)
	except KeyError:	
		d[cat]=[co]			