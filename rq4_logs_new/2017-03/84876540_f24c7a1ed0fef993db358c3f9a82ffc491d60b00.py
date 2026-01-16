import png
import os
import numpy as np


def identify_colors(img_struct, img):
	if img_struct[3]['planes'] == 1:
		palette = [2 if (p[2] > p[1] and p[2] > p[0] and sum(p) / 3 < 130 and sum(p) / 3 > 135)
					else (1 - sum(p) / (3 * 255.0)) 
				   for p in img_struct[3]['palette']]
		img = [[palette[x] for x in l] for l in img]
	else:

		img = [[2 if l[i + 2] > l[i + 1] and l[i + 2] > l[i] and sum(l[i : i + 3]) / 3 > 80 and sum(l[i : i + 3]) / 3 <= 230 else 
				1 - int(round((l[i] + l[i + 1] + l[i + 2]) / (3 * 255.0)))
					for i in range(0, len(l), 3)] 
				for l in img]
	
	for _ in range(1):
		count_2s = [[0] * len(l) for l in img]
		for i in range(len(img)):
			for j in range(len(img[i])):
				if img[i][j] == 2:
					for (a, b) in [(a, b) for a in [0, 1, -1] for b in [0, 1, -1]]:
						if i + a >= 0 and \
						   j + b >= 0 and \
						   i + a < len(img) and \
						   j + b < len(img[i]):
						    count_2s[i + a][j + b] += 1
		#print len(img[i])
		count_2s = [[min([1, x / 3]) for x in l] for l in count_2s]

		img = [[count_2s[i][j] * 2 if img[i][j] == 2 else img[i][j]
				for j in range(len(img[i]))] 
				for i in range(len(img))]
	
	'''img1 = [[x/2 for x in l] for l in img]
	g = open(problem['title'] + '.png', 'wb')
	writer = png.Writer(len(img1[0]), len(img1), greyscale=True, bitdepth=1)
	writer.write(g, img1)
	g.close()'''
	return img

def read_problem(problem, png_file, txt_file):
	with open(txt_file) as f:
		problem['title' ] = f.readline().rstrip()
		problem['type'  ] = f.readline().rstrip()
		problem['result'] = int(f.readline().rstrip())

	f = open(png_file)
	r=png.Reader(file=f)
	img_struct = r.read()
	img = list(img_struct[2])
	img = identify_colors(img_struct, img)
	f.close()

	size = (len(img), len(img[0]))
	x = 0
	y = 0
	while img[x][y] != 2:
		y += 1
		if y== len(img[0]):
			y = 0
			x += 1
	
	window_size = (x, y)
	while img[x][window_size[1]] == 2:
		x += 1
	while img[window_size[0]][y] == 2:
		y += 1
	window_size = (x - window_size[0], y - window_size[1])
	#print window_size
	
	x = 1
	y = 1
	windows = []
	found = 0
	while x < size[0] and y < size[1]:
		if img[x][y] == 2 or \
		(found and img[x + 1][y] == 2):
			found = 1
			new_window = [img[x + i][y: y + window_size[1]] for i in range(window_size[0])]
			windows.append(np.array(new_window))
			y += window_size[1]
			if y >= size[1]:
				y = 0
				x += window_size[0]

		y += 1
		if y >= size[1]:
			y = 0
			x += 1 + found * window_size[0]
			found = 0
	
	#print len(windows)
	problem['windows'] = windows


def write_problem(problem, out_file):
	#print len(problem['windows'])
	with open(out_file, 'w+') as f:
		f.write('== Attributes ==\n')
		f.write('title=' + problem['title'] + '\n')
		f.write('type= '+ problem['type'] + '\n')
		f.write('result=' + str(problem['result']) + '\n')
		f.write('window_size=' + str(problem['windows'][0].shape) + '\n\n')
		f.write('== Input ==' + '\n')
		
		aux = {0:'0', 1:'1', 2:'0'}
		for window in problem['windows'][0:3]:
			#print len(window)
			#print window.shape
			for line in window:
				f.write(''.join([aux[x] for x in line]))
				f.write('\n')
			f.write('\n')
		f.write('== Output ==' + '\n')
		for window in problem['windows'][4:]:
			for line in window:
				f.write(''.join([aux[x] for x in line]))
				f.write('\n')
			f.write('\n')



if __name__ == '__main__':
	problem_dir = '../Problem_images/resized'
	problem_txt_dir = '../Problems_txt'
	output_dir = '../Problems'
	problems = []
	index = 0
	#for folder in os.listdir(problem_dir):
	#	folder_name = problem_dir + os.path.sep + folder
	folder_name = '../Problem_images/resized'
	for file_name in os.listdir(folder_name):
		problem = {}

		txt_file = os.path.join(problem_txt_dir, file_name.split('.')[0] + '.txt')
		png_file = os.path.join(folder_name, file_name)
		out_file = os.path.join(output_dir, str(index) + '.txt')
		index += 1
		#print png_file
		read_problem(problem, png_file, txt_file)
		write_problem(problem, out_file)