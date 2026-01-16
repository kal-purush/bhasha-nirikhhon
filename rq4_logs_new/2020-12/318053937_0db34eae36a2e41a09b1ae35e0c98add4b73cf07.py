from os import listdir, system
from PIL import Image as im, ImageDraw as imdrw
from random import randint, choice

system("cls")

PATH = 'Images'
SCALE = 50 # Taille des carrés
DIVS = 150 # Nombre d'images en hauteurs

if __name__ == "__main__":
	files = listdir(f'Drive/{PATH}/')
	print(f'Loading {len(files)} images from Drive/{PATH}/...')
	images = {}
	
	for file in files:
		if file:
			imgb = im.open(f"Drive/{PATH}/{file}")
			imgr =  im.new('RGB', (SCALE, SCALE))
			cW, cH = imgb.size
			moy = 0
			if cW >= SCALE and cH >= SCALE:
				for x in range(SCALE):
					for y in range(SCALE):
						size = min(cH, cW)
						coords = int(x/SCALE * size), int(y/SCALE*size)
						cc = sum(imgb.getpixel(coords))//3
						imgr.putpixel((x, y), (cc, cc, cc))
						moy += cc
		if not moy//SCALE**2 in images:
		 	images[moy//SCALE**2] = []
		images[moy//SCALE**2].append(imgr)

	print(f'Scaled {len(images)} images from: Drive/{PATH}/{file}...')

	ref = im.open(f'Drive/{PATH}/Chat.jpeg')
	print(f"Drive/{PATH}/Chat.jpeg loaded")
	refW, refH = ref.size
	W, H = DIVS * refW // refH, DIVS
	res = im.new('RGB', (W * SCALE, H * SCALE))

	ctx = imdrw.Draw(res)
	for x in range(W):
		for y in range(H):
			coords = int(x/W*refW), int(y/H*refH)
			c = ref.getpixel(coords)
			x1 = x * SCALE 
			y1 = y * SCALE 
			x2 = (x+1) * SCALE 
			y2 = (y+1) * SCALE
			c = sum(c)//3
			# ctx.rectangle([x1, y1, x2, y2], fill = (c, c, c), width=0)
			if c in images:
				res.paste(choice(images[c]), (x1, y1))

print(f'Creating {PATH}.png...')
res.save(PATH + '.png')
system(PATH + '.png')