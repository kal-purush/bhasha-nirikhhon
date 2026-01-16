#!/usr/bin/python

#https://en.wikipedia.org/wiki/YUV

import Image
import sys
import struct

if len(sys.argv) != 2 :
	print "give file as input"
	sys.exit(0)

image_name = sys.argv[1]

im = Image.open(image_name)
pix = im.load()
width, height = im.size

print width, height

#print len(pix[0, 0]) #rgba

y444 = bytearray(0)
u444 = bytearray(0)
v444 = bytearray(0)

for h in xrange(0, height) : 
	for w in xrange(0, width) : 
		r, g, b, a = pix[w, h] #rgba
		
		# ntsc standard
		y = 0.299 * r + 0.587 * g + 0.114 * b;
		u = -0.147 * r - 0.289 * g + 0.436 * b;
		v = 0.615 * r - 0.515 * g - 0.100 * b;

		y444.append(struct.pack("B", int(y) & 0xff))
		u444.append(struct.pack("B", int(u + 128) & 0xff))
		v444.append(struct.pack("B", int(v + 128) & 0xff))

u420 = bytearray(0)
for h in xrange(0, height/2) : 
	h2 = h * 2
	for w in xrange(0, width/2) : 
		w2 = w * 2
		u00 = u444[h2 * width + w2]
		u01 = u444[h2 * width + w2 + 1]
		u10 = u444[(h2 + 1) * width + w2]
		u11 = u444[(h2 + 1) * width + w2 + 1]
		u = (u00 + u01 + u10 + u11) / 4
		u420.append(struct.pack("B", int(u) & 0xff))

v420 = bytearray(0)
for h in xrange(0, height/2) : 
	h2 = h * 2
	for w in xrange(0, width/2) : 
		w2 = w * 2
		v00 = v444[h2 * width + w2]
		v01 = v444[h2 * width + w2 + 1]
		v10 = v444[(h2 + 1) * width + w2]
		v11 = v444[(h2 + 1) * width + w2 + 1]
		v = (v00 + v01 + v10 + v11) / 4
		v420.append(struct.pack("B", int(v) & 0xff))
		
new_name = '_'.join(image_name.split(".")[:-1]) + '_' + str(width) + 'x' + str(height) + ".yuv"
f = open(new_name, "wb")
f.write(y444)
f.write(u420)
f.write(v420)