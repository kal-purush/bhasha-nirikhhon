import SmartSprinklerModule as ssm
import csv
import matplotlib.pyplot as plt

### NEED TO CREATE A FUNCTION THAT READS THE MOTOR ANGLES!

Po = -35 #default

To = input("Desired tilt angle (deg): ")

ssm.rotate_motor('pan',Po*3.14159/180)
ssm.rotate_motor('tilt',To*3.14159/180)

ok = 0
im = ssm.capture_image(3,3)
FLAME,cxo,cyo,EDGE = ssm.find_centroid(im)
while not(FLAME) or EDGE or not(ok):
	print 'Bad positioning! Move Flame'
	
	plt.imshow(im)
	plt.plot(cxo,cyo,marker='o',color='r')
	plt.show()

	ok = input('Is this ok (1/0): ')

	im = ssm.capture_image(3,3)
	FLAME,cxo,cyo,EDGE = ssm.find_centroid(im)

R = input("What is the range (ft): ")

# Vary Pan and Tilt for +/- 10,5,3 degrees

changes = [20, -20, 10, -10, 5, -5, 3, -3]
for change in changes:
        if abs(To+change)<90:
                F = 0; cxo = 0; cyo = 0; E=0; cx = 0; cy = 0
                F,cxo,cyo,E = ssm.find_centroid(ssm.capture_image(3,3))

                ssm.rotate_motor('tilt',(To+change)*3.14159/180)
                F,cx,cy,E = ssm.find_centroid(ssm.capture_image(3,3))
                if not(E) and F:
                        ssm.write_csv(R,To,Po,To+change,Po,cxo,cyo,cx,cy)
                elif not(F):
                        ssm.write_csv(R,To,Po,To+change,Po,cxo,cyo,float('inf'),float('inf'))
                elif E:
                        ssm.write_csv(R,To,Po,To+change,Po,cxo,cyo,cx,cy,1)

                F = 0; cxo = 0; cyo = 0; E=0; cx = 0; cy = 0
                F,cxo,cyo,E = ssm.find_centroid(ssm.capture_image(3,3))

                ssm.rotate_motor('tilt',(To)*3.14159/180)
                F,cx,cy,E = ssm.find_centroid(ssm.capture_image(3,3))
                if not(E) and F:
                        ssm.write_csv(R,To,Po,To,Po,cxo,cyo,cx,cy)
                elif not(F):
                        ssm.write_csv(R,To,Po,To,Po,cxo,cyo,float('inf'),float('inf'))
                elif E:
                        ssm.write_csv(R,To,Po,To,Po,cxo,cyo,cx,cy,1)
        if abs(Po+change)<90:
                F = 0; cxo = 0; cyo = 0; E=0; cx = 0; cy = 0
                F,cxo,cyo,E = ssm.find_centroid(ssm.capture_image(3,3))

                ssm.rotate_motor('pan',(Po+change)*3.14159/180)
                F,cx,cy,E = ssm.find_centroid(ssm.capture_image(3,3))
                if not(E) and F:
                        ssm.write_csv(R,To,Po+change,To,Po,cxo,cyo,cx,cy)
                elif not(F):
                        ssm.write_csv(R,To,Po+change,To,Po,cxo,cyo,float('inf'),float('inf'))
                elif E:
                        ssm.write_csv(R,To,Po+change,To,Po,cxo,cyo,cx,cy,1)

                F = 0; cxo = 0; cyo = 0; E=0; cx = 0; cy = 0
                F,cxo,cyo,E = ssm.find_centroid(ssm.capture_image(3,3))

                ssm.rotate_motor('pan',(Po)*3.14159/180)
                F,cx,cy,E = ssm.find_centroid(ssm.capture_image(3,3))
                if not(E) and F:
                        ssm.write_csv(R,To,Po,To,Po,cxo,cyo,cx,cy)
                elif not(F):
                        ssm.write_csv(R,To,Po,To,Po,cxo,cyo,float('inf'),float('inf'))
                elif E:
                        ssm.write_csv(R,To,Po,To,Po,cxo,cyo,cx,cy,1)                        

import killmotors