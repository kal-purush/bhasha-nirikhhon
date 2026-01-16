import cv2 as cv
import numpy as np
import math
from collections import defaultdict

#OUTPUT_DIR = "/home/chaneylc/Desktop/Circles"
OUTPUT_DIR = r"C:\Users\marven\Documents\Spring-2020\CIS690\MeshCount"

#input is image using first-version soybeans mesh
#threshold function to find 3d printed mesh from an RGB image 1980x2592
#return is a blue background with red-masked mesh
def threshold(src):

    dst = src.copy()
    lab = cv.cvtColor(dst, cv.COLOR_RGB2LAB)

    #LAB threshold ranges that was found manually to work with
    #the lightbox
    L = [(0,44),(0,255),(0,255)]
    

    #use numpy logical and to find all pixels that satify the LAB range for each channel
    L_range = np.logical_and(L[0][0] < lab[:,:,0], lab[:,:,0] < L[0][1])
    a_range = np.logical_and(L[1][0] < lab[:,:,1], lab[:,:,1] < L[1][1])
    b_range = np.logical_and(L[2][0] < lab[:,:,2], lab[:,:,2] < L[2][1])

    valid_range = np.logical_and(L_range, a_range, b_range)

    #set all pixels
    lab[valid_range] = 0
    lab[np.logical_not(valid_range)] = 255

    #convert the image back to RGB
    rgb = cv.cvtColor(lab, cv.COLOR_LAB2RGB)

    #Debug statements to visualize the image outpu
    cv.imwrite("{}\Mesh.jpg".format(OUTPUT_DIR), rgb)

    return rgb


#function to replace a certain channel with value between a range
def replaceChannelValue(src, channelIndex, lo, hi, value):

	#src is the input image
	dst = src.copy()

	#all channel values specified from parameter
	values = dst[:,:,channelIndex]

	#indices that satisfy the given range from lo to hi
	indices = np.where((values>lo) & (values<=hi))

	#set the output image where the indices are true to the value given s.a black [0,0,0]
	dst[indices] = value

	return dst

def circleDetect(src):

	edges = cv.Canny(image=src, threshold1=240, threshold2=255)
	edges_smoothed = cv.GaussianBlur(edges, ksize=(5,5), sigmaX=10)
	lines = cv.HoughLinesP(edges_smoothed, rho=1, theta=1*np.pi/180, threshold=40, minLineLength=30, maxLineGap=25)
	img_lines = cv.cvtColor(src.copy(), cv.COLOR_BGR2RGB)
	line_nos = lines.shape[0]
	for i in range(line_nos):
	    x_1 = lines[i][0][0]
	    y_1 = lines[i][0][1]    
	    x_2 = lines[i][0][2]
	    y_2 = lines[i][0][3]    
	    cv.line(img_lines, pt1=(x_1,y_1), pt2=(x_2,y_2), color=(255,0,0), thickness=2)
	return img_lines

def  circularity(area, perimeter):

	return 4 * (math.pi) * (area / (perimeter**2))

#driver code
if __name__ == "__main__":

	#read the input file
	src = cv.imread("{}\IMG3.jpg".format(OUTPUT_DIR), 0)

	cv.imwrite("{}\Gray.png".format(OUTPUT_DIR), src)

	output = src.copy()

	edges = cv.Canny(src, threshold1=240, threshold2=255)

	cv.imwrite("{}\Edges.png".format(OUTPUT_DIR), edges)

	edges_smoothed = cv.GaussianBlur(edges, ksize=(5,5), sigmaX=10)

	contours, _ = cv.findContours(edges_smoothed, cv.RETR_EXTERNAL, cv.CHAIN_APPROX_SIMPLE)

	print(len(contours))

	#assume that there will be four equally sized coins that are bigger than the measured seeds
	#first create an mapping of all areas
	areaMap = defaultdict(int)

	for contour in contours:
		
		area = cv.contourArea(contour)
		
		areaMap[area] = contour

	#sort the keys by the area values, top 4 contours will be the coins
	sortedKeys = sorted(areaMap, reverse=True)

	if sortedKeys and len(sortedKeys) >= 4:

		coinKeys = sortedKeys[:4]

		print(len(sortedKeys))        

		circularities = []

		for k in coinKeys:
            
			contour = areaMap[k]

			perimeter = cv.arcLength(contour, True)

			circularities.append(circularity(k, perimeter))


		mean = sum(circularities) / float(len(circularities))

		variance = 0
        
		print(circularities)        

		for value in circularities:

			variance += (value-mean)**2

		variance /= len(circularities)

		print(variance)

		for k in coinKeys:

			contour = areaMap[k]

			area = cv.contourArea(contour)

			perimeter = cv.arcLength(contour, True)

			(x, y), radius = cv.minEnclosingCircle(contour)

			center = (int(x), int(y))

			#cv.circle(output, center, int(radius), (255,0,0), 3)

			M = cv.moments(contour)
			cX = int(M["m10"] / M["m00"])
			cY = int(M["m01"] / M["m00"])

            # Setting the epsilon calculation to 0.0001 since it gives the most precise   
            #approximation value. Going lower doesn't improve the precision of the values anymore.
			approx = cv.approxPolyDP(contour,0.0001*perimeter,True)
            
			print("appr: {}".format(len(approx)))            

			circularity = 4 * (math.pi) * (area / (perimeter**2))

			if circularity >= 0.8:
				cv.putText(output,"{} : {}".format(str(circularity)[:5], area),(cX,cY-500), cv.FONT_HERSHEY_SIMPLEX, 4,(0,0,0),4,cv.LINE_AA)
				cv.drawContours(output, [approx], -1, (255,0,0), 2)
                
				x,y,w,h = cv.boundingRect(contours[0])
				cv.rectangle(output,(x,y),(x+w,y+h),(0,255,0),2)
                
	# circles = cv.HoughCircles(edges_smoothed,cv.HOUGH_GRADIENT,1,100,
 #                            param1=500,param2=30,minRadius=200,maxRadius=200)

	# circles = np.uint16(np.around(circles))

	# print(circles)

	# for i in circles[0,:]:
	#     # draw the outer circle
	#     cv.circle(output,(i[0],i[1]),i[2],(255,255,255),2)
	#     # draw the center of the circle
	#     cv.circle(output,(i[0],i[1]),2,(255,255,255),3)

	cv.imwrite("{}\Detected.png".format(OUTPUT_DIR), output)

	#cv.imwrite("{}/Detected2.png".format(OUTPUT_DIR), circleDetect(src.copy()))
	# #create a mask image 'dst' of the mesh
	# dst = threshold(src)

	# #subtract the mesh from the original using opencv saturation subtract
	# sub = cv.subtract(dst, src)

	# cv.imwrite("{}/Subtracted.jpg".format(OUTPUT_DIR), sub)

	# #convert the image to grayscale
	# gray = cv.cvtColor(sub, cv.COLOR_RGB2GRAY)

	# #final threshold on gray image to segment seeds from background and mesh mask
	# ret, gray = cv.threshold(gray, 100, 255, cv.THRESH_BINARY)

	# cv.imwrite("{}/Threshed.jpg".format(OUTPUT_DIR), gray)

	#find contours, area threshold, count
    
	# Ignore the first 4 sorted keys since we assume that the first 4 keys correspond to the coins    
	seedKeys = sortedKeys[4:]
    
    # Create a copy of the source since drawContours() modifies the source image
	seedOutput = src.copy()
    
	seedCount = 0    
    
	for k in seedKeys:
        
		contour = areaMap[k]
        
		contour_area = cv.contourArea(contour)
        
        # Grab all contours whose area is over 5000 to ignore any noise that might have been identified as seeds
		if(contour_area > 5000):
            
			seedCount += 1

			cv.drawContours(seedOutput, [contour], -1, [255, 0, 0], 2)
            
            # identify the co-oridinates to draw a bounding rectangle around the identified contours
			x, y, w, h = cv.boundingRect(contour)
            
            # Draw a bounding rectangle around the identified contours
			cv.rectangle(seedOutput, (x, y), (x + w, y + h), (0, 255, 0), 2)   
            
			M = cv.moments(contour)            
			cx = int(M['m10']/M['m00'])
			cy= int(M['m01']/M['m00'])  
            
			radius = math.sqrt(contour_area/ math.pi)            
            
            # Plot the area of the contour (seed) next to the rectangle
			cv.putText(seedOutput, "{}: {}".format(str(contour_area), radius), (x, y), cv.FONT_HERSHEY_SIMPLEX, 1,(0,0,0), 4, cv.LINE_AA)                 
            
                     
	cv.imwrite("{}\SeedOutput.png".format(OUTPUT_DIR), seedOutput)
    
	print(seedCount)    