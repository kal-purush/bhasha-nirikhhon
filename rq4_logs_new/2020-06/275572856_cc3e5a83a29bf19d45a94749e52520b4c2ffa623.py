

# # !/usr/bin/env python3
# '''Records measurments to a given file. Usage example:
# $ ./record_measurments.py out.txt'''
# #mport sys

import time
import math

import random

from rplidar import RPLidar
PORT_NAME = '/dev/ttyUSB0'

lidar = RPLidar(PORT_NAME)
lidar.set_pwm(500)


# Import and initialize the pygame library
import pygame
pygame.init()
# Set up the drawing window
screen = pygame.display.set_mode([1000, 1000])
font = pygame.font.Font(pygame.font.get_default_font(), 24)
# Fill the background with white
screen.fill((255, 255, 255))


# Based on LIDAR specs
nMinRange = 200
nMaxRange = 6000

# needed for placing things on the grid, what is the origin in the screen variable above, using center of screen
nXAdjust = 500
nYAdjust = 500
nScaleDown = .2   # makes image fit on screen, scales everything down

# how accurate do you want the match when comparing known map with current map
nImageMatchGoal =.4

# percentage
# cotrols the number or readings when you scan the room
nMapCoverage = .7


# used to round values consistently / experimentation
def CustomRound(nValue,nPrecision) :

    # value is starting amount
    # precision is the modulus  eg. 10 rounds to the nearest 10

    # -1 is rounded to 10's left of decimal

    return round(nValue,-1)



def PrintAtSpot(nradius,xpass,ypass) :

    #print(xpass,ypass)
    if xpass >= 0 and ypass >= 0:
        xpass = int((nScaleDown*xpass) + nXAdjust)
        ypass = int(nYAdjust - (nScaleDown*ypass))

    #Quad 2
    elif xpass >= 0 and ypass <= 0:
        xpass = int((nScaleDown*xpass) + nXAdjust)
        ypass = abs(int(nScaleDown*ypass)) + nYAdjust

    elif xpass <= 0 and ypass <= 0:
        xpass = nXAdjust - abs(int(nScaleDown*xpass))
        ypass = abs(int(nScaleDown*ypass)) + nYAdjust

    elif xpass <= 0 and ypass >= 0:
        xpass = nXAdjust-abs(int(nScaleDown*xpass))
        ypass = int(nYAdjust-(nScaleDown*ypass))

    pygame.draw.circle(screen, (100, 150, 50), (int(xpass), int(ypass)), nradius)
    pygame.display.flip()


def PaintMeasures() :

        pygame.draw.circle(screen, (255, 0, 255), (nXAdjust, nYAdjust), 10)
        text_surface = font.render( str(0), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust, nYAdjust ))


        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust, nYAdjust+ int(1000 * nScaleDown) ), 5)
        text_surface = font.render(str(-1000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust, nYAdjust+int(1000*nScaleDown) ))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust, nYAdjust + int(2000 * nScaleDown)), 5)
        text_surface = font.render(str(-2000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust, nYAdjust + int(2000 * nScaleDown)))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust, nYAdjust + int(3000 * nScaleDown)), 5)
        text_surface = font.render( str(-3000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust, nYAdjust + int(3000 * nScaleDown)))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust, nYAdjust + int(4000 * nScaleDown)), 5)
        text_surface = font.render(str(-4000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust, nYAdjust + int(4000 * nScaleDown)))


        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust, nYAdjust + int(-1000 * nScaleDown)), 5)
        text_surface = font.render(str(1000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust, nYAdjust + int(-1000 * nScaleDown)))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust, nYAdjust + int(-2000 * nScaleDown)), 5)
        text_surface = font.render(str(2000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust, nYAdjust + int(-2000 * nScaleDown)))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust, nYAdjust + int(-3000 * nScaleDown)), 5)
        text_surface = font.render(str(3000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust, nYAdjust + int(-3000 * nScaleDown)))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust, nYAdjust + int(-4000 * nScaleDown)), 5)
        text_surface = font.render(str(4000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust, nYAdjust + int(-4000 * nScaleDown)))



        pygame.draw.circle(screen, (0, 0, 255), ( nXAdjust + int(nScaleDown*1000), nYAdjust), 5)
        text_surface = font.render(str(1000), True, (0, 0, 0))
        screen.blit(text_surface, ( nXAdjust + int(nScaleDown*1000) , nYAdjust) )

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust + int(nScaleDown * 2000), nYAdjust), 5)
        text_surface = font.render(str(2000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust + int(nScaleDown * 2000), nYAdjust))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust + int(nScaleDown * 3000), nYAdjust), 5)
        text_surface = font.render(str(3000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust + int(nScaleDown * 3000), nYAdjust))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust + int(nScaleDown * 4000), nYAdjust), 5)
        text_surface = font.render(str(4000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust + int(nScaleDown * 4000), nYAdjust))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust + int(nScaleDown * 5000), nYAdjust), 5)
        text_surface = font.render(str(5000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust + int(nScaleDown * 5000), nYAdjust))


        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust + int(nScaleDown * -1000), nYAdjust), 5)
        text_surface = font.render(str(-1000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust + int(nScaleDown * -1000), nYAdjust))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust + int(nScaleDown * -2000), nYAdjust), 5)
        text_surface = font.render(str(-2000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust + int(nScaleDown * -2000), nYAdjust))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust + int(nScaleDown * -3000), nYAdjust), 5)
        text_surface = font.render(str(-3000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust + int(nScaleDown * -3000), nYAdjust))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust + int(nScaleDown * -4000), nYAdjust), 5)
        text_surface = font.render(str(-4000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust + int(nScaleDown * -4000), nYAdjust))

        pygame.draw.circle(screen, (0, 0, 255), (nXAdjust + int(nScaleDown * -5000), nYAdjust), 5)
        text_surface = font.render(str(-5000), True, (0, 0, 0))
        screen.blit(text_surface, (nXAdjust + int(nScaleDown * -5000), nYAdjust))

        pygame.display.flip()


# paint room on screen
def RefreshMap(_PositionArray, _Color=(255, 0, 255)):

    # grid values
    PaintMeasures()

    for nIteration in range(len(_PositionArray)):

        xpass = _PositionArray[nIteration][0]
        ypass = _PositionArray[nIteration][1]

        # print(xpass,ypass)
        if xpass >= 0 and ypass >= 0:
            xpass = int((nScaleDown * xpass) + nXAdjust)
            ypass = int(nYAdjust - (nScaleDown * ypass))

        # Quad 2
        elif xpass >= 0 and ypass <= 0:
            xpass = int((nScaleDown * xpass) + nXAdjust)
            ypass = abs(int(nScaleDown * ypass)) + nYAdjust

        elif xpass <= 0 and ypass <= 0:
            xpass = nXAdjust - abs(int(nScaleDown * xpass))
            ypass = abs(int(nScaleDown * ypass)) + nYAdjust

        elif xpass <= 0 and ypass >= 0:
            xpass = nXAdjust - abs(int(nScaleDown * xpass))
            ypass = int(nYAdjust - (nScaleDown * ypass))

        pygame.draw.circle(screen, _Color, (int(xpass), int(ypass)), 5)

    pygame.display.flip()


# rotate the new map to assist with matching to the new map

def RotateNewMap(nDegrees) :

    for nIteration in range(len(nXYNewPoints))   :
        nAngleFromNewOrigin[nIteration] = nAngleFromNewOrigin[nIteration]+nDegrees
        x = CustomRound(nDistancefromNewOrigin[nIteration] * math.sin((math.pi / 180) * nAngleFromNewOrigin[nIteration]), -1)
        y = CustomRound(nDistancefromNewOrigin[nIteration] * math.cos((math.pi / 180) * nAngleFromNewOrigin[nIteration]), -1)
        nXYNewPoints[nIteration] = (x, y)


# moves new map around, changes the readings
def ShiftNewMap(nShiftX,nShiftY):

  nNewDist = math.sqrt(nShiftX ^ 2 + nShiftY ^ 2)

  for nIteration in range(len(nXYNewPoints)) :
        nXYNewPoints[nIteration] =  (nXYNewPoints[nIteration][0]+nShiftX,   nXYNewPoints[nIteration][1]+nShiftY)
        nDistancefromNewOrigin[nIteration] = nNewDist

        #nAngleFromNewOrigin.append(nAngleFromOrigin[nIteration])
        #nXYNewPoints[nIteration] = (nXYNewPoints[nIteration][0] + nShiftX, nXYNewPoints[nIteration][1] + nShiftY)
        # nAngleFromNewOrigin.append(nAngleFromOrigin[nIteration])
        #print(nDistancefromOrigin[nIteration] )


try:

    nXYPoints =[]
    nDistancefromOrigin = []
    nAngleFromOrigin = []


    # used to see how many duplicate readings you avoided.   when you hit target percentage over last 1000 readings, exit.  the map is accurate enough
    nFound =0
    nTries =0

    print('Recording measurements... ', time.time())


    # collect points for initial map
    for measurment in lidar.iter_measurments():

        #convert angle and distance to x and y
        # from lidar library
        # position 1 = accuracy
        # 2 is angle from lidar unit
        # 3 is distance from lidar unit
        if measurment[1] == 15 and  measurment[3] < nMaxRange and measurment[3] > nMinRange :

            x = CustomRound(measurment[3] * math.sin((math.pi / 180) * measurment[2]), -1)
            y = CustomRound(measurment[3] * math.cos((math.pi / 180) * measurment[2]), -1)

            # avoids duplicates
            if (x,y) in nXYPoints:
                nFound = nFound +1
            else:
                nAngleFromOrigin.append(round(measurment[2],0))
                nDistancefromOrigin.append(int(measurment[3]))
                # nXPositions.append(x)
                # nYPositions.append(y)
                nXYPoints.append( (x,y) )


            nTries = nTries + 1

             # if over the last 1000 iterrations, the hit percentages is high, then exit
            if nFound/nTries > nMapCoverage :
                print("exiting measurements")
                break
            elif nTries%1000 ==0:
                nTries =1
                nFound =0
            # else:
            #     print("tries, arraysize", (nFound/nTries) * 100, len(nAngleFromOrigin))

    # update display
    RefreshMap(nXYPoints)
    print(" first map complete, x",len(nXYPoints), " readings ", time.time())


    print("MOVE and press key")
    input()

    # variables for 2nd map
    nXYNewPoints = []
    nAngleFromNewOrigin = []
    nDistancefromNewOrigin = []

    ############################## make reasings
    lidar.stop()
    lidar.disconnect()
    lidar = RPLidar(PORT_NAME)
    lidar.set_pwm(500)

    for measurment in lidar.iter_measurments():

        #convert angle and distance to x and y

        if measurment[1] == 15 and  measurment[3] < nMaxRange and measurment[3] > nMinRange :

            x = CustomRound(measurment[3] * math.sin((math.pi / 180) * measurment[2]), -1)
            y = CustomRound(measurment[3] * math.cos((math.pi / 180) * measurment[2]), -1)

            # avoids duplicates
            if (x,y) in nXYNewPoints:
                nFound = nFound +1

            else:
                nAngleFromNewOrigin.append(round(measurment[2],0))
                nDistancefromNewOrigin.append(int(measurment[3]))
                nXYNewPoints.append( (x,y) )


            nTries = nTries + 1

             # if over the last 1000 iterrations, the hit percentages is high, then exit
            if nFound/nTries > nMapCoverage :
                print("exiting new measurements")
                break
            elif nTries%1000 ==0:
                nTries =1
                nFound =0
            # else:
            #     print("tries, arraysize", (nFound/nTries) * 100, len(nAngleFromOrigin))


    # adds 2nd map to the screen
    RefreshMap(nXYNewPoints,(0, 255,0))



    ########################################   TRY TO FIND THE NEW POSITION BY MATCHING THE MAPPING
    ############# code not finished, currently only searches top right quadrant of the grid

    nCurXAdjust = 0
    nCurYAdjust = 0
    nFindCount = 0

    # # temp, generates a map using the original instead of scannning for test purposes only

    Count =0
    for Count in range(360):

        print("degree",Count)

        # searching a grid increments of 10 to match the accuracy of the initial readings
        # fewer readings speeds up search

        for nCurXAdjust in range(0,1000,10) :

            # different distances
            for nCurYAdjust in range(0,1000,10) :

                nFindCount = 0

                # compare 20 different random points at this distance and the current angle
                nIteration=0
                for nIteration in range(20) :

                    # pick random matches not 20 in a row
                    nRandom = random.randint(0, len(nXYNewPoints)-1 )


                    # calculate the adjusted position of the 2nd map to see if it matches the original
                    nNewSpot = ( nXYNewPoints[nRandom][0]+ nCurXAdjust ,nXYNewPoints[nRandom][1]+ nCurYAdjust)
                    if nNewSpot in nXYPoints :
                        nFindCount = nFindCount+1


                #if close , try again with more points to confirm the final location
                if nFindCount/nIteration > nImageMatchGoal:

                    print("2nd try at ",nCurXAdjust,nCurYAdjust, Count, nFindCount/nIteration  )
                    nFindCount = 0
                    nIteration=0

                    for nIteration in range(30) :
                        # pick random matches not 20 in a row
                        nRandom = random.randint(0, len(nXYNewPoints)-1 )

                        nNewSpot = ( nXYNewPoints[nRandom][0]+ nCurXAdjust ,nXYNewPoints[nRandom][1]+ nCurYAdjust)
                        if nNewSpot in nXYPoints :
                            nFindCount = nFindCount+1

                    print("2nd RESULTS at ", nCurXAdjust, nCurYAdjust, Count, nFindCount / nIteration)


                # found a match when you it target accuracy goal, exit
                if nFindCount/nIteration > nImageMatchGoal:
                    print("match ")
                    print("x.y,% ", nCurXAdjust, nCurYAdjust,  nFindCount / nIteration)

                    # shift the map and see the 2 are perfectly overlayed (proof you found the current location from the origin)
                    input("key for shift")
                    ShiftNewMap(nCurXAdjust,nCurYAdjust)
                    screen.fill((255, 255, 255))
                    RefreshMap(nXYPoints)
                    RefreshMap(nXYNewPoints,(0, 255,0))
                    break

            if nFindCount / nIteration > nImageMatchGoal:
                break


        if nFindCount / nIteration > nImageMatchGoal:
            break

        # rotate 1 degree at a time and then compare at all positions on the grid again
        print("rotate")
        RotateNewMap((1))
        screen.fill((255, 255, 255))
        RefreshMap(nXYPoints)
        RefreshMap(nXYNewPoints,(0, 255,0))


    print("sleeping", time.time())
    time.sleep(1000)

except KeyboardInterrupt:
    print('Stoping.')

finally:

    lidar.stop()
    lidar.disconnect()

    # Done! Time to quit.
    pygame.quit()
