#Justas Cyvas
#Intelligent Manipulation in Clutter

from openravepy import *
import numpy

env = Environment() # create openrave environment
env.SetViewer('qtcoin') # attach viewer (optional)
env.Load('/home/justas/openrave/src/data/project.env.xml') # load a simple scene

robot=env.GetRobots()[0]
manip = robot.GetActiveManipulator()
ikmodel = databases.inversekinematics.InverseKinematicsModel(robot,iktype=IkParameterization.Type.Transform6D)
if not ikmodel.load():
    ikmodel.autogenerate()


# 8 different grasps
oTee1 = numpy.array([[  1.00000000e+00,   1.22192725e-07,  -1.19171965e-07,
          3.47264059e-03],
       [  1.19171965e-07,  -9.99686782e-01,  -2.50267460e-02,
          6.62944722e-02],
       [ -1.22192725e-07,   2.50267460e-02,  -9.99686782e-01,
          2.62707526e-02],
       [  0.00000000e+00,   0.00000000e+00,   0.00000000e+00,
          1.00000000e+00]])
oTee2 = numpy.array([[  2.37482244e-02,   1.17585616e-07,  -9.99717971e-01,
          3.90461244e-02],
       [  8.90174121e-03,  -9.99960356e-01,   2.11342572e-04,
          7.31541038e-02],
       [ -9.99678339e-01,  -8.90424968e-03,  -2.37472840e-02,
         -4.33071104e-03],
       [  0.00000000e+00,   0.00000000e+00,   0.00000000e+00,
          1.00000000e+00]])
oTee3 = numpy.array([[ -1.01539864e-02,   9.53162635e-02,   9.95395251e-01,
         -2.58575178e-02],
       [ -6.72533545e-08,  -9.95446570e-01,   9.53211769e-02,
          6.07311308e-02],
       [  9.99948447e-01,   9.67822993e-04,   1.01077574e-02,
         -1.35561413e-02],
       [  0.00000000e+00,   0.00000000e+00,   0.00000000e+00,
          1.00000000e+00]])
oTee4 = numpy.array([[ -9.92667996e-01,  -2.97161410e-02,  -1.17163135e-01,
          2.18411284e-02],
       [  2.95466875e-02,  -9.99558332e-01,   3.18329868e-03,
          6.23814210e-02],
       [ -1.17205983e-01,  -3.01823813e-04,   9.93107580e-01,
         -3.90135159e-02],
       [  0.00000000e+00,   0.00000000e+00,   0.00000000e+00,
          1.00000000e+00]])
oTee5 = numpy.array([[  1.00000000e+00,   1.19296260e-07,   1.19209272e-07,
          1.27952309e-02],
       [ -1.19209215e-07,   9.99999734e-01,  -7.29918426e-04,
          5.95620531e-02],
       [ -1.19296317e-07,   7.29918426e-04,   9.99999734e-01,
         -4.31119522e-02],
       [  0.00000000e+00,   0.00000000e+00,   0.00000000e+00,
          1.00000000e+00]])
oTee6 = numpy.array([[  1.74181279e-02,   7.29605999e-04,   9.99848027e-01,
         -2.77164572e-02],
       [ -7.55016305e-08,   9.99999734e-01,  -7.29715387e-04,
          5.95621095e-02],
       [ -9.99848293e-01,   1.26347858e-05,   1.74181233e-02,
          1.88826884e-03],
       [  0.00000000e+00,   0.00000000e+00,   0.00000000e+00,
          1.00000000e+00]])
oTee7 = numpy.array([[ -9.99982300e-01,  -4.46150623e-06,  -5.94969212e-03,
         -1.81005062e-03],
       [ -1.20114499e-07,   9.99999734e-01,  -7.29683583e-04,
          5.95623335e-02],
       [  5.94969380e-03,  -7.29669954e-04,  -9.99982034e-01,
          4.31115758e-02],
       [  0.00000000e+00,   0.00000000e+00,   0.00000000e+00,
          1.00000000e+00]])
oTee8 = numpy.array([[  8.24627509e-02,  -4.46147693e-06,  -9.96594147e-01,
          5.86724836e-02],
       [  7.27552711e-04,   9.99999734e-01,   5.57243110e-05,
          5.95323923e-02],
       [  9.96593882e-01,  -7.29669954e-04,   8.24627322e-02,
          1.56485815e-03],
       [  0.00000000e+00,   0.00000000e+00,   0.00000000e+00,
          1.00000000e+00]])
		  
grasplist = [oTee1, oTee2,oTee3,oTee4,oTee5,oTee6,oTee7,oTee8] # make a set of 8 grasps
manipprob = interfaces.BaseManipulation(robot) # create the interface for basic manipulation programs

#make a list of 3 possible obstacles
obj1 = env.GetKinBody('mug1')
obj2 = env.GetKinBody('mug2')
obj3 = env.GetKinBody('mug6')
objects = [obj1,obj2,obj3]

mainObj = env.GetKinBody('mug3') # assign the main object to a variable

#a function that takes the current main object and the list of obstacles and returns a new list of obstacles
def getCollisionList(currentMainObj,currentList):
    
    target = currentMainObj 
    collObj = []

    #disabling objects
    for obj in objects:
        obj.Enable(False)
    #enabling main object
    target.Enable(True)
    #enabling used objects
    for obj in currentList:
        obj.Enable(True)

    wTo = target.GetTransform() # assigning the transform on an object to a variable

    #finding a valid grasp
    for grasp in grasplist:
        wTee = numpy.dot(wTo,grasp)
    
        Tgoal = wTee
        sol = manip.FindIKSolution(Tgoal, IkFilterOptions.CheckEnvCollisions) # get collision-free solution
        if sol != None:
            break

    #getting trajectory
    traj = manipprob.MoveManipulator(sol,execute=False,outputtrajobj=True)
    
    #interpolating trajectory
    numWP = traj.GetNumWaypoints()
    wp = []
    wp.append(traj.GetWaypoint(-numWP)[0:7])
    for i in range (numWP,1,-1):
        current = traj.GetWaypoint(-i)[0:7]
        following = traj.GetWaypoint(-i+1)[0:7]
        inc = numpy.subtract(following,current)
        inc = numpy.divide(inc,20)
        for j in range (1,20):
            wp.append(numpy.add(current,j*inc))
        wp.append(following)

    #enabling objects
    for obj in objects:
        obj.Enable(True)
        
    #adding objects to the colliding object list
    for w in wp:
        robot.SetDOFValues(w,manip.GetArmIndices())
        #res = manipprob.MoveManipulator(w)
        for obj in objects:
            if env.CheckCollision(robot,obj):
                collObj.append(obj)
                #collObj = getCollisionList(obj,collObj)
                obj.Enable(False)
        
    return collObj

raw_input() # press enter to begin
complete = False
collObj = []
newCollisions = []
newMain = mainObj
while not complete:
    size = len(collObj)
    newCollisions = getCollisionList(newMain,collObj)[::-1]
    collObj = collObj + newCollisions
    print(collObj)
    if (len(collObj)==0):
        complete = True
        break
    newMain = collObj[len(collObj)-1]
    if (size== len(collObj)):
        complete = True
    robot.SetDOFValues([0,0,0,0,0,0,0],manip.GetArmIndices())
    

    
collObj = collObj[::1] # reversing the list
collObj.append(mainObj) # appending the initial main object to the end of the list

#enabling all objects
for obj in objects:
    obj.Enable(True)

#removing all obstacles
for obj in collObj:
    target = obj
    pose = target.GetTransformPose()#collObj = getCollisionList(mainObj,collObj) + collObj

    wTo = target.GetTransform()
    #finding a valid grasp
    for grasp in grasplist:
        wTee = numpy.dot(wTo,grasp)
    
        Tgoal = wTee
        sol = manip.FindIKSolution(Tgoal, IkFilterOptions.CheckEnvCollisions) # get collision-free solution
        if sol != None:
            break
    res = manipprob.MoveManipulator(sol)
    robot.WaitForController(0) # wait
            
    taskprob = interfaces.TaskManipulation(robot) # create the interface for task manipulation programs
    taskprob.CloseFingers() # close fingers until collision
    robot.WaitForController(0) # wait
    robot.Grab(target)
    # move manipulator to all zeros, set jitter to 0.04 since cup is initially colliding with table
    manipprob.MoveManipulator(numpy.zeros(len(manip.GetArmIndices())),jitter=0.15)
    robot.WaitForController(0) # wait
    taskprob.ReleaseFingers(target)
    env.Remove(target)
    robot.WaitForController(0) # wait