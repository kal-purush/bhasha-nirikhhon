import math
from main.geoms.geoms import lerp, pointRightOfLine, pointLeftOfLine, getDistance, getArea
from main.geoms.circular_facet import getCircumcircle, isMajorArc, getCenter

#Linear facet: ['linear', intersects]
#Corner facet: ['corner', intersects]
#Arc facet: ['arc', arccenter, arcradius, arcintersects]
#Curved corner facet: ['curvedcorner, prevcenter, nextcenter, prevradius, nextradius, intersects]

#Velocity should be a function of time and position: v(t, p)
def advectPoint(p, velocity, t, h, mode='RK4'):
    if mode == 'RK1':
        v = velocity(t, p)
        pfinal = [p[0]+h*v[0], p[1]+h*v[1]]
    elif mode == 'RK4':
        v1 = velocity(t, p)
        p2 = [p[0]+h/2*v1[0], p[1]+h/2*v1[1]]
        v2 = velocity(t+h/2, p2)
        p3 = [p[0]+h/2*v2[0], p[1]+h/2*v2[1]]
        v3 = velocity(t+h/2, p3)
        p4 = [p[0]+h*v3[0], p[1]+h*v3[1]]
        v4 = velocity(t+h, p4)

        pfinal = [p[0]+h/6*(v1[0]+2*v2[0]+2*v3[0]+v4[0]), p[1]+h/6*(v1[1]+2*v2[1]+2*v3[1]+v4[1])]
    return pfinal

def getNormal(facet, p):
    if facet.name == 'linear':
        if getDistance(facet.pLeft, facet.pRight) == 0: print("getNormal({}, {})".format(facet, p))
        tangent = [(facet.pRight[0]-facet.pLeft[0])/getDistance(facet.pLeft, facet.pRight), (facet.pRight[1]-facet.pLeft[1])/getDistance(facet.pLeft, facet.pRight)]
        normal = [tangent[1], -tangent[0]]
        return normal
    elif facet.name == 'arc':
        if getDistance(p, facet.center) == 0: print("getNormal({}, {})".format(facet, p))
        if facet.radius > 0:
            normal = [(p[0]-facet.center[0])/getDistance(p, facet.center), (p[1]-facet.center[1])/getDistance(p, facet.center)]
        else:
            normal = [(facet.center[0]-p[0])/getDistance(p, facet.center), (facet.center[1]-p[1])/getDistance(p, facet.center)]
        return normal
    #TODO: corner facets?
    else:
        print("Improper facet in call to getNormal")
        print("getNormal({}, {})".format(facet, p))
        return None

def isDegenFacet(facet, threshold):
    if facet.name in ['linear', 'arc']:
        return getDistance(facet.pLeft, facet.pRight) < threshold
    else:
        print("Improper facet in isDegenFacet")
        print("isDegenFacet({}, {})".format(facet, threshold))

class Facet:

    def __init__(self, name, pLeft, pRight):
        self.name = name
        self.pLeft = pLeft
        self.pRight = pRight

        self.advect_collinearity_threshold = 1e6 #If advected facet has radius larger than this, convert it to a line
        self.equality_threshold = 1e-6 #If values are within this threshold, call them equal

    def __str__(self):
        return f"{self.name}"

    def advected(self, velocity):
        pass

class LinearFacet(Facet):
    def __init__(self, pLeft, pRight):
        super().__init__('linear', pLeft, pRight)
        self.curvature = 0
        self.midpoint = lerp(self.pLeft, self.pRight, 0.5)

    def advected(self, velocity, t, h, mode='RK4'):
        #Advect by using 3 control points: 2 endpoints + midpoint
        #Returns either LinearFacet or ArcFacet
        shiftpLeft = advectPoint(self.pLeft, velocity, t, h, mode=mode)
        shiftpRight = advectPoint(self.pRight, velocity, t, h, mode=mode)
        shiftmid = advectPoint(self.midpoint, velocity, t, h, mode=mode)
        [shiftcirclecenter, shiftcircleradius] = getCircumcircle(shiftpLeft, shiftmid, shiftpRight)
        if shiftcircleradius is None or shiftcircleradius > self.advect_collinearity_threshold:
            #Collinear: advect by creating linear facet
            return LinearFacet(shiftpLeft, shiftpRight)
        else:
            #Not collinear: advect by creating arc facet
            if pointRightOfLine(shiftcirclecenter, shiftpLeft, shiftpRight):
                #Circumcenter on right of line, need to invert radius
                shiftcircleradius *= -1

            #if getDistance(shiftpLeft, shiftpRight) < 1e-10:
            #    print("Mini facet: {}".format(self))

            return ArcFacet(shiftcirclecenter, shiftcircleradius, shiftpLeft, shiftpRight)

    def update_endpoints(self, new_pLeft, new_pRight):
        return LinearFacet(new_pLeft, new_pRight)

    def __str__(self):
        return "['{}', [{}, {}]]".format(self.name, self.pLeft, self.pRight)

    def __eq__(self, other_facet):
        if isinstance(other_facet, self.__class__) and getDistance(other_facet.pLeft, self.pLeft) < self.equality_threshold and getDistance(other_facet.pRight, self.pRight) < self.equality_threshold:
            return True
        return False

class ArcFacet(Facet):
    def __init__(self, center, radius, pLeft, pRight):
        super().__init__('arc', pLeft, pRight)
        self.center = center
        if abs(radius) < getDistance(pLeft, pRight)/2:
            print("Radius is too small: radius {}, distance {}".format(radius, getDistance(pLeft, pRight)))
            if radius < 0:
                self.radius = -getDistance(pLeft, pRight)/2
            else:
                self.radius = getDistance(pLeft, pRight)/2
        else:
            self.radius = radius
        if self.radius == 0:
            self.curvature = float('inf')
        else:
            self.curvature = 1/self.radius
        self.is_major_arc = isMajorArc(self.pLeft, self.pRight, self.center, self.radius)
        self.midpoint = self.getMidpoint()

    #TODO: deal with case when arc is perfectly pi radians? divide by zero issue
    def getMidpoint(self):
        mid = lerp(self.pLeft, self.pRight, 0.5)
        if self.is_major_arc:
            mid = lerp(mid, self.center, 2)
        midconstant = math.sqrt((mid[0]-self.center[0])**2 + (mid[1]-self.center[1])**2)
        if midconstant < 1e-13:
            #print("Error in facet.py: midconstant = 0")
            vect = [self.center[0]-self.pLeft[0], self.center[1]-self.pLeft[1]]
            mid = [self.center[0] - vect[1], self.center[1] - vect[0]]
        else:
            mid = [self.center[0] + abs(self.radius)/midconstant*(mid[0]-self.center[0]), self.center[1] + abs(self.radius)/midconstant*(mid[1]-self.center[1])]
        return mid

    #Return list of two ArcFacets
    def splitInTwo(self):
        return [ArcFacet(self.center, self.radius, self.pLeft, self.midpoint), ArcFacet(self.center, self.radius, self.midpoint, self.pRight)]

    def pointInArcRange(self, p):
        if not(self.is_major_arc):
            return (pointLeftOfLine(p, self.center, self.pLeft) == (self.radius > 0)) and (pointRightOfLine(p, self.center, self.pRight) == (self.radius > 0))
        else:
            return not((pointRightOfLine(p, self.center, self.pLeft) == (self.radius > 0)) and (pointLeftOfLine(p, self.center, self.pRight) == (self.radius > 0)))

    def advected(self, velocity, t, h, mode='RK4'):
        #Advect by using 3 control points: 2 endpoints + midpoint of arc
        #Returns either LinearFacet or ArcFacet
        shiftpLeft = advectPoint(self.pLeft, velocity, t, h, mode=mode)
        shiftpRight = advectPoint(self.pRight, velocity, t, h, mode=mode)
        shiftmid = advectPoint(self.midpoint, velocity, t, h, mode=mode)
        [shiftcirclecenter, shiftcircleradius] = getCircumcircle(shiftpLeft, shiftmid, shiftpRight) #shiftcircleradius is always positive
        if shiftcircleradius is None or shiftcircleradius > self.advect_collinearity_threshold:
            #Collinear: advect by creating linear facet
            return LinearFacet(shiftpLeft, shiftpRight)
        else:
            #Not collinear: advect by creating arc facet
            if pointLeftOfLine(shiftmid, shiftpLeft, shiftpRight): # TODO replaced 6/10 because of numerical instabilities with nearly collinear points: getArea([shiftpLeft, shiftmid, shiftpRight]) < 0:
                shiftcircleradius *= -1

            #if getDistance(shiftpLeft, shiftpRight) < 1e-10:
            #    print("Mini facet: {}".format(self))

            return ArcFacet(shiftcirclecenter, shiftcircleradius, shiftpLeft, shiftpRight)

    #TODO: update_pLeft and update_pRight maintains curvature. Is there a balance between updating center and curvature?
    def update_endpoints(self, new_pLeft, new_pRight):
        if abs(self.radius) < getDistance(new_pLeft, new_pRight)/2:
            if self.radius < 0:
                self.radius = -getDistance(new_pLeft, new_pRight)/2
            else:
                self.radius = getDistance(new_pLeft, new_pRight)/2
            self.center = lerp(new_pLeft, new_pRight, 0.5)
        else:
            self.center = getCenter(new_pLeft, new_pRight, self.radius)
        return ArcFacet(self.center, self.radius, new_pLeft, new_pRight)

    def __str__(self):
        return "['{}', {}, {}, [{}, {}]]".format(self.name, self.center, self.radius, self.pLeft, self.pRight)

    def __eq__(self, other_facet):
        if isinstance(other_facet, self.__class__) and getDistance(other_facet.pLeft, self.pLeft) < self.equality_threshold and getDistance(other_facet.pRight, self.pRight) < self.equality_threshold and getDistance(other_facet.center, self.center) < self.equality_threshold and abs(other_facet.radius-self.radius) < self.equality_threshold:
            return True
        return False

class CornerFacet(Facet):
    def __init__(self, centerLeft, centerRight, radiusLeft, radiusRight, pLeft, corner, pRight):
        super().__init__('corner', pLeft, pRight)
        self.centerLeft = centerLeft
        self.centerRight = centerRight
        self.radiusLeft = radiusLeft
        self.radiusRight = radiusRight
        self.corner = corner

    def advected(self, velocity, t, h, mode='RK4'):
        #Advect each side as LinearFacet or ArcFacet
        #Returns CornerFacet
        shiftpLeft = advectPoint(self.pLeft, velocity, t, h, mode=mode)
        shiftcorner = advectPoint(self.corner, velocity, t, h, mode=mode)
        shiftpRight = advectPoint(self.pRight, velocity, t, h, mode=mode)
        midleft = lerp(self.pLeft, self.corner, 0.5)
        if self.radiusLeft is not None:
            midleftconstant = math.sqrt((midleft[0]-self.centerLeft[0])**2 + (midleft[1]-self.centerLeft[1])**2)
            midleft = [self.centerLeft[0] + self.radiusLeft/midleftconstant*(midleft[0]-self.centerLeft[0]), self.centerLeft[1] + self.radiusLeft/midleftconstant*(midleft[1]-self.centerLeft[1])]
            #If the circular facet is a major arc
            if ((self.centerLeft[0]-self.pLeft[0])*(midleft[1]-self.pLeft[1])-(self.centerLeft[1]-self.pLeft[1])*(midleft[0]-self.pLeft[0]))*self.radiusLeft > 0:
                midleft = lerp(midleft, self.centerLeft, 2)
        shiftmidleft = advectPoint(midleft, velocity, t, h, mode=mode)
        midright = lerp(self.corner, self.pRight, 0.5)
        if self.radiusRight is not None:
            midrightconstant = math.sqrt((midright[0]-self.centerRight[0])**2 + (midright[1]-self.centerRight[1])**2)
            midright = [self.centerRight[0] + self.radiusRight/midrightconstant*(midright[0]-self.centerRight[0]), self.centerRight[1] + self.radiusRight/midrightconstant*(midright[1]-self.centerRight[1])]
            #If the circular facet is a major arc
            if ((self.centerRight[0]-self.corner[0])*(midright[1]-self.corner[1])-(self.centerRight[1]-self.corner[1])*(midright[0]-self.corner[0]))*self.radiusRight > 0:
                midright = lerp(midright, self.centerRight, 2)
        shiftmidright = advectPoint(midright, velocity, t, h, mode=mode)

        [shiftcirclecenterleft, shiftcircleradiusleft] = getCircumcircle(shiftpLeft, shiftmidleft, shiftcorner)
        if shiftcircleradiusleft is None or shiftcircleradiusleft > self.advect_collinearity_threshold:
            #Collinear: treat left edge as linear facet
            shiftcirclecenterleft = None
            shiftcircleradiusleft = None
        else:
            #Not collinear: treat left edge as arc facet
            if (shiftcirclecenterleft[0]-shiftpLeft[0])*(-(shiftcorner[1]-shiftpLeft[1])) + (shiftcirclecenterleft[1]-shiftpLeft[1])*(shiftcorner[0]-shiftpLeft[0]) < 0:
                #Circumcenter on right of line, need to invert radius
                shiftcircleradiusleft *= -1
        [shiftcirclecenterright, shiftcircleradiusright] = getCircumcircle(shiftcorner, shiftmidright, shiftpRight)
        if shiftcircleradiusright is None or shiftcircleradiusright > self.advect_collinearity_threshold:
            #Collinear: treat right edge as linear facet
            shiftcirclecenterright = None
            shiftcircleradiusright = None
        else:
            #Not collinear: treat right edge as arc facet
            if (shiftcirclecenterright[0]-shiftcorner[0])*(-(shiftpRight[1]-shiftcorner[1])) + (shiftcirclecenterright[1]-shiftcorner[1])*(shiftpRight[0]-shiftcorner[0]) < 0:
                #Circumcenter on right of line, need to invert radius
                shiftcircleradiusright *= -1

        return CornerFacet(shiftcirclecenterleft, shiftcirclecenterright, shiftcircleradiusleft, shiftcircleradiusright, shiftpLeft, shiftcorner, shiftpRight)

    def __str__(self):
        return "['{}', {}, {}, {}, {}, [{}, {}, {}]]".format('curvedcorner', self.centerLeft, self.centerRight, self.radiusLeft, self.radiusRight, self.pLeft, self.corner, self.pRight)

    #TODO: define equality for corner?