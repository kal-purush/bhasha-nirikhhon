
# continuation of scratch_6, addressing complex edges with multiple maxima/minima
# introduction of dict structure, featureExtrema, to maintain local extrema (x, y) value, centroid disp value, u spline value, v spline surface optional value
# remove pre-calculation of ParsedEdge['vertex1extremaMax'], ParsedEdge['vertex2extremaMax'] flags

from stepcode.Part21 import Parser
import os, glob, re
#from geomdl import BSpline
from geomdl import NURBS
from geomdl import compatibility
from geomdl import utilities
from geomdl import evaluators

#import copy

from collections import namedtuple  # PyCharm bug will flag this as type error
Point = namedtuple("Point", "x y z")

#from scipy import spatial # numpy version < 1.24
import numpy as np


# eps machine precision constant
eps = np.finfo(float).eps

eps_STEP_AP21 = 1e-6  # STEP precision seems to end here

#eps_bspline = 1E-10 # precision used in bspline/NURB surface subroutines
#machine_EPS = np.finfo(float).eps # float comparison

# need a precision factor that reflects increasing accuracy arising from iterative calculations of centroid --------------------------------------<<<<<<<<<<<<<<<<<
# also scale factor affecting numerical precision
# also STEP seems to be E-6 decimal places at best
iterativeScaledPrecision = 1e-4  # for testing

#todo: dict data arrays like ParsedEdge should become classes, once all required fields & methods identified


def angleAxis2RotMat(rotAxis, theta):
    """
    Affine transformation matrix from 3x3 rotation matrix and displacement vector.
    3x3 rotation matrix derived from CCW angle theta around axis
    From: http://en.wikipedia.org/wiki/Rotation_matrix#Axis_and_angle & transform3d

    :param rotAxis: axis of rotation vector axis emanating from origin
    :param theta: scalar radian angle of rotation
    :return: 4x4 matrix representing affine transformation
    """

    # normalise to unit vector
    rotNorm = np.linalg.norm(rotAxis)
    if rotNorm > eps:
        rotAxis = rotAxis / rotNorm
        rotAxis = rotAxis.tolist()

    # if rotAxis.__class__.__name__ == "list":
    #     rotAxis = Point(rotAxis[0], rotAxis[1], rotAxis[2])

    s = np.sin(theta)
    c = np.cos(theta)
    C = 1 - c
    xs = rotAxis.x * s
    ys = rotAxis.y * s
    zs = rotAxis.z * s
    xC = rotAxis.x * C
    yC = rotAxis.y * C
    zC = rotAxis.z * C
    xyC = rotAxis.x * yC
    yzC = rotAxis.y * zC
    zxC = rotAxis.z * xC
    xxC = rotAxis.x * xC
    yyC = rotAxis.y * yC
    zzC = rotAxis.z * zC

    return np.array(
        [
            [xxC + c, xyC - zs, zxC + ys],
            [xyC + zs, yyC + c, yzC - xs],
            [zxC - ys, yzC + xs, zzC + c],
        ]
    )


def rotationMatrix(axis, theta):
    """
    Return the rotation matrix associated with counterclockwise rotation about the given axis by theta radians.

    :param axis: axis defined as a vector point relative to cartesian origin
    :param theta: rotation angle in radians
    :return: rotation matrix
    """

    # Euler-Rodrigues formula
    # http://stackoverflow.com/questions/6802577/python-rotation-of-3d-vector
    # from numpy import asarray, array, dot
    # from math import sqrt, cos, sin

    #axis = np.asarray(axis)
    #theta = np.asarray(theta)
    SDAA = np.sqrt(np.dot(axis, axis))
    if SDAA == 0.0:
        raise RuntimeError("rotationMatrix() zero div error (0,0,0)?")

    axis = axis / SDAA
    a = np.cos(theta / 2)
    b, c, d = -axis * np.sin(theta / 2)
    aa, bb, cc, dd = a * a, b * b, c * c, d * d
    bc, ad, ac, ab, bd, cd = b * c, a * d, a * c, a * b, b * d, c * d
    return np.array(
        [
            [aa + bb - cc - dd, 2 * (bc + ad), 2 * (bd - ac)],
            [2 * (bc - ad), aa + cc - bb - dd, 2 * (cd + ab)],
            [2 * (bd + ac), 2 * (cd - ab), aa + dd - bb - cc],
        ]
    )


def arrayTypeCast(xyz):
    if type(xyz) == list:
        xyz = np.array([xyz[0], xyz[1], xyz[2]])
    elif type(xyz) == Point:
        xyz = np.array([xyz.x, xyz.y, xyz.z])
    elif type(xyz) == np.ndarray:
        pass
    else:
        raise RuntimeError("input type error")
    return xyz


def cart2pol(x, y):
    rho = np.sqrt(x**2 + y**2)
    phi = np.arctan2(y, x)
    return (rho, phi)


def pol2cart(rho, phi):
    x = rho * np.cos(phi)
    y = rho * np.sin(phi)
    return (x, y)


def array3x1match(a, A):
    '''test whether a numpy array of 3 elements (a) exists within a list of 3 element arrays'''
    T = [np.isclose(a[0], b[0], atol=eps) and np.isclose(a[1], b[1], atol=eps) and np.isclose(a[2], b[2], atol=eps) for b in A]
    return any(T)


# does one find a centroid through axis-intersection?
# a number of axes that pass through a common point?
# equidistant corners?
def medianPoint(pointArray):
    """
    Returns centroid determined as the median of cartesian values in an input array of points

    :param pointArray: array of Cartesian points
    :return: point
    """

    # from statistics import median # apparently slow/accurate
    # from numpy import median
    if len(pointArray) < 2:
        raise RuntimeError("medianPoint() malformed input")

    xRange = [xyz[0] for xyz in pointArray]
    xCentroid = max(xRange) - ((max(xRange) - min(xRange)) / 2)
    yRange = [xyz[1] for xyz in pointArray]
    yCentroid = max(yRange) - ((max(yRange) - min(yRange)) / 2)
    zRange = [xyz[2] for xyz in pointArray]
    zCentroid = max(zRange) - ((max(zRange) - min(zRange)) / 2)
    return np.array([xCentroid, yCentroid, zCentroid])


def pointDisp(p1, p2):
    """
    Return displacement(s) between input points or list of points

    :param p1: single point
    :param p2: single or list of points
    :return: disp value or list of values
    """

    if not isinstance(p2, tuple):
        raise RuntimeError("pointDisp second variable must be single point")
    if isinstance(p1, tuple):
        # noinspection PyUnresolvedReferences
        return sqrt((p1.x - p2.x) ** 2 + (p1.y - p2.y) ** 2 + (p1.z - p2.z) ** 2)
    elif isinstance(p1, list):
        return [
            np.sqrt((pp.x - p2.x) ** 2 + (pp.y - p2.y) ** 2 + (pp.z - p2.z) ** 2)
            for pp in p1
        ]
    else:
        raise RuntimeError("pointDisp error")

# def checkFilePath(filePath):
#     if os.path.isfile(filePath) and os.access(filePath, os.R_OK):
#         return filePath
#     else:
#         raise Exception("{0} is not a readable filepath".format(filePath))

primitivesDir = os.path.normpath(
    r"/media/foobert/Dell2HDD/PY_DELL2/SWKS_Rhino_compare_circle/primitives"
)
primitivesDir = os.path.normpath(
    r"/home/foobert/STEP_test_files/primitives"
)
testDir = os.path.normpath(
    r"/home/foobert/STEP_test_files"
)

#filepath = primitivesDir + "/Cube/unit_cube_inc8.0_blend.06.stp"
#filepath = primitivesDir + "/Cylinder/unit_cyl.stp"
#filepath = primitivesDir + "/Primitive_Cone-PartCone.step"

#filepath = r"/home/foobert/Downloads/RR_STEP_test_1.step"
#filepath = r"/home/foobert/Downloads/RR_STEP_test_N.step"
#filepath = r"/home/foobert/Downloads/00000001_1ffb81a71e5b402e966b9341_step_000.step"
#filepath = r"/home/foobert/Downloads/00000010_b4b99d35e04b4277931f9a9c_step_000.step"
# filepath = r"/home/foobert/Downloads/LEA-M8F(AP203).STEP"

#filepath = testDir + "/simpleSpindle1.step"
#filepath = testDir + "/Maltese_cruciform.step"
#filepath = testDir + "/TiltedConeAP203.step"
#filepath = testDir + "/OffsetCone_AP214_noPlacement_noParametric.step"
#filepath = testDir + "/OffsetCone-PartCone.step"
#filepath = testDir + "/TiltedCylinder4_AP214_PC.step"
#filepath = testDir + "/Cylinder5_AP214_PC.step"
#filepath = testDir + "/DrexelBlendedCylinder_topOvalPlane.step"
#DrexelBlendedCylinder_curvedBlend.step
#filepath = testDir + "/DrexelBlendedCylinder_midriffTube.step"
#filepath = testDir + "/TiltedCylinder2.step"
#filepath = primitivesDir + "/Cube/unit_cube.stp"
filepath = testDir + "/Drexel_blended_cylinder.step"
#filepath = testDir + "/DrexelBlendedCylinder_curvedBlend.step"


# not all surface min/max found
# edge min/max found - require local maxima for local centroid determination

#filepath = testDir + "/TiltedCylinder.step"
#filepath = testDir + "/Synth_ellipse_plain.step"

try:
    # filepath sanity check
    checkPath = glob.glob(filepath)
    if len(checkPath) != 1:
        raise Exception("pathname failure")
    else:
        with open(filepath) as f:
            STEPdata = f.read()
except:
    raise Exception("file failure")

STEPdata = STEPdata.replace(";\n", ";;")
STEPdata = STEPdata.replace("\n", "")
STEPdata = STEPdata.replace(";;", ";\n")

P = Parser()
# model = P.parse(/home/foobert/.config/JetBrains/PyCharmCE2023.1/scratches/simple_schema.exp)
model = P.parse(STEPdata)

STEP_entities = model.sections[0].entities

ClosedShells = []
FaceInstances = []
for cs in STEP_entities:
    if hasattr(cs, "type_name"):  # stepcode.Part21.ComplexEntity type missing attribute
        if cs.type_name == "CLOSED_SHELL":
            ClosedShells.append(cs)

def ref2index(s):
    return int(s[1:]) - 1

# encountering step files with non-contiguous line and #ref indices
if ref2index(STEP_entities[-1].ref) is not len(STEP_entities):
    STEP_entities.sort(key=lambda x: int(x.ref[1:]))
    padded_STEP_entities = [0] * int(STEP_entities[-1].ref[1:])
    for s in STEP_entities:
        padded_STEP_entities[ref2index(s.ref)] = s
    STEP_entities = padded_STEP_entities


def radiusCentre3points_2(p1, p2, p3):
    # radius + centre from 3 point circle

    t = p2 - p1
    u = p3 - p1
    v = p3 - p2

    w = np.cross(t, u)  # triangle normal
    wsl = np.linalg.norm(w)
    if (wsl < 10e-14): return False, False  # triangle area too small (additionally check points for colinearity)

    wsl = np.dot(w, w)
    iwsl2 = 1. / (2. * wsl)
    tt = np.dot(t, t)
    uu = np.dot(u, u)

    circCenter = p1 + (u * tt * (np.dot(u, v)) - t * uu * (np.dot(t, v))) * iwsl2
    circRadius = np.sqrt(tt * uu * (np.dot(v, v)) * iwsl2 * 0.5)
    # circAxis   = w / np.sqrt(wsl)

    return circRadius, circCenter


def rationalSurfaceExtremaParam_5(S, p,
                                  maxSearch=True,
                                  localExtrema=False,
                                  curvatureTest=False,
                                  uv_xyz=True,
                                  eps1=0.0001,
                                  eps2=0.0005,
                                  deltaFactor=4,
                                  eps_bspline = 1E-10):
    '''
    Use either Newton-Raphson, or a hillclimbing neighbour search to find minimal or maximal distance on a surface, S, from a supplied point, p
    - not tested with surfaces closed along U & V
    maxSearch: search for most distant point from p if true, else nearest if false
    localExtrema: find any local extrema which differs from surrounding sample points, tolerance not sufficiently defined
    curvatureTest: rather than displacement of sampled points to provide seed locations for N-R/hillclimb search,
    use a combination of the centres of curvature at any point across U-axis and V-axis to identify C2 inflection point.
    uv_xyz: return u,v normalised values or cartesian x,y,z values
    eps1: point coincidence limit, halt N-R on reaching this minima
    eps2: cosine angle limit, halt N-R on reaching this minima
    delta: sample interval dividing surface into points
    '''

    # easy to find a minimum, but a maximum found through the original hillclimbing algorithm will settle on local maxima
    # for surfaces, minimize the following:
    #
    # f = Su(u,v) * r = 0
    # g = Sv(u,v) * r = 0
    #
    #   where r = S(u,v) - P
    #
    # Requires Newton-Raphson iteration, objective function is vector valued
    #
    #     J d = k
    #
    #     d =   [ u* - u, v* - v ] (alternatively u[i+1] -  u[i], v[i+1]- v[i])
    #     k = - [ f(u,v), g(u,v) ]
    #     J =     |Su|^2   +  Suu * r       Su*Sv  +  Suv * r
    #              Su*Sv   +  Svu * r      |Sv|^2  +  Svv * r
    #
    # halting conditions:
    # point coincidence
    #
    #         |S(u,v) - p| < e1
    #
    # cosine
    #
    #    |Su(u,v)*(S(u,v) - P)|
    #    ----------------------  < e2
    #    |Su(u,v)| |S(u,v) - P|
    #
    #    |Sv(u,v)*(S(u,v) - P)|
    #    ----------------------  < e2
    #    |Sv(u,v)| |S(u,v) - P|
    #
    # 1) first check 2 & 3
    # 2) if at least one of these is not, compute new value, otherwise halt
    # 3) ensure the parameter stays within range
    #     * if not closed, don't allow outside of range a-b
    #     * if closed (e.g. circle), allow to move back to beginning
    # 4)  if |(u* - u)C'(u)| < e1, halt

    # todo: take value from STEP fields
    # check if surface closed along U and V

    def closedV():
        return np.isclose([np.array(s[0]) - np.array(s[-1]) for s in S.ctrlpts2d], eps_bspline).all()

    def closedU():
        return np.isclose(np.array(S.ctrlpts2d[0]) - np.array(S.ctrlpts2d[-1]), eps_bspline).all()  # eps_STEP_AP21

    #S.delta = delta  # set evaluation delta
    # tradeoff between sample density and feature identification

    #deltaFactor = 5
    #deltaFactor = 4

    # S.delta_u = 1 / (S.ctrlpts_size_u * deltaFactor)
    # S.delta_v = 1 / (S.ctrlpts_size_v * deltaFactor)

    S.delta_u = 1 / len(S.knotvector_u) * deltaFactor
    S.delta_v = 1 / len(S.knotvector_v) * deltaFactor

    # if S.evalpts == None:  # evaluate surface points---------appears redundant
    #     S.evaluate()

    def neighbouringUV(U, V, isClosedU, isClosedV):

        for u in range(0, U - 2 + (2 * isClosedU)):
            for v in range(0, V - 2 + (2 * isClosedV)):

                if v == 0:
                    NW = (((u + 2) % U) * V)
                    N = (((u + 2) % U) * V) + 1
                    NE = (((u + 2) % U) * V) + 2

                    W = (((u + 1) % U) * V)
                    C = (((u + 1) % U) * V) + 1
                    E = (((u + 1) % U) * V) + 2

                    SW = ((u % U) * V)
                    S = ((u % U) * V) + 1
                    SE = ((u % U) * V) + 2

                if v >= 1:
                    NW = N
                    W = C  # central instance
                    SW = S

                    N = NE
                    C = E
                    S = SE

                    NE = (((u + 2) % U) * V) + ((v + 2) % V)
                    E = (((u + 1) % U) * V) + ((v + 2) % V)
                    SE = ((u % U) * V) + ((v + 2) % V)

                # print([NW, N, NE])
                # print([W, C, E])
                # print([SW, S, SE])
                # print("==========")
                # print([u, v, ((u * V) + v) ])
                # print("==========")

                # if v==V - 2:
                #     _1=1

                # if C==3:
                #     _1=1

                yield ([NW, N, NE, W, C, E, SW, S, SE], (u, v))

    # def _neighbouringUV(U, V, isClosedU, isClosedV):
    #     # generator function to yield neighbouring indices over a matrix of size U x V
    #
    #     openU = not isClosedU
    #     openV = not isClosedV
    #
    #     for u in range(openU, U - openU):
    #         for v in range(openV, V - openV):
    #
    #             if v == 0:  # closed in v direction
    #                 if u == U - 1:
    #                     NW = V - 1
    #                 else:
    #                     NW = ((u + 2) * V) + v - 1
    #
    #                 W = (V * (u + 1)) - 1
    #
    #                 if u == 0:
    #                     SW = (U * V) - 1  # V - 1
    #                 else:
    #                     SW = (u * V) - 1
    #
    #                 if u == U - 1:  # closed in u direction
    #                     N = v
    #                 else:
    #                     N = V * (u + 1)
    #
    #                 C = u * V
    #
    #                 if u == 0:
    #                     S = V * (U - 1)
    #                 else:
    #                     S = V * (u - 1)
    #
    #             if v == 1:
    #                 if u == U - 1:  # closed in u direction
    #                     NW = v - 1
    #                 else:
    #                     NW = ((u + 1) * V) + v - 1
    #
    #                 W = (u * V) + v - 1
    #
    #                 if u == 0:
    #                     SW = ((U - 1) * V) + v - 1
    #                 else:
    #                     SW = ((u - 1) * V) + v - 1
    #
    #                 if u == U - 1:  # closed in v direction
    #                     N = v
    #                 else:
    #                     N = ((u + 1) * V) + v
    #
    #                 C = (u * V) + v
    #
    #                 if u == 0:
    #                     S = ((U - 1) * V) + 1
    #                 else:
    #                     S = ((u - 1) * V) + v
    #
    #             if v > 1:
    #                 NW = N
    #                 W = C  # central instance
    #                 SW = S
    #
    #                 N = NE
    #                 C = E
    #                 S = SE
    #
    #             if v == V - 1:  # closed in v direction
    #                 if u < U - 1:
    #                     NE = (u + 1) * V
    #                 elif u == U - 1:  # closed in u direction
    #                     NE = 0
    #
    #                 if u < U - 1:
    #                     E = u * V
    #                 elif u == U - 1:
    #                     E = ((u - 1) * V) + v + 1
    #
    #                 if u == 0:
    #                     SE = ((U - 1) * V)
    #                 elif u < U - 1:
    #                     SE = (u - 1) * V
    #                 elif u == U - 1:
    #                     SE = ((u - 2) * V) + v + 1
    #
    #             elif v < V - 1:
    #                 if u < U - 1:  # only have to calculate this once per loop
    #                     NE = ((u + 1) * V) + v + 1
    #                 elif u == U - 1:  # closed in u direction
    #                     NE = v + 1
    #
    #                 if u < U - 1:  # only have to calculate this once per loop
    #                     E = (u * V) + v + 1
    #                 elif u == U - 1:
    #                     E = (u * V) + v + 1
    #
    #                 if u == 0:
    #                     SE = ((U - 1) * V) + v + 1
    #                 elif u < U - 1:  # only have to calculate this once per loop
    #                     SE = ((u - 1) * V) + v + 1
    #                 elif u == U - 1:
    #                     SE = ((u - 1) * V) + v + 1
    #
    #             # print([NW, N, NE])
    #             # print([W, C, E])
    #             # print([SW, S, SE])
    #             # print("==========")
    #
    #             yield ([NW, N, NE, W, C, E, SW, S, SE], (u, v))

    def localNeighbourSearch(S):
        extremaUV = []
        extremaP = []
        maxBound = []

        # not S.evalpts
        uSamples = np.linspace(S.knotvector_u[0],
                               S.knotvector_u[-1],
                               num=len(S.knotvector_u) * deltaFactor,
                               endpoint=~closedU())
        vSamples = np.linspace(S.knotvector_v[0],
                               S.knotvector_v[-1],
                               num=len(S.knotvector_v) * deltaFactor,
                               endpoint=~closedV())
        # uSamples = np.linspace(S.knotvector_u[0], S.knotvector_u[-1], num=len(S.knotvector_u) * deltaFactor)
        # vSamples = np.linspace(S.knotvector_v[0], S.knotvector_v[-1], num=len(S.knotvector_v) * deltaFactor)
        uvArray = np.zeros((len(uSamples) * len(vSamples), 2))

        for vi, vS in enumerate(vSamples):
            for ui, uS in enumerate(uSamples):
                uvArray[(ui * len(vSamples)) + vi] = [uS, vS]

        pArray = np.array(S.evaluate_list(uvArray))

        #test1 = S.evaluate_single(uvArray[0]) #(0.0, 0.0))
        #test2 = S.evaluate_single(uvArray[207]) #(0.0, 1.0))
        #minSample=[]

        for CC, uv in neighbouringUV(len(uSamples), len(vSamples), closedU(), closedV()):

            pNW = pArray[CC[0]]
            pN  = pArray[CC[1]]
            pNE = pArray[CC[2]]
            pE  = pArray[CC[3]]
            pC =  pArray[CC[4]]
            pW  = pArray[CC[5]]
            pSW = pArray[CC[6]]
            pS  = pArray[CC[7]]
            pSE = pArray[CC[8]]

            dpuv_NW = np.linalg.norm(p - pNW)
            dpuv_N  = np.linalg.norm(p - pN)
            dpuv_NE = np.linalg.norm(p - pNE)
            dpuv_E  = np.linalg.norm(p - pE)
            dpuv    = np.linalg.norm(p - pC)
            dpuv_W  = np.linalg.norm(p - pW)
            dpuv_SW = np.linalg.norm(p - pSW)
            dpuv_S  = np.linalg.norm(p - pS)
            dpuv_SE = np.linalg.norm(p - pSE)

            #testSample = [pNW, pN, pNE, pW, pC, pE, pSW, pS, pSE]
            # if ((dpuv_S <= dpuv_NW) and (dpuv_S <= dpuv_N) and (dpuv_S <= dpuv_NE) and
            #         (dpuv_S <= dpuv_W) and (dpuv_S <= dpuv_E) and
            #         (dpuv_S <= dpuv_SW) and (dpuv_S <= dpuv) and (dpuv_S <= dpuv_SE)):
            #     minSample.append(pS)

            if maxSearch:
                if ((dpuv >= dpuv_NW) and (dpuv >= dpuv_N) and (dpuv >= dpuv_NE) and
                        (dpuv >= dpuv_W) and (dpuv >= dpuv_E) and
                        (dpuv >= dpuv_SW) and (dpuv >= dpuv_S) and (dpuv >= dpuv_SE)):

                    #b_NW = np.linalg.norm(pC - pNW)
                    b_N = np.linalg.norm(pC - pN)
                    #b_NE = np.linalg.norm(pC - pNE)
                    b_E = np.linalg.norm(pC - pE)

                    b_W = np.linalg.norm(pC - pW)
                    #b_SW = np.linalg.norm(pC - pSW)
                    b_S = np.linalg.norm(pC - pS)
                    #b_SE = np.linalg.norm(pC - pSE)

                    maxBound.append(max([b_N, b_E, b_W, b_S]))
                    extremaUV.append(uvArray[CC[4]]) # uv[0]*S.sample_size_v + uv[1]
                    extremaP.append(pArray[CC[4]])

            else:  # minS
                if ((dpuv <= dpuv_NW) and (dpuv <= dpuv_N) and (dpuv <= dpuv_NE) and
                        (dpuv <= dpuv_W) and (dpuv <= dpuv_E) and
                        (dpuv <= dpuv_SW) and (dpuv <= dpuv_S) and (dpuv <= dpuv_SE)):
                    # where a point is orthogonal to a planar surface -> sphere radius test, or accept minima


                    #b_NW = np.linalg.norm(pC - pNW)
                    b_N = np.linalg.norm(pC - pN)
                    #b_NE = np.linalg.norm(pC - pNE)
                    b_E = np.linalg.norm(pC - pE)

                    b_W = np.linalg.norm(pC - pW)
                    #b_SW = np.linalg.norm(pC - pSW)
                    b_S = np.linalg.norm(pC - pS)
                    #b_SE = np.linalg.norm(pC - pSE)

                    maxBound.append(max([b_N, b_E, b_W, b_S]))
                    extremaUV.append(uvArray[CC[4]])
                    extremaP.append(pArray[CC[4]])

        return extremaUV, extremaP, maxBound

    def curvatureSearch():

        # not S.evalpts
        uSamples = np.linspace(S.knotvector_u[0],
                               S.knotvector_u[-1],
                               num=len(S.knotvector_u) * deltaFactor,
                               endpoint=~closedU())
        vSamples = np.linspace(S.knotvector_v[0],
                               S.knotvector_v[-1],
                               num=len(S.knotvector_v) * deltaFactor,
                               endpoint=~closedV())
        uvArray = np.zeros((len(uSamples) * len(vSamples), 2))

        for vi, vS in enumerate(vSamples):
            for ui, uS in enumerate(uSamples):
                uvArray[(ui * len(vSamples)) + vi] = [uS, vS]

        pArray = np.array(S.evaluate_list(uvArray))

        discreteK = [np.inf] * len(uvArray)
        for CC, uv in neighbouringUV(len(uSamples), len(vSamples), closedU(), closedV()):

            puv = np.array(pArray[CC[4]])
            pur, pucc = radiusCentre3points_2(pArray[CC[3]], puv, pArray[CC[5]])
            pvr, pvcc = radiusCentre3points_2(pArray[CC[7]], puv, pArray[CC[1]])

            # find media point of U curvature centre and V curvature, kuv
            if pur is np.inf and pvr is not np.inf:
                kuv = pvcc - puv
            elif pur is not np.inf and pvr is np.inf:
                kuv = pucc - puv
            elif pur is np.inf and pvr is np.inf:
                discreteK[(S.sample_size_u * uv[0]) + uv[1]] = np.inf
                break
            else:
                kuv = ((pucc - puv) + (pvcc - puv)) / 2

            dkuv = np.linalg.norm(kuv - puv)

            if np.linalg.norm(p - puv) > np.linalg.norm(kuv - p):
                # p is on the same side of the surface as the median curvature
                discreteK[(S.sample_size_u * uv[0]) + uv[1]] = dkuv

            if np.linalg.norm(p - puv) < np.linalg.norm(kuv - p):
                # smallest radius, with curvature centre on far side of surface from p
                discreteK[(S.sample_size_u * uv[0]) + uv[1]] = -dkuv

            # todo: examine issue with non-colinear curve centre and projecting point
            # e.g. proj(p)
            #                 if np.dot(p - p1, pucc - p1)/np.linalg.norm(pucc - p1) > 0:
            #                     discreteK[i] = pur
            #                 else:
            #                     discreteK[i] = -pur

        return discreteK

    def localCurvatureExtrema(discreteK):
        leu = []
        # localExtremaP = []
        # maxBound = []

        for CC, uv in neighbouringUV(len(S.knotvector_u) * deltaFactor,
                                     len(S.knotvector_v) * deltaFactor,
                                     closedU(), closedV()):
            surroundNodes = [CC[1], CC[3], CC[5], CC[7]]

            if not any(discreteK[ssn] == np.inf for ssn in surroundNodes):
                if maxSearch:
                    if (all((np.abs(discreteK[ssn]) >= discreteK[(S.sample_size_u * uv[0]) + uv[1]]) for ssn in surroundNodes) and
                            (discreteK[(S.sample_size_u * uv[0]) + uv[1]] > 0)):
                        # this version excludes negative curves from surrounding curvature values
                        # if all(np.abs(discreteK[ssn]) > discreteK[S.sample_size_u * u + v] or
                        # (discreteK[ssn]<0) for ssn in surroundNodes) and
                        # (discreteK[S.sample_size_u * u + v] > 0):

                        pC = np.array(S.evaluate_single(uv))
                        localExtremaP(pC)
                        maxBound.append(max([np.linalg.norm(pC - np.array(S.evaluate_single(uvk))) for uvk in surroundNodes]))
                        leu.append(uv)
                else:
                    if all((np.abs(discreteK[ssn]) >= discreteK[(S.sample_size_u * uv[0]) + uv[1]]) for ssn in
                           surroundNodes) and (discreteK[(S.sample_size_u * uv[0]) + uv[1]] < 0):

                        pC = np.array(S.evaluate_single(uv))
                        localExtremaP(pC)
                        maxBound.append(max([np.linalg.norm(pC - np.array(S.evaluate_single(uvk))) for uvk in surroundNodes]))
                        leu.append(uv)

        return leu

    def subSearchUV(mpU, mpV, tol):
        # create simple subdivisions around a minimum point as simple hillclimb search
        tolFlag = 0
        divU = S.delta_u
        divV = S.delta_v
        dpuvlocal = np.linalg.norm(p - np.array(S.evaluate_single((mpU, mpV,))))
        #dpuvlocal = np.inner(p - np.array(S.evaluate_single((mpU, mpV,))))

        while not tolFlag:
            divU /= 2
            divV /= 2

            subDivs = [[mpU - divU, mpV + divV],
                       [mpU, mpV + divV],
                       [mpU + divU, mpV + divV],
                       [mpU - divU, mpV],
                       [mpU, mpV],
                       [mpU + divU, mpV],
                       [mpU - divU, mpV - divV],
                       [mpU, mpV - divV],
                       [mpU + divU, mpV - divV]]

            for sdv in subDivs:
                if sdv[0] > 1.0: sdv[0] = 1.0
                if sdv[0] < 0.0: sdv[0] = 0.0
                if sdv[1] > 1.0: sdv[1] = 1.0
                if sdv[1] < 0.0: sdv[1] = 0.0

            pSubDivs = S.evaluate_list(subDivs)

            # test np.inner
            # dispUV = [np.inner(p - psd) for psd in pSubDivs]

            dpuv_NW = np.linalg.norm(p - pSubDivs[0])
            dpuv_N = np.linalg.norm(p - pSubDivs[1])
            dpuv_NE = np.linalg.norm(p - pSubDivs[2])

            dpuv_W = np.linalg.norm(p - pSubDivs[3])
            # dpuv = np.linalg.norm(p - pSubDivs[4])
            dpuv_E = np.linalg.norm(p - pSubDivs[5])

            dpuv_SW = np.linalg.norm(p - pSubDivs[6])
            dpuv_S = np.linalg.norm(p - pSubDivs[7])
            dpuv_SE = np.linalg.norm(p - pSubDivs[8])

            dispUV = [dpuv_NW,
                      dpuv_N,
                      dpuv_NE,
                      dpuv_W,
                      #dpuvlocal,
                      dpuv_E,
                      dpuv_SW,
                      dpuv_S,
                      dpuv_SE]

            if maxSearch:
                if np.abs(max(dispUV) - dpuvlocal) >= tol:
                    #np.abs(max(dispUV) - dpuvlocal) >= tol:
                    maxUVindex = dispUV.index(max(dispUV))
                    mpU = subDivs[maxUVindex][0]
                    mpV = subDivs[maxUVindex][1]
                    dpuvlocal = max(dispUV)
                else:
                    tolFlag += 1
            else:
                if np.abs(min(dispUV) - dpuvlocal) >= tol:
                    minUVindex = dispUV.index(min(dispUV))
                    mpU = subDivs[minUVindex][0]
                    mpV = subDivs[minUVindex][1]
                    dpuvlocal = min(dispUV)
                else:
                    tolFlag += 1
        return (mpU, mpV,)

    def NewtonRaphson(cuv, maxits=5):

        # def f(uv):
        #     return surface.derivatives(uv[0], uv[1], 2)

        # e[0][0], the surface point itself
        # e[0][1], the 1st derivative w.r.t. v
        # e[2][1], the 2nd derivative w.r.t. u and 1st derivative w.r.t. v

        def n(uv, e, r):  # np.array inputs
            #   f = Su(u,v) * r = 0
            #   g = Sv(u,v) * r = 0

            Su = e[1][0]
            Sv = e[0][1]

            Suu = e[2][0]
            Svv = e[0][2]

            Suv = e[1][1]
            Svu = e[1][1]

            f = np.dot(Su, r)
            g = np.dot(Sv, r)
            k = [-f, -g]

            J00 = np.dot(Su, Su) + np.dot(Suu, r)
            J01 = np.dot(Su, Sv) + np.dot(Suv, r)
            J10 = np.dot(Su, Sv) + np.dot(Svu, r)
            J11 = np.dot(Sv, Sv) + np.dot(Svv, r)

            # d =   [ u* - u, v* - v ]
            # k = - [ f(u,v), g(u,v) ]
            # J =   |Su|^2   +  Suu * r       Su*Sv  +  Suv * r
            #        Su*Sv   +  Svu * r      |Sv|^2  +  Svv * r

            J = [[J00, J01], [J10, J11]]
            d = np.linalg.solve(J, k)
            return d + uv

        i = 0
        # e = None
        while i < maxits:
            if (cuv[0] < 0) or (cuv[0] > 1) or (cuv[1] < 0) or (cuv[1] > 1):
                return (np.inf, np.inf) # derivatives sometimes > |2|

            #test = S.derivatives(1.0, 0.0, 0)

            e = np.array(S.derivatives(cuv[0], cuv[1], 2))
            dif = e[0][0] - p # e[0][0] is surface point evaluated at cuv(u,v)
            c1v = np.linalg.norm(dif)
            c1 = c1v < eps1 #  |S(u,v) - p| < e1 (point coincidence)

            #  |Su(u,v)*(S(u,v) - P)|
            #  ----------------------  < e2 (cosine minima)
            #  |Su(u,v)| |S(u,v) - P|

            c2an = np.dot(e[1][0], dif)
            c2ad = np.linalg.norm(e[1][0]) * c1v
            c2av = c2an / c2ad
            c2a = c2av < eps2

            #  |Sv(u,v)*(S(u,v) - P)|
            #  ----------------------  < e2 (cosine minima)
            #  |Sv(u,v)| |S(u,v) - P|

            c2bn = np.dot(e[0][1], dif)
            c2bd = np.linalg.norm(e[0][1]) * c1v
            c2bv = c2bn / c2bd
            c2b = c2bv < eps2

            # exit if all tolerance are met,
            if (c1 and c2a and c2b):
                return cuv

            # otherwise, take a step
            ct = n(cuv, e, dif)

            #  correct for exceeding bounds
            if ct[0] < S.knotvector_u[0]: # [ maxu - ( ct[0] - minu ), ct[1] ]
                if closedU(): ct = [S.knotvector_u[-1] - (S.knotvector_u[0] - ct[0]), ct[1]]
                #if closedU(): ct = [S.knotvector_u[0] + ct[0] % (S.knotvector_u[-1] - S.knotvector_u[0]), ct[1]]
                else: ct = [S.knotvector_u[0] + eps_bspline, ct[1]]

            elif ct[0] > S.knotvector_u[-1]:
                if closedU(): ct = [S.knotvector_u[0] + (ct[0] - S.knotvector_u[-1]), ct[1]]
                #if closedU(): ct = [S.knotvector_u[-1] - ct[0] % (S.knotvector_u[-1] - S.knotvector_u[0]), ct[1]]
                else: ct = [S.knotvector_u[-1] - eps_bspline, ct[1]]

            if ct[1] < S.knotvector_v[0]:
                if closedV(): ct = [ct[0], S.knotvector_v[-1] - (S.knotvector_v[0] - ct[1])]
                #if closedV(): ct = [ct[0], S.knotvector_v[0] + ct[1] % (S.knotvector_v[-1] - S.knotvector_v[0])]
                else: ct = [ct[0], S.knotvector_v[0] + eps_bspline]

            elif ct[1] > S.knotvector_v[-1]:
                #if closedV(): ct = [ct[0], S.knotvector_v[0] + (ct[1] - S.knotvector_v[-1])]
                if closedV(): ct = [ct[0], S.knotvector_v[-1] - ct[1] % (S.knotvector_v[-1] - S.knotvector_v[0])]
                else: ct = [ct[0], S.knotvector_v[-1] - eps_bspline]

            c3v0 = np.linalg.norm((ct[0] - cuv[0]) * e[1][0])
            c3v1 = np.linalg.norm((ct[1] - cuv[1]) * e[0][1])

            # if |(u* - u) C'(u)| < e1, halt
            if (c3v0 + c3v1 < eps1):
                return cuv
            cuv = ct
            i += 1

        if i == maxits:  # Newton-Raphson fails
            return (np.inf, np.inf)
        return cuv

    if curvatureTest:  # discrete grid summing curvatures along U and V axes
        localExtremaUV, localExtremaP, maxBound = localCurvatureExtrema(curvatureSearch())
    else:
        localExtremaUV, localExtremaP, maxBound = localNeighbourSearch(S)

    if len(localExtremaUV) == 0: return []

    extremaUV = []
    for luv, lp, mb in zip(localExtremaUV, localExtremaP, maxBound):
        mUV = NewtonRaphson(luv)

        if not np.inf in mUV:
            # find a maximum deviation to indicate when N-R fails
            if np.linalg.norm(lp - np.array(S.evaluate_single(mUV))) <= mb:
                extremaUV.append(mUV)
            else:
                mUV = (np.inf, np.inf)
        if np.inf in mUV:
            # Newton-Raphson fail, try hillclimb
            mUV = subSearchUV(luv[0], luv[1], eps_bspline)
            extremaUV.append(mUV)
        # print(S.evalpts[(S.sample_size_u * luv[0]) + luv[1]])

    # filter out identical values
    extremaUnique = []
    for m in extremaUV:
        # displacement between any 2 points < eps1
        # compare u, v individually to reduce comparison pool
        if not any([np.isclose(m, u, eps1).all() for u in extremaUnique]):
            extremaUnique.append(m)

    if (localExtrema and (len(extremaUnique) == 1)) or not localExtrema:  # if there is only one extrema, localextrema is moot
        if uv_xyz: return extremaUnique # return single value in list for compatibility
        else: return [np.array(S.evaluate_single(extremaUnique[0])),]
    else:
        if localExtrema:
            if uv_xyz: return extremaUnique
            else: return S.evaluate_list(extremaUnique)
        else: # return single maxima
            extremaUniquePoint = S.evaluate_list(extremaUnique)
            dispsExtrema = [np.linalg.norm(np.array(e) - p) for e in extremaUniquePoint]
            if maxSearch: # return single value in list for compatibility
                if uv_xyz: return [extremaUnique[dispsExtrema.index(max(dispsExtrema))],]
                else: return [np.array(extremaUniquePoint[dispsExtrema.index(max(dispsExtrema))]),]
            else: # minsearch
                if uv_xyz: return [extremaUnique[dispsExtrema.index(min(dispsExtrema))],]
                else: return [np.array(extremaUniquePoint[dispsExtrema.index(min(dispsExtrema))]),]



# def rationalSurfaceExtremaParam_5(S, p,
#                                   maxSearch=True,
#                                   localExtrema=False,
#                                   curvatureTest=False,
#                                   uv_xyz=True,
#                                   eps1=0.0001,
#                                   eps2=0.0005,
#                                   deltaFactor=4,
#                                   eps_bspline = 1E-10):
#     '''
#     Use either Newton-Raphson, or a hillclimbing neighbour search to find minimal or maximal distance on a surface, S, from a supplied point, p
#     - not tested with surfaces closed along U & V
#     maxSearch: search for most distant point from p if true, else nearest if false
#     localExtrema: find any local extrema which differs from surrounding sample points, tolerance not sufficiently defined
#     curvatureTest: rather than displacement of sampled points to provide seed locations for N-R/hillclimb search,
#     use a combination of the centres of curvature at any point across U-axis and V-axis to identify C2 inflection point.
#     uv_xyz: return u,v normalised values or cartesian x,y,z values
#     eps1: point coincidence limit, halt N-R on reaching this minima
#     eps2: cosine angle limit, halt N-R on reaching this minima
#     delta: sample interval dividing surface into points
#     '''
#
#     # easy to find a minimum, but a maximum found through the original hillclimbing algorithm will settle on local maxima
#     # for surfaces, minimize the following:
#     #
#     # f = Su(u,v) * r = 0
#     # g = Sv(u,v) * r = 0
#     #
#     #   where r = S(u,v) - P
#     #
#     # Requires Newton-Raphson iteration, objective function is vector valued
#     #
#     #     J d = k
#     #
#     #     d =   [ u* - u, v* - v ] (alternatively u[i+1] -  u[i], v[i+1]- v[i])
#     #     k = - [ f(u,v), g(u,v) ]
#     #     J =     |Su|^2   +  Suu * r       Su*Sv  +  Suv * r
#     #              Su*Sv   +  Svu * r      |Sv|^2  +  Svv * r
#     #
#     # halting conditions:
#     # point coincidence
#     #
#     #         |S(u,v) - p| < e1
#     #
#     # cosine
#     #
#     #    |Su(u,v)*(S(u,v) - P)|
#     #    ----------------------  < e2
#     #    |Su(u,v)| |S(u,v) - P|
#     #
#     #    |Sv(u,v)*(S(u,v) - P)|
#     #    ----------------------  < e2
#     #    |Sv(u,v)| |S(u,v) - P|
#     #
#     # 1) first check 2 & 3
#     # 2) if at least one of these is not, compute new value, otherwise halt
#     # 3) ensure the parameter stays within range
#     #     * if not closed, don't allow outside of range a-b
#     #     * if closed (e.g. circle), allow to move back to beginning
#     # 4)  if |(u* - u)C'(u)| < e1, halt
#
#     # todo: take value from STEP fields
#     # check if surface closed along U and V
#
#     def closedV():
#         return np.isclose([np.array(s[0]) - np.array(s[-1]) for s in S.ctrlpts2d], eps_bspline).all()
#
#     def closedU():
#         return np.isclose(np.array(S.ctrlpts2d[0]) - np.array(S.ctrlpts2d[-1]), eps_bspline).all()  # eps_STEP_AP21
#
#     #S.delta = delta  # set evaluation delta
#     # tradeoff between sample density and feature identification
#
#     #deltaFactor = 5
#     deltaFactor = 4
#
#     # S.delta_u = 1 / (S.ctrlpts_size_u * deltaFactor)
#     # S.delta_v = 1 / (S.ctrlpts_size_v * deltaFactor)
#
#     S.delta_u = 1 / len(S.knotvector_u) * deltaFactor
#     S.delta_v = 1 / len(S.knotvector_v) * deltaFactor
#
#     # if S.evalpts == None:  # evaluate surface points---------appears redundant
#     #     S.evaluate()
#
#     def neighbouringUV(U, V, isClosedU, isClosedV):
#
#         for u in range(0, U - 2 + (2 * isClosedV)):
#             for v in range(0, V - 2 + (2 * isClosedU)):
#
#                 if v == 0:
#                     NW = (((u + 2) % U) * V)
#                     N = (((u + 2) % U) * V) + 1
#                     NE = (((u + 2) % U) * V) + 2
#
#                     W = (((u + 1) % U) * V)
#                     C = (((u + 1) % U) * V) + 1
#                     E = (((u + 1) % U) * V) + 2
#
#                     SW = ((u % U) * V)
#                     S = ((u % U) * V) + 1
#                     SE = ((u % U) * V) + 2
#
#                 if v >= 1:
#                     NW = N
#                     W = C  # central instance
#                     SW = S
#
#                     N = NE
#                     C = E
#                     S = SE
#
#                     NE = (((u + 2) % U) * V) + ((v + 2) % V)
#                     E = (((u + 1) % U) * V) + ((v + 2) % V)
#                     SE = ((u % U) * V) + ((v + 2) % V)
#
#                 # print([NW, N, NE])
#                 # print([W, C, E])
#                 # print([SW, S, SE])
#                 # print("==========")
#                 # print([u, v, ((u * V) + v) ])
#                 # print("==========")
#
#                 yield ([NW, N, NE, W, C, E, SW, S, SE], (u, v))
#
#     # def _neighbouringUV(U, V, isClosedU, isClosedV):
#     #     # generator function to yield neighbouring indices over a matrix of size U x V
#     #
#     #     openU = not isClosedU
#     #     openV = not isClosedV
#     #
#     #     for u in range(openU, U - openU):
#     #         for v in range(openV, V - openV):
#     #
#     #             if v == 0:  # closed in v direction
#     #                 if u == U - 1:
#     #                     NW = V - 1
#     #                 else:
#     #                     NW = ((u + 2) * V) + v - 1
#     #
#     #                 W = (V * (u + 1)) - 1
#     #
#     #                 if u == 0:
#     #                     SW = (U * V) - 1  # V - 1
#     #                 else:
#     #                     SW = (u * V) - 1
#     #
#     #                 if u == U - 1:  # closed in u direction
#     #                     N = v
#     #                 else:
#     #                     N = V * (u + 1)
#     #
#     #                 C = u * V
#     #
#     #                 if u == 0:
#     #                     S = V * (U - 1)
#     #                 else:
#     #                     S = V * (u - 1)
#     #
#     #             if v == 1:
#     #                 if u == U - 1:  # closed in u direction
#     #                     NW = v - 1
#     #                 else:
#     #                     NW = ((u + 1) * V) + v - 1
#     #
#     #                 W = (u * V) + v - 1
#     #
#     #                 if u == 0:
#     #                     SW = ((U - 1) * V) + v - 1
#     #                 else:
#     #                     SW = ((u - 1) * V) + v - 1
#     #
#     #                 if u == U - 1:  # closed in v direction
#     #                     N = v
#     #                 else:
#     #                     N = ((u + 1) * V) + v
#     #
#     #                 C = (u * V) + v
#     #
#     #                 if u == 0:
#     #                     S = ((U - 1) * V) + 1
#     #                 else:
#     #                     S = ((u - 1) * V) + v
#     #
#     #             if v > 1:
#     #                 NW = N
#     #                 W = C  # central instance
#     #                 SW = S
#     #
#     #                 N = NE
#     #                 C = E
#     #                 S = SE
#     #
#     #             if v == V - 1:  # closed in v direction
#     #                 if u < U - 1:
#     #                     NE = (u + 1) * V
#     #                 elif u == U - 1:  # closed in u direction
#     #                     NE = 0
#     #
#     #                 if u < U - 1:
#     #                     E = u * V
#     #                 elif u == U - 1:
#     #                     E = ((u - 1) * V) + v + 1
#     #
#     #                 if u == 0:
#     #                     SE = ((U - 1) * V)
#     #                 elif u < U - 1:
#     #                     SE = (u - 1) * V
#     #                 elif u == U - 1:
#     #                     SE = ((u - 2) * V) + v + 1
#     #
#     #             elif v < V - 1:
#     #                 if u < U - 1:  # only have to calculate this once per loop
#     #                     NE = ((u + 1) * V) + v + 1
#     #                 elif u == U - 1:  # closed in u direction
#     #                     NE = v + 1
#     #
#     #                 if u < U - 1:  # only have to calculate this once per loop
#     #                     E = (u * V) + v + 1
#     #                 elif u == U - 1:
#     #                     E = (u * V) + v + 1
#     #
#     #                 if u == 0:
#     #                     SE = ((U - 1) * V) + v + 1
#     #                 elif u < U - 1:  # only have to calculate this once per loop
#     #                     SE = ((u - 1) * V) + v + 1
#     #                 elif u == U - 1:
#     #                     SE = ((u - 1) * V) + v + 1
#     #
#     #             # print([NW, N, NE])
#     #             # print([W, C, E])
#     #             # print([SW, S, SE])
#     #             # print("==========")
#     #
#     #             yield ([NW, N, NE, W, C, E, SW, S, SE], (u, v))
#
#     def localNeighbourSearch(S):
#         extremaUV = []
#         extremaP = []
#         maxBound = []
#
#         # not S.evalpts
#         uSamples = np.linspace(S.knotvector_u[0], S.knotvector_u[-1], num=len(S.knotvector_u) * deltaFactor, endpoint=(closedU()))
#         vSamples = np.linspace(S.knotvector_v[0], S.knotvector_v[-1], num=len(S.knotvector_v) * deltaFactor, endpoint=(closedV()))
#         uvArray = np.zeros((len(uSamples) * len(vSamples), 2))
#
#         for vi, vS in enumerate(vSamples):
#             for ui, uS in enumerate(uSamples):
#                 uvArray[(ui * len(vSamples)) + vi] = [uS, vS]
#
#         pArray = np.array(S.evaluate_list(uvArray))
#
#         for CC, uv in neighbouringUV(len(uSamples), len(vSamples), closedU(), closedV()):
#
#             pNW = pArray[CC[0]]
#             pN  = pArray[CC[1]]
#             pNE = pArray[CC[2]]
#             pE  = pArray[CC[3]]
#             pC =  pArray[CC[4]]
#             pW  = pArray[CC[5]]
#             pSW = pArray[CC[6]]
#             pS  = pArray[CC[7]]
#             pSE = pArray[CC[8]]
#
#             dpuv_NW = np.linalg.norm(p - pNW)
#             dpuv_N  = np.linalg.norm(p - pN)
#             dpuv_NE = np.linalg.norm(p - pNE)
#             dpuv_E  = np.linalg.norm(p - pE)
#             dpuv    = np.linalg.norm(p - pC)
#             dpuv_W  = np.linalg.norm(p - pW)
#             dpuv_SW = np.linalg.norm(p - pSW)
#             dpuv_S  = np.linalg.norm(p - pS)
#             dpuv_SE = np.linalg.norm(p - pSE)
#
#             testSample = [pNW, pN, pNE, pW, pC, pE, pSW, pS, pSE]
#
#             if maxSearch:
#                 if ((dpuv >= dpuv_NW) and (dpuv >= dpuv_N) and (dpuv >= dpuv_NE) and
#                         (dpuv >= dpuv_W) and (dpuv >= dpuv_E) and
#                         (dpuv >= dpuv_SW) and (dpuv >= dpuv_S) and (dpuv >= dpuv_SE)):
#
#                     #b_NW = np.linalg.norm(pC - pNW)
#                     b_N = np.linalg.norm(pC - pN)
#                     #b_NE = np.linalg.norm(pC - pNE)
#                     b_E = np.linalg.norm(pC - pE)
#
#                     b_W = np.linalg.norm(pC - pW)
#                     #b_SW = np.linalg.norm(pC - pSW)
#                     b_S = np.linalg.norm(pC - pS)
#                     #b_SE = np.linalg.norm(pC - pSE)
#
#                     maxBound.append(max([b_N, b_E, b_W, b_S]))
#                     extremaUV.append(uvArray[CC[4]]) # uv[0]*S.sample_size_v + uv[1]
#                     extremaP.append(pArray[CC[4]])
#
#             else:  # minS
#                 if ((dpuv <= dpuv_NW) and (dpuv <= dpuv_N) and (dpuv <= dpuv_NE) and
#                         (dpuv <= dpuv_W) and (dpuv <= dpuv_E) and
#                         (dpuv <= dpuv_SW) and (dpuv <= dpuv_S) and (dpuv <= dpuv_SE)):
#                     # where a point is orthogonal to a planar surface -> sphere radius test, or accept minima
#
#
#                     #b_NW = np.linalg.norm(pC - pNW)
#                     b_N = np.linalg.norm(pC - pN)
#                     #b_NE = np.linalg.norm(pC - pNE)
#                     b_E = np.linalg.norm(pC - pE)
#
#                     b_W = np.linalg.norm(pC - pW)
#                     #b_SW = np.linalg.norm(pC - pSW)
#                     b_S = np.linalg.norm(pC - pS)
#                     #b_SE = np.linalg.norm(pC - pSE)
#
#                     maxBound.append(max([b_N, b_E, b_W, b_S]))
#                     extremaUV.append(uvArray[CC[4]])
#                     extremaP.append(pArray[CC[4]])
#
#         return extremaUV, extremaP, maxBound
#
#     def curvatureSearch():
#
#         # not S.evalpts
#         uSamples = np.linspace(S.knotvector_u[0], S.knotvector_u[-1], num=len(S.knotvector_u) * deltaFactor,
#                                endpoint=(closedU()))
#         vSamples = np.linspace(S.knotvector_v[0], S.knotvector_v[-1], num=len(S.knotvector_v) * deltaFactor,
#                                endpoint=(closedV()))
#         uvArray = np.zeros((len(uSamples) * len(vSamples), 2))
#
#         for vi, vS in enumerate(vSamples):
#             for ui, uS in enumerate(uSamples):
#                 uvArray[(ui * len(vSamples)) + vi] = [uS, vS]
#
#         pArray = np.array(S.evaluate_list(uvArray))
#
#         discreteK = [np.inf] * len(uvArray)
#         for CC, uv in neighbouringUV(len(uSamples), len(vSamples), closedU(), closedV()):
#
#             puv = np.array(pArray[CC[4]])
#             pur, pucc = radiusCentre3points_2(pArray[CC[3]], puv, pArray[CC[5]])
#             pvr, pvcc = radiusCentre3points_2(pArray[CC[7]], puv, pArray[CC[1]])
#
#             # S.evalpts[(S.sample_size_u * u) + v]
#
#             # find media point of U curvature centre and V curvature, kuv
#             if pur is np.inf and pvr is not np.inf:
#                 kuv = pvcc - puv
#             elif pur is not np.inf and pvr is np.inf:
#                 kuv = pucc - puv
#             elif pur is np.inf and pvr is np.inf:
#                 discreteK[(S.sample_size_u * uv[0]) + uv[1]] = np.inf
#                 break
#             else:
#                 kuv = ((pucc - puv) + (pvcc - puv)) / 2
#
#             dkuv = np.linalg.norm(kuv - puv)
#
#             if np.linalg.norm(p - puv) > np.linalg.norm(kuv - p):
#                 # p is on the same side of the surface as the median curvature
#                 discreteK[(S.sample_size_u * uv[0]) + uv[1]] = dkuv
#
#             if np.linalg.norm(p - puv) < np.linalg.norm(kuv - p):
#                 # smallest radius, with curvature centre on far side of surface from p
#                 discreteK[(S.sample_size_u * uv[0]) + uv[1]] = -dkuv
#
#             # todo: examine issue with non-colinear curve centre and projecting point
#             # e.g. proj(p)
#             #                 if np.dot(p - p1, pucc - p1)/np.linalg.norm(pucc - p1) > 0:
#             #                     discreteK[i] = pur
#             #                 else:
#             #                     discreteK[i] = -pur
#
#         return discreteK
#
#     def localCurvatureExtrema(discreteK):
#         leu = []
#
#         for CC, uv in neighbouringUV(S.sample_size_u, S.sample_size_v, closedU(), closedV()):
#             surroundNodes = [CC[1], CC[3], CC[5], CC[7]]
#
#             if not any(discreteK[ssn] == np.inf for ssn in surroundNodes):
#                 if maxSearch:
#                     if (all((np.abs(discreteK[ssn]) >= discreteK[(S.sample_size_u * uv[0]) + uv[1]]) for ssn in surroundNodes) and
#                             (discreteK[(S.sample_size_u * uv[0]) + uv[1]] > 0)):
#                         # this version excludes negative curves from surrounding curvature values
#                         # if all(np.abs(discreteK[ssn]) > discreteK[S.sample_size_u * u + v] or
#                         # (discreteK[ssn]<0) for ssn in surroundNodes) and
#                         # (discreteK[S.sample_size_u * u + v] > 0):
#                         leu.append(uv)
#                 else:
#                     if all((np.abs(discreteK[ssn]) >= discreteK[(S.sample_size_u * uv[0]) + uv[1]]) for ssn in
#                            surroundNodes) and (discreteK[(S.sample_size_u * uv[0]) + uv[1]] < 0):
#                         leu.append(uv)
#
#         # , localExtremaP, maxBound
#         return leu
#
#     def subSearchUV(mpU, mpV, tol):
#         # create simple subdivisions around a minimum point as simple hillclimb search
#         tolFlag = 0
#         divU = S.delta_u
#         divV = S.delta_v
#         dpuvlocal = np.linalg.norm(p - np.array(S.evaluate_single((mpU, mpV,))))
#         #dpuvlocal = np.inner(p - np.array(S.evaluate_single((mpU, mpV,))))
#
#         while not tolFlag:
#             divU /= 2
#             divV /= 2
#
#             subDivs = [[mpU - divU, mpV + divV],
#                        [mpU, mpV + divV],
#                        [mpU + divU, mpV + divV],
#                        [mpU - divU, mpV],
#                        [mpU, mpV],
#                        [mpU + divU, mpV],
#                        [mpU - divU, mpV - divV],
#                        [mpU, mpV - divV],
#                        [mpU + divU, mpV - divV]]
#
#             for sdv in subDivs:
#                 if sdv[0] > 1.0: sdv[0] = 1.0
#                 if sdv[0] < 0.0: sdv[0] = 0.0
#                 if sdv[1] > 1.0: sdv[1] = 1.0
#                 if sdv[1] < 0.0: sdv[1] = 0.0
#
#             pSubDivs = S.evaluate_list(subDivs)
#
#             # test np.inner
#             # dispUV = [np.inner(p - psd) for psd in pSubDivs]
#
#             dpuv_NW = np.linalg.norm(p - pSubDivs[0])
#             dpuv_N = np.linalg.norm(p - pSubDivs[1])
#             dpuv_NE = np.linalg.norm(p - pSubDivs[2])
#
#             dpuv_W = np.linalg.norm(p - pSubDivs[3])
#             # dpuv = np.linalg.norm(p - pSubDivs[4])
#             dpuv_E = np.linalg.norm(p - pSubDivs[5])
#
#             dpuv_SW = np.linalg.norm(p - pSubDivs[6])
#             dpuv_S = np.linalg.norm(p - pSubDivs[7])
#             dpuv_SE = np.linalg.norm(p - pSubDivs[8])
#
#             dispUV = [dpuv_NW,
#                       dpuv_N,
#                       dpuv_NE,
#                       dpuv_W,
#                       #dpuvlocal,
#                       dpuv_E,
#                       dpuv_SW,
#                       dpuv_S,
#                       dpuv_SE]
#
#             if maxSearch:
#                 if np.abs(max(dispUV) - dpuvlocal) >= tol:
#                     #np.abs(max(dispUV) - dpuvlocal) >= tol:
#                     maxUVindex = dispUV.index(max(dispUV))
#                     mpU = subDivs[maxUVindex][0]
#                     mpV = subDivs[maxUVindex][1]
#                     dpuvlocal = max(dispUV)
#                 else:
#                     tolFlag += 1
#             else:
#                 if np.abs(min(dispUV) - dpuvlocal) >= tol:
#                     minUVindex = dispUV.index(min(dispUV))
#                     mpU = subDivs[minUVindex][0]
#                     mpV = subDivs[minUVindex][1]
#                     dpuvlocal = min(dispUV)
#                 else:
#                     tolFlag += 1
#         return (mpU, mpV,)
#
#     def NewtonRaphson(cuv, maxits=5):
#
#         # def f(uv):
#         #     return surface.derivatives(uv[0], uv[1], 2)
#
#         # e[0][0], the surface point itself
#         # e[0][1], the 1st derivative w.r.t. v
#         # e[2][1], the 2nd derivative w.r.t. u and 1st derivative w.r.t. v
#
#         def n(uv, e, r):  # np.array inputs
#             #   f = Su(u,v) * r = 0
#             #   g = Sv(u,v) * r = 0
#
#             Su = e[1][0]
#             Sv = e[0][1]
#
#             Suu = e[2][0]
#             Svv = e[0][2]
#
#             Suv = e[1][1]
#             Svu = e[1][1]
#
#             f = np.dot(Su, r)
#             g = np.dot(Sv, r)
#             k = [-f, -g]
#
#             J00 = np.dot(Su, Su) + np.dot(Suu, r)
#             J01 = np.dot(Su, Sv) + np.dot(Suv, r)
#             J10 = np.dot(Su, Sv) + np.dot(Svu, r)
#             J11 = np.dot(Sv, Sv) + np.dot(Svv, r)
#
#             # d =   [ u* - u, v* - v ]
#             # k = - [ f(u,v), g(u,v) ]
#             # J =   |Su|^2   +  Suu * r       Su*Sv  +  Suv * r
#             #        Su*Sv   +  Svu * r      |Sv|^2  +  Svv * r
#
#             J = [[J00, J01], [J10, J11]]
#             d = np.linalg.solve(J, k)
#             return d + uv
#
#         i = 0
#         # e = None
#         while i < maxits:
#             if (cuv[0] < 0) or (cuv[0] > 1) or (cuv[1] < 0) or (cuv[1] > 1):
#                 return (np.inf, np.inf) # derivatives sometimes > |2|
#
#             #test = S.derivatives(1.0, 0.0, 0)
#
#             e = np.array(S.derivatives(cuv[0], cuv[1], 2))
#             dif = e[0][0] - p # e[0][0] is surface point evaluated at cuv(u,v)
#             c1v = np.linalg.norm(dif)
#             c1 = c1v < eps1 #  |S(u,v) - p| < e1 (point coincidence)
#
#             #  |Su(u,v)*(S(u,v) - P)|
#             #  ----------------------  < e2 (cosine minima)
#             #  |Su(u,v)| |S(u,v) - P|
#
#             c2an = np.dot(e[1][0], dif)
#             c2ad = np.linalg.norm(e[1][0]) * c1v
#             c2av = c2an / c2ad
#             c2a = c2av < eps2
#
#             #  |Sv(u,v)*(S(u,v) - P)|
#             #  ----------------------  < e2 (cosine minima)
#             #  |Sv(u,v)| |S(u,v) - P|
#
#             c2bn = np.dot(e[0][1], dif)
#             c2bd = np.linalg.norm(e[0][1]) * c1v
#             c2bv = c2bn / c2bd
#             c2b = c2bv < eps2
#
#             # exit if all tolerance are met,
#             if (c1 and c2a and c2b):
#                 return cuv
#
#             # otherwise, take a step
#             ct = n(cuv, e, dif)
#
#             #  correct for exceeding bounds
#             if ct[0] < S.knotvector_u[0]: # [ maxu - ( ct[0] - minu ), ct[1] ]
#                 if closedU(): ct = [S.knotvector_u[-1] - (S.knotvector_u[0] - ct[0]), ct[1]]
#                 #if closedU(): ct = [S.knotvector_u[0] + ct[0] % (S.knotvector_u[-1] - S.knotvector_u[0]), ct[1]]
#                 else: ct = [S.knotvector_u[0] + eps_bspline, ct[1]]
#
#             elif ct[0] > S.knotvector_u[-1]:
#                 if closedU(): ct = [S.knotvector_u[0] + (ct[0] - S.knotvector_u[-1]), ct[1]]
#                 #if closedU(): ct = [S.knotvector_u[-1] - ct[0] % (S.knotvector_u[-1] - S.knotvector_u[0]), ct[1]]
#                 else: ct = [S.knotvector_u[-1] - eps_bspline, ct[1]]
#
#             if ct[1] < S.knotvector_v[0]:
#                 if closedV(): ct = [ct[0], S.knotvector_v[-1] - (S.knotvector_v[0] - ct[1])]
#                 #if closedV(): ct = [ct[0], S.knotvector_v[0] + ct[1] % (S.knotvector_v[-1] - S.knotvector_v[0])]
#                 else: ct = [ct[0], S.knotvector_v[0] + eps_bspline]
#
#             elif ct[1] > S.knotvector_v[-1]:
#                 #if closedV(): ct = [ct[0], S.knotvector_v[0] + (ct[1] - S.knotvector_v[-1])]
#                 if closedV(): ct = [ct[0], S.knotvector_v[-1] - ct[1] % (S.knotvector_v[-1] - S.knotvector_v[0])]
#                 else: ct = [ct[0], S.knotvector_v[-1] - eps_bspline]
#
#             c3v0 = np.linalg.norm((ct[0] - cuv[0]) * e[1][0])
#             c3v1 = np.linalg.norm((ct[1] - cuv[1]) * e[0][1])
#
#             # if |(u* - u) C'(u)| < e1, halt
#             if (c3v0 + c3v1 < eps1):
#                 return cuv
#             cuv = ct
#             i += 1
#
#         if i == maxits:  # Newton-Raphson fails
#             return (np.inf, np.inf)
#         return cuv
#
#     if curvatureTest:  # discrete grid summing curvatures along U and V axes
#         localExtremaUV, localExtremaP, maxBound = localCurvatureExtrema(curvatureSearch())
#     else:
#         localExtremaUV, localExtremaP, maxBound = localNeighbourSearch(S)
#
#     extremaUV = []
#     for luv, lp, mb in zip(localExtremaUV, localExtremaP, maxBound):
#         #mUV = subSearchUV(luv[0], luv[1], eps_bspline)
#         mUV = NewtonRaphson(luv)
#
#         if not np.inf in mUV:
#             # find a maximum deviation to indicate when N-R fails
#             if np.linalg.norm(lp - np.array(S.evaluate_single(mUV))) <= mb:
#                 extremaUV.append(mUV)
#             else:
#                 mUV = (np.inf, np.inf)
#         if np.inf in mUV:
#             # Newton-Raphson fail, try hillclimb
#             mUV = subSearchUV(luv[0], luv[1], eps_bspline)
#             extremaUV.append(mUV)
#         # print(S.evalpts[(S.sample_size_u * luv[0]) + luv[1]])
#
#     # filter out identical values
#     extremaUnique = []
#     for m in extremaUV:
#         # displacement between any 2 points < eps1
#         # compare u, v individually to reduce comparison pool
#         if not any([np.isclose(m, u, eps1).all() for u in extremaUnique]):
#             extremaUnique.append(m)
#
#     # print(S.evaluate_list(extremaUnique))
#
#     if len(extremaUnique) == 0: return []
#
#     if (localExtrema and (len(extremaUnique) == 1)) or not localExtrema:  # if there is only one extrema, localextrema is moot
#         if uv_xyz: return extremaUnique # return single value in list for compatibility
#         else: return [np.array(S.evaluate_single(extremaUnique[0])),]
#     else:
#         if localExtrema:
#             if uv_xyz: return extremaUnique
#             else: return S.evaluate_list(extremaUnique)
#         else: # return single maxima
#             extremaUniquePoint = S.evaluate_list(extremaUnique)
#             dispsExtrema = [np.linalg.norm(np.array(e) - p) for e in extremaUniquePoint]
#             if maxSearch: # return single value in list for compatibility
#                 if uv_xyz: return [extremaUnique[dispsExtrema.index(max(dispsExtrema))],]
#                 else: return [np.array(extremaUniquePoint[dispsExtrema.index(max(dispsExtrema))]),]
#             else: # minsearch
#                 if uv_xyz: return [extremaUnique[dispsExtrema.index(min(dispsExtrema))],]
#                 else: return [np.array(extremaUniquePoint[dispsExtrema.index(min(dispsExtrema))]),]


# def rationalSurfaceExtremaParam_4(S, p,
#                                   maxSearch=True,
#                                   localExtrema=False,
#                                   curvatureTest=False,
#                                   uv_xyz=True,
#                                   eps1=0.0001,
#                                   eps2=0.0005,
#                                   delta=0.025,
#                                   eps_bspline = 1E-10):
#     '''
#     Use either Newton-Raphson, or a hillclimbing neighbour search to find minimal or maximal distance on a surface, S, from a supplied point, p
#     - not tested with surfaces closed along U & V
#     maxSearch: search for most distant point from p if true, else nearest if false
#     localExtrema: find any local extrema which differs from surrounding sample points, tolerance not sufficiently defined
#     curvatureTest: rather than displacement of sampled points to provide seed locations for N-R/hillclimb search,
#     use a combination of the centres of curvature at any point across U-axis and V-axis to identify C2 inflection point.
#     uv_xyz: return u,v normalised values or cartesian x,y,z values
#     eps1: point coincidence limit, halt N-R on reaching this minima
#     eps2: cosine angle limit, halt N-R on reaching this minima
#     delta: sample interval dividing surface into points
#     '''
#
#     # easy to find a minimum, but a maximum found through the original hillclimbing algorithm will settle on local maxima
#     # for surfaces, minimize the following:
#     #
#     # f = Su(u,v) * r = 0
#     # g = Sv(u,v) * r = 0
#     #
#     #   where r = S(u,v) - P
#     #
#     # Requires Newton-Raphson iteration, objective function is vector valued
#     #
#     #     J d = k
#     #
#     #     d =   [ u* - u, v* - v ] (alternatively u[i+1] -  u[i], v[i+1]- v[i])
#     #     k = - [ f(u,v), g(u,v) ]
#     #     J =     |Su|^2   +  Suu * r       Su*Sv  +  Suv * r
#     #              Su*Sv   +  Svu * r      |Sv|^2  +  Svv * r
#     #
#     # halting conditions:
#     # point coincidence
#     #
#     #         |S(u,v) - p| < e1
#     #
#     # cosine
#     #
#     #    |Su(u,v)*(S(u,v) - P)|
#     #    ----------------------  < e2
#     #    |Su(u,v)| |S(u,v) - P|
#     #
#     #    |Sv(u,v)*(S(u,v) - P)|
#     #    ----------------------  < e2
#     #    |Sv(u,v)| |S(u,v) - P|
#     #
#     # 1) first check 2 & 3
#     # 2) if at least one of these is not, compute new value, otherwise halt
#     # 3) ensure the parameter stays within range
#     #     * if not closed, don't allow outside of range a-b
#     #     * if closed (e.g. circle), allow to move back to beginning
#     # 4)  if |(u* - u)C'(u)| < e1, halt
#
#     # todo: take value from STEP fields
#     # check if surface closed along U and V
#
#     def closedU():
#         return np.isclose([np.array(s[0]) - np.array(s[-1]) for s in S.ctrlpts2d], eps_bspline).all()
#
#     def closedV():
#         return np.isclose(np.array(S.ctrlpts2d[0]) - np.array(S.ctrlpts2d[-1]), eps_bspline).all()  # eps_STEP_AP21
#
#     S.delta = delta  # set evaluation delta
#     # tradeoff between sample density and feature identification
#
#     if S.evalpts == None:  # evaluate surface points---------appears redundant
#         S.evaluate()
#
#     def neighbouringUV(S):
#         # generator function to yield neighbouring indices over a matrix of size U x V
#
#         U = S.sample_size_u
#         V = S.sample_size_v
#
#         openU = not closedU()
#         openV = not closedV()
#
#         for v in range(openV, V - openV):
#             for u in range(openU, U - openU):
#
#                 if openV == 0 and openU == 0:
#                     if ((u == 0 and v == 0) or
#                         (u == 0 and v == V - 1) or
#                         (u == U - 1 and v == 0) or
#                         (u == U - 1 and v == V - 1)):
#                         # how does the topology of closed U & V work anyway?
#                         continue
#
#                 if u == 0:  # closed in u direction
#                     if v == V - 1:  # closed in v direction
#                         NW = (U * (v + 1)) - 1
#                     else:
#                         NW = (U * (v + 2)) - 1
#
#                     W = (U * (v + 1)) - 1
#
#                     if v == 0:
#                         SW = U - 1
#                     else:
#                         SW = (U * v) - 1
#
#                     if v == V - 1:  # closed in v direction
#                         N = u
#                     else:
#                         N = U * (v + 1)
#
#                     C = U * v
#
#                     if v == 0:
#                         S = U
#                     else:
#                         S = U * (v - 1)
#
#                 if u == 1:
#                     if v == V - 1:  # closed in v direction
#                         NW = u - 1
#                     else:
#                         NW = U * (v + 1)
#
#                     W = U * v
#
#                     if v == 0:
#                         SW = U * (V - 1)
#                     else:
#                         SW = U * (v - 1)  # ??
#
#                     if v == V - 1:  # closed in v direction
#                         N = u
#                     else:
#                         N = (U * (v + 1)) + 1
#
#                     C = (U * v) + 1
#
#                     if v == 0:
#                         S = (U * (V - 1)) + 1
#                     else:
#                         S = (U * (v - 1)) + 1
#
#                 if u > 1:
#                     NW = N
#                     W = C  # central instance
#                     SW = S
#
#                     N = NE
#                     C = E
#                     S = SE
#
#                 elif u == U - 1:  # closed in u direction
#                     if v == V - 1:  # closed in v direction
#                         NE = u
#                     else:
#                         NE = (U * (v + 1)) + 1
#
#                     E = (U * v) + 1
#
#                     if v == 0:
#                         SE = (U * (V - 1)) + u + 1
#                     else:
#                         SE = (U * (v - 2)) + 1
#
#                 if u < U - 1:  # only have to calculate this once per loop
#                     if v == V - 1:  # closed in v direction
#                         NE = u + 1
#                     else:
#                         NE = (U * (v + 1)) + u + 1
#
#                     E = (U * v) + u + 1
#
#                     if v == 0:
#                         SE = (U * (V - 1)) + u + 1
#                     else:
#                         SE = (U * (v - 1)) + u + 1
#
#                 # print([NW, N, NE])
#                 # print([W, C, E])
#                 # print([SW, S, SE])
#                 # print("==========")
#
#                 yield ([NW, N, NE, W, C, E, SW, S, SE], (v, u))
#
#     def localNeighbourSearch():
#         extremaUV = []
#
#         for CC, uv in neighbouringUV(S):
#             dpuv_NW = np.linalg.norm(p - np.array(S.evalpts[CC[0]]))
#             dpuv_N  = np.linalg.norm(p - np.array(S.evalpts[CC[1]]))
#             dpuv_NE = np.linalg.norm(p - np.array(S.evalpts[CC[2]]))
#             dpuv_E  = np.linalg.norm(p - np.array(S.evalpts[CC[3]]))
#             dpuv    = np.linalg.norm(p - np.array(S.evalpts[CC[4]]))
#             dpuv_W  = np.linalg.norm(p - np.array(S.evalpts[CC[5]]))
#             dpuv_SW = np.linalg.norm(p - np.array(S.evalpts[CC[6]]))
#             dpuv_S  = np.linalg.norm(p - np.array(S.evalpts[CC[7]]))
#             dpuv_SE = np.linalg.norm(p - np.array(S.evalpts[CC[8]]))
#
#             if maxSearch:
#                 if ((dpuv >= dpuv_NW) and (dpuv >= dpuv_N) and (dpuv >= dpuv_NE) and
#                         (dpuv >= dpuv_W) and (dpuv >= dpuv_E) and
#                         (dpuv >= dpuv_SW) and (dpuv >= dpuv_S) and (dpuv >= dpuv_SE)):
#                     extremaUV.append(uv)
#             else:  # minS
#                 if ((dpuv <= dpuv_NW) and (dpuv <= dpuv_N) and (dpuv <= dpuv_NE) and
#                         (dpuv <= dpuv_W) and (dpuv <= dpuv_E) and
#                         (dpuv <= dpuv_SW) and (dpuv <= dpuv_S) and (dpuv <= dpuv_SE)):
#                     # where a point is orthogonal to a planar surface -> sphere radius test, or accept minima
#                     extremaUV.append(uv)
#
#         return extremaUV
#
#     def curvatureSearch():
#         discreteK = [np.inf] * len(S.evalpts)
#         for CC, uv in neighbouringUV(S):
#
#             puv = np.array(S.evalpts[CC[4]])
#             pur, pucc = radiusCentre3points_2(np.array(S.evalpts[CC[3]]), puv, np.array(S.evalpts[CC[5]]))
#             pvr, pvcc = radiusCentre3points_2(np.array(S.evalpts[CC[7]]), puv, np.array(S.evalpts[CC[1]]))
#
#             # S.evalpts[(S.sample_size_u * u) + v]
#
#             # find media point of U curvature centre and V curvature, kuv
#             if pur is np.inf and pvr is not np.inf:
#                 kuv = pvcc - puv
#             elif pur is not np.inf and pvr is np.inf:
#                 kuv = pucc - puv
#             elif pur is np.inf and pvr is np.inf:
#                 discreteK[(S.sample_size_u * uv[0]) + uv[1]] = np.inf
#                 break
#             else:
#                 kuv = ((pucc - puv) + (pvcc - puv)) / 2
#
#             dkuv = np.linalg.norm(kuv - puv)
#
#             if np.linalg.norm(p - puv) > np.linalg.norm(kuv - p):
#                 # p is on the same side of the surface as the median curvature
#                 discreteK[(S.sample_size_u * uv[0]) + uv[1]] = dkuv
#
#             if np.linalg.norm(p - puv) < np.linalg.norm(kuv - p):
#                 # smallest radius, with curvature centre on far side of surface from p
#                 discreteK[(S.sample_size_u * uv[0]) + uv[1]] = -dkuv
#
#             # todo: examine issue with non-colinear curve centre and projecting point
#             # e.g. proj(p)
#             #                 if np.dot(p - p1, pucc - p1)/np.linalg.norm(pucc - p1) > 0:
#             #                     discreteK[i] = pur
#             #                 else:
#             #                     discreteK[i] = -pur
#
#         return discreteK
#
#     def localCurvatureExtrema(discreteK):
#         leu = []
#         for CC, uv in neighbouringUV(S):
#             surroundNodes = [CC[1], CC[3], CC[5], CC[7]]
#
#             if not any(discreteK[ssn] == np.inf for ssn in surroundNodes):
#                 if maxSearch:
#                     if (all((np.abs(discreteK[ssn]) >= discreteK[(S.sample_size_u * uv[0]) + uv[1]]) for ssn in surroundNodes) and
#                             (discreteK[(S.sample_size_u * uv[0]) + uv[1]] > 0)):
#                         # this version excludes negative curves from surrounding curvature values
#                         # if all(np.abs(discreteK[ssn]) > discreteK[S.sample_size_u * u + v] or
#                         # (discreteK[ssn]<0) for ssn in surroundNodes) and
#                         # (discreteK[S.sample_size_u * u + v] > 0):
#                         leu.append(uv)
#                 else:
#                     if all((np.abs(discreteK[ssn]) >= discreteK[(S.sample_size_u * uv[0]) + uv[1]]) for ssn in
#                            surroundNodes) and (discreteK[(S.sample_size_u * uv[0]) + uv[1]] < 0):
#                         leu.append(uv)
#         return leu
#
#     def subSearchUV(mpU, mpV, tol):
#         # create simple subdivisions around a minimum point as simple hillclimb search
#         tolFlag = 0
#         divU = S.delta_u
#         divV = S.delta_v
#         dpuvlocal = np.linalg.norm(p - S.evaluate_single((mpU, mpV,)))
#
#         while not tolFlag:
#             divU /= 2
#             divV /= 2
#
#             subDivs = [(mpU - divU, mpV + divV),
#                        (mpU, mpV + divV),
#                        (mpU + divU, mpV + divV),
#                        (mpU - divU, mpV),
#                        (mpU, mpV),
#                        (mpU + divU, mpV),
#                        (mpU - divU, mpV - divV),
#                        (mpU, mpV - divV),
#                        (mpU + divU, mpV - divV)]
#
#             pSubDivs = S.evaluate_list(subDivs)
#
#             dpuv_NW = np.linalg.norm(p - pSubDivs[0])
#             dpuv_N = np.linalg.norm(p - pSubDivs[1])
#             dpuv_NE = np.linalg.norm(p - pSubDivs[2])
#
#             dpuv_W = np.linalg.norm(p - pSubDivs[3])
#             # dpuv = np.linalg.norm(p - pSubDivs[4])
#             dpuv_E = np.linalg.norm(p - pSubDivs[5])
#
#             dpuv_SW = np.linalg.norm(p - pSubDivs[6])
#             dpuv_S = np.linalg.norm(p - pSubDivs[7])
#             dpuv_SE = np.linalg.norm(p - pSubDivs[8])
#
#             dispUV = [dpuv_NW,
#                       dpuv_N,
#                       dpuv_NE,
#                       dpuv_W,
#                       dpuvlocal,
#                       dpuv_E,
#                       dpuv_SW,
#                       dpuv_S,
#                       dpuv_SE]
#
#             if maxSearch:
#                 if np.abs(max(dispUV) - dpuvlocal) >= tol:
#                     maxUVindex = dispUV.index(max(dispUV))
#                     mpU = subDivs[maxUVindex][0]
#                     mpV = subDivs[maxUVindex][1]
#                     dpuvlocal = max(dispUV)
#                 else:
#                     tolFlag += 1
#             else:
#                 if np.abs(min(dispUV) - dpuvlocal) >= tol:
#                     minUVindex = dispUV.index(min(dispUV))
#                     mpU = subDivs[minUVindex][0]
#                     mpV = subDivs[minUVindex][1]
#                     dpuvlocal = min(dispUV)
#                 else:
#                     tolFlag += 1
#         return (mpU, mpV,)
#
#     def NewtonRaphson(cuv, maxits=5):
#
#         # def f(uv):
#         #     return surface.derivatives(uv[0], uv[1], 2)
#
#         def n(uv, e, r):  # np.array inputs
#             #   f = Su(u,v) * r = 0
#             #   g = Sv(u,v) * r = 0
#
#             Su = e[1][0]
#             Sv = e[0][1]
#
#             Suu = e[2][0]
#             Svv = e[0][2]
#
#             Suv = e[1][1]
#             Svu = e[1][1]
#
#             f = np.dot(Su, r)
#             g = np.dot(Sv, r)
#             k = [-f, -g]
#
#             J00 = np.dot(Su, Su) + np.dot(Suu, r)
#             J01 = np.dot(Su, Sv) + np.dot(Suv, r)
#             J10 = np.dot(Su, Sv) + np.dot(Svu, r)
#             J11 = np.dot(Sv, Sv) + np.dot(Svv, r)
#
#             # d =   [ u* - u, v* - v ]
#             # k = - [ f(u,v), g(u,v) ]
#             # J =   |Su|^2   +  Suu * r       Su*Sv  +  Suv * r
#             #        Su*Sv   +  Svu * r      |Sv|^2  +  Svv * r
#
#             J = [[J00, J01], [J10, J11]]
#             d = np.linalg.solve(J, k)
#             return d + uv
#
#         i = 0
#         # e = None
#         while i < maxits:
#
#             e = np.array(S.derivatives(cuv[0], cuv[1], 2))
#             dif = e[0][0] - p
#             c1v = np.linalg.norm(dif)   #  |S(u,v) - p| < e1 (point coincidence)
#
#             #  |Su(u,v)*(S(u,v) - P)|
#             #  ----------------------  < e2 (cosine minima)
#             #  |Su(u,v)| |S(u,v) - P|
#
#             c2an = np.dot(e[1][0], dif)
#             c2ad = np.linalg.norm(e[1][0]) * c1v
#             c2bn = np.dot(e[0][1], dif)
#             c2bd = np.linalg.norm(e[0][1]) * c1v
#             c2av = c2an / c2ad
#             c2bv = c2bn / c2bd
#             c1 = c1v < eps1
#             c2a = c2av < eps2
#             c2b = c2bv < eps2
#
#             # exit if all tolerance are met,
#             if (c1 and c2a and c2b): return cuv
#
#             # otherwise, take a step
#             ct = n(cuv, e, dif)
#
#             #  correct for exceeding bounds
#             if ct[0] < S.knotvector_u[0]: # [ maxu - ( ct[0] - minu ), ct[1] ]
#                 #if closedU(): ct = [S.knotvector_u[-1] - (ct[0] - S.knotvector_u[0]), ct[1]] #- incorrect in Piegel Tiller NURBS book
#                 if closedU(): ct = [S.knotvector_u[-1] - (S.knotvector_u[0] - ct[0]), ct[1]]
#                 else: ct = [S.knotvector_u[0] + eps_bspline, ct[1]]
#
#             elif ct[0] > S.knotvector_u[-1]: # [ minu + ( ct[0] - maxu ), ct[1] ]
#                 #if closedU(): ct = [S.knotvector_u[0] + (ct[0] - S.knotvector_u[-1]), ct[1]]
#                 if closedU(): ct = [S.knotvector_u[0] + (ct[0] % S.knotvector_u[-1]), ct[1]]
#                 #if closedU(): ct = [S.knotvector_u[0] + (S.knotvector_u[-1] - ct[0]), ct[1]]
#                 else: ct = [S.knotvector_u[-1] - eps_bspline, ct[1]]
#
#             if ct[1] < S.knotvector_v[0]: # [ ct[0], maxv - ( ct[1] - minv ) ]
#                 if closedV(): ct = [ct[0], S.knotvector_v[-1] - (ct[1] - S.knotvector_v[0])]
#                 #if closedV(): ct = [ct[0], S.knotvector_v[-1] - (S.knotvector_v[0] - ct[1])]
#                 else: ct = [ct[0], S.knotvector_v[0] + eps_bspline]
#
#             elif ct[1] > S.knotvector_v[-1]: # [ ct[0], minv + ( ct[0] - maxv ) ]
#                 if closedV(): ct = [ct[0], S.knotvector_v[0] + (ct[0] - S.knotvector_v[-1])]
#                 else: ct = [ct[0], S.knotvector_v[-1] - eps_bspline]
#
#             # if ( ct[0] < minu ){
#             #                 ct = closedu ? [ maxu - ( ct[0] - minu ), ct[1] ] : [ minu + Constants.EPSILON, ct[1] ];
#             #             } else if (ct[0] > maxu){
#             #                 ct = closedu ? [ minu + ( ct[0] - maxu ), ct[1] ] : [ maxu - Constants.EPSILON, ct[1] ];
#             #             }
#             #
#             #             if ( ct[1] < minv ){
#             #                 ct = closedv ? [ ct[0], maxv - ( ct[1] - minv ) ] : [ ct[0], minv + Constants.EPSILON ];
#             #             } else if (ct[1] > maxv){
#             #                 ct = closedv ? [ ct[0], minv + ( ct[0] - maxv ) ] : [ ct[0], maxv - Constants.EPSILON ];
#             #             }
#
#             c3v0 = np.linalg.norm((ct[0] - cuv[0]) * e[1][0])
#             c3v1 = np.linalg.norm((ct[1] - cuv[1]) * e[0][1])
#
#             # if |(u* - u) C'(u)| < e1, halt
#             if (c3v0 + c3v1 < eps1):
#                 return cuv
#             cuv = ct
#             i += 1
#
#         if i == maxits:  # Newton-Raphson fails
#             return (np.inf, np.inf)
#         return cuv
#
#     if curvatureTest:  # discrete grid summing curvatures along U and V axes
#         localExtremaUV = localCurvatureExtrema(curvatureSearch())
#     else:
#         localExtremaUV = localNeighbourSearch()
#
#     extremaUV = []
#     for luv in localExtremaUV:
#         # print(luv[0] * S.delta_u, luv[1] * S.delta_v,)
#         # tc = NewtonRaphson((luv[0] * S.delta_u, luv[1] * S.delta_v,))
#         # print(S.evaluate_single(tc))
#         mUV = NewtonRaphson((luv[0] * S.delta_u, luv[1] * S.delta_v,))
#         if np.inf not in mUV:
#             extremaUV.append(mUV)
#         else:
#             # Newton-Raphson fail, try hillclimb
#             mUV = subSearchUV(luv[0] * S.delta_u, luv[1] * S.delta_v, eps_bspline)
#             extremaUV.append(mUV)
#         # print(S.evalpts[(S.sample_size_u * luv[0]) + luv[1]])
#
#     # filter out identical values
#     extremaUnique = []
#     for m in extremaUV:
#         # displacement between any 2 points < eps1
#         # compare u, v individually to reduce comparison pool
#         if not any([np.isclose(m, u, eps1).all() for u in extremaUnique]):
#             extremaUnique.append(m)
#
#     # print(S.evaluate_list(extremaUnique))
#
#     if len(extremaUnique) == 0: return []
#
#     # if localExtrema and (len(extremaUnique) > 1):  # if there is only one extrema, localextrema is moot
#     #     if uv_xyz: return extremaUnique
#     #     else: return S.evaluate_list(extremaUnique)
#     # else:
#     #     extremaUniquePoint = S.evaluate_list(extremaUnique)
#     #     dispsExtrema = [np.linalg.norm(np.array(e) - p) for e in extremaUniquePoint]
#     #     if maxSearch:
#     #         if uv_xyz: return [extremaUnique[dispsExtrema.index(max(dispsExtrema))],]
#     #         else: return [extremaUniquePoint[dispsExtrema.index(max(dispsExtrema))],]
#     #     else:
#     #         if uv_xyz: return [extremaUnique[dispsExtrema.index(min(dispsExtrema))],]
#     #         else: return [extremaUniquePoint[dispsExtrema.index(min(dispsExtrema))].]
#
#     if (localExtrema and (len(extremaUnique) == 1)) or not localExtrema:  # if there is only one extrema, localextrema is moot
#         if uv_xyz: return extremaUnique # return single value in list for compatibility
#         else: return [np.array(S.evaluate_single(extremaUnique[0])),]
#     else:
#         if localExtrema:
#             if uv_xyz: return extremaUnique
#             else: return S.evaluate_list(extremaUnique)
#         else: # return single maxima
#             extremaUniquePoint = S.evaluate_list(extremaUnique)
#             dispsExtrema = [np.linalg.norm(np.array(e) - p) for e in extremaUniquePoint]
#             if maxSearch: # return single value in list for compatibility
#                 if uv_xyz: return [extremaUnique[dispsExtrema.index(max(dispsExtrema))],]
#                 else: return [np.array(extremaUniquePoint[dispsExtrema.index(max(dispsExtrema))]),]
#             else: # minsearch
#                 if uv_xyz: return [extremaUnique[dispsExtrema.index(min(dispsExtrema))],]
#                 else: return [np.array(extremaUniquePoint[dispsExtrema.index(min(dispsExtrema))]),]


# def BsplineCurveExtremaDisp(curve, p,
#                             maxSearch=True,
#                             localExtrema=False,
#                             curvatureTest=False,
#                             uv_xyz=True,
#                             eps1=0.0001,
#                             eps2=0.0005,
#                             delta=0.025,
#                             eps_bspline = 1E-10):
#
#     # based on Nurbs Book, Piegl & Tiller p.230
#     # same idea to find orthogonal tangent, but at maximum/minimum displacement from a centroid point
#     # revised to test minima segment candidates using 3-point defined circle parameters
#
#     # def radiusCentre3points2(p1, p2, p3):
#     #     # radius + centre from 3 point circle
#     #
#     #     #collinear test
#     #     coords = np.array([p1, p2, p3])
#     #     coords -= coords[0]  # offset for collinear points to intersect the origin
#     #     if np.linalg.matrix_rank(coords, tol=None) == 1:
#     #         return np.inf, []
#     #
#     #     t = p2 - p1
#     #     u = p3 - p1
#     #     v = p3 - p2
#     #
#     #     w = np.cross(t, u)  # triangle normal
#     #     wsl = np.linalg.norm(w)
#     #     if (wsl < 10e-14):
#     #         return False  # triangle area too small
#     #
#     #     wsl = np.dot(w, w)
#     #     iwsl2 = 1. / (2. * wsl)
#     #     tt = np.dot(t, t)
#     #     uu = np.dot(u, u)
#     #
#     #     circCenter = p1 + (u * tt * (np.dot(u, v)) - t * uu * (np.dot(t, v))) * iwsl2
#     #     circRadius = np.sqrt(tt * uu * (np.dot(v, v)) * iwsl2 * 0.5)
#     #     # circAxis   = w / np.sqrt(wsl)
#     #
#     #     return circRadius, circCenter
#
#     # eps1 = 0.0001   # Euclidean distance measure
#     # eps2 = 0.0005   # zero cosine measure
#     # eps_bspline = 1E-10
#     minU = curve.knotvector[0]
#     maxU = curve.knotvector[-1]
#
#     def closedC():
#         return np.isclose(np.linalg.norm(np.array(curve.ctrlpts[0]) - np.array(curve.ctrlpts[-1])), eps_bspline)
#
#     def subSearchU(C, mpU, tol):
#         # create simple subdivisions around a minimum point on curve as simple hillclimb search
#         tolFlag = 0
#         divU = C.delta
#         dpulocal = np.linalg.norm(p - C.evaluate_single(mpU))
#
#         while not tolFlag:
#             divU /= 2
#             subDivs = [mpU - divU, mpU, mpU + divU]
#             pSubDivs = C.evaluate_list(subDivs)
#
#             dpu_L = np.linalg.norm(p - pSubDivs[0])
#             dpu_R = np.linalg.norm(p - pSubDivs[1])
#
#             dispU = [dpu_L, dpulocal, dpu_R]
#
#             if maxSearch:
#                 if np.abs(max(dispU) - dpulocal) >= tol:
#                     maxUindex = dispU.index(max(dispU))
#                     mpU = subDivs[maxUindex]
#                     dpulocal = max(dispU)
#                 else:
#                     tolFlag += 1
#             else:
#                 if np.abs(min(dispU) - dpulocal) >= tol:
#                     minUindex = dispU.index(min(dispU))
#                     mpU = subDivs[minUindex]
#                     dpulocal = min(dispU)
#                 else:
#                     tolFlag += 1
#         return mpU
#
#     def NewtonRaphson(cu, maxits=5):
#
#         def n(u2, e1, d):
#             #   Newton's method: 	 u* = u - f / f'
#             #   use product rule to form derivative, f':   f' = C"(u) * ( C(u) - p ) + C'(u) * C'(u)
#             #   d:  ( C(u) - p )
#             return u2 - (np.dot(e1[1], d) / (np.dot(e1[2], d) + np.dot(e1[1], e1[1])))
#
#         i = 0
#         while (i < maxits):
#             # Newton iteration to find orthogonal tangent and is max/min agnostic
#             e = np.array(curve.derivatives(cu, order=2))  # f(cu)
#             dif = e[0] - p                      # C(u) - p
#             c1v = np.linalg.norm(dif)           # |C(u) - p|
#             c2n = np.dot(e[1], dif)             # C'(u) * (C(u) - P)
#             c2d = np.linalg.norm(e[1]) * c1v    # |C'(u)||C(u) - P|
#             c2v = c2n / c2d                     # |C'(u) * (C(u) - P)| / |C'(u)||C(u) - P|
#             # c2v = np.dot(e[1], dif) / (np.linalg.norm(e[1]) * np.linalg.norm(dif))
#
#             if (c1v < eps1) and (np.abs(c2v) < eps2):
#                 return cu
#             # ct = n(cu, e, dif)                           #   u* = u - f / f'
#             ct = cu - (np.dot(e[1], dif) / (np.dot(e[2], dif) + np.dot(e[1], e[1])))
#
#             if ct < minU:
#                 if closedC(): ct = maxU - (minU - ct) # ct = maxU - (ct - minU) # NURBS book
#                 else: ct = minU
#
#             elif ct > maxU:
#                 if closedC(): ct = minU + (ct - maxU)
#                 else: ct = maxU
#
#             c3v = np.linalg.norm(np.multiply(ct - cu, e[1]))
#             if c3v < eps1:
#                 return cu
#
#             cu = ct
#             i += 1
#
#         if i == maxits:  # Newton-Raphson fails
#             return np.inf
#         return cu
#
#     # numSamples = curve.ctrlpts_size * curve.degree * curveSampleFactor
#     # span = (curve.knotvector[-1] - curve.knotvector[0]) / (numSamples - 1)
#     # kvs = np.array([curve.knotvector[0] + (span * i) for i in range(0, numSamples)])
#     # pts = curve.evaluate_list(kvs)
#
#     #pts, kvs = splineToPolyline(curve, curveSampleFactor=delta) todo 1/curve.delta?
#     pts, kvs = splineToPolyline(curve, curveSampleFactor=2)
#
#     # original version fails on points of equal minimal curvature
#
#     # 2-part selection for global maxima using both displacement and curve inflection
#     # curve inflection determined from local curvature minima - prob doesn't work for self intersecting curves
#     # get set of local minima curvature - use for curve characterisation
#     # from set, find minima/maxima disp from point-of-interest to curve point tangent
#
#     # curvature approach fails without C2 continuity at minima/maxima?
#     # not much of an issue with STEP spline limitations
#
#     if curvatureTest: discreteK = [np.inf] * len(pts)
#
#     localDisp = [0] * len(pts)
#     localExtremaU = []
#
#     # get discrete curvature values and point displacement
#     for i in range(1-closedC(), len(pts) - 1 + closedC()):
#         if (i == 0):
#             p0 = np.array(pts[-1])
#             p1 = np.array(pts[0])
#             p2 = np.array(pts[1])
#
#         elif (i == len(pts)-1):
#             p0 = np.array(pts[-2])
#             p1 = np.array(pts[-1])
#             p2 = np.array(pts[0])
#
#         else:
#             p0 = np.array(pts[i - 1])
#             p1 = np.array(pts[i])
#             p2 = np.array(pts[i + 1])
#
#         if i==0 or i==1 or i==len(pts)-1:
#             dp0 = np.linalg.norm(p0 - p)
#             dp1 = np.linalg.norm(p1 - p)
#             dp2 = np.linalg.norm(p2 - p)
#
#         if (i>1) and (i<len(pts)-1):
#             dp0 = dp1
#             dp1 = dp2
#             dp2 = np.linalg.norm(p2 - p)
#
#         if maxSearch:
#             if (dp1 >= dp2) and (dp1 >= dp0):
#                 localExtremaU.append(kvs[i])
#         else:  # minS
#             if (dp1 <= dp2) and (dp1 <= dp0):
#                 # where a point is orthogonal to a planar surface -> sphere radius test, or accept minima
#                 localExtremaU.append(kvs[i])
#
#         localDisp[i] = dp1
#
#         if curvatureTest:
#             pur, pucc = radiusCentre3points_2(p0, p1, p2)
#             if (pur == np.inf):
#                 break
#             else:
#                 # p is on the same side of the surface as the mean curvature
#                 if np.dot(p - p1, pucc - p1)/np.linalg.norm(pucc - p1) > 0:
#                     discreteK[i] = pur
#                 else:
#                     discreteK[i] = -pur
#
#     if curvatureTest:  # discrete grid summing curvatures along U and V axes
#         localExtremaU = []
#
#         # get local max curvature values
#         for i in range(1 - closedC(), len(discreteK) - 1 + closedC()):
#             if (i == 0):
#                 k0 = discreteK[-1]
#                 k1 = discreteK[0]
#                 k2 = discreteK[1]
#
#             elif (i == len(discreteK) - 1):
#                 k0 = discreteK[-2]
#                 k1 = discreteK[-1]
#                 k2 = discreteK[0]
#
#             else:
#                 k0 = discreteK[i - 1]
#                 k1 = discreteK[i]
#                 k2 = discreteK[i + 1]
#
#             if (k0 != np.inf) and (k1 != np.inf) and (k2 != np.inf):
#                 if (np.abs(k1) <= np.abs(k2)) and (np.abs(k1) <= np.abs(k0)):
#                     if (k1 > 0) and maxSearch:
#                         localExtremaU.append(kvs[i])
#                     elif (k1 < 0) and not maxSearch:
#                         # where a point is orthogonal to a planar surface -> sphere radius test, or accept minima
#                         localExtremaU.append(kvs[i])
#
#     extremaU = []
#     for lu in localExtremaU:
#         mU = NewtonRaphson(lu)
#         if mU != np.inf:
#             extremaU.append(mU)
#         else:
#             # Newton-Raphson fail, try hillclimb
#             mU = subSearchU(curve, lu, eps_bspline)
#             extremaU.append(mU)
#
#     # filter out identical values
#     extremaUnique = []
#     for m in extremaU:
#         # displacement between any 2 points < eps1
#         # compare u, v individually to reduce comparison pool
#         if not any(np.isclose(m, u, eps1).all() for u in extremaUnique):
#             extremaUnique.append(m)
#
#     if len(extremaU) == 0: #return []
#         # minima/maxima at curve ends
#         if all([localDisp[1] < ll for ll in localDisp[2:-2]]): # u = 0
#             if uv_xyz: return [0.,]
#             else: return curve.evaluate_single(0.)
#         elif all([localDisp[-2] > ll for ll in localDisp[1:-1:-3]]): # u = 1
#             if uv_xyz: return [1.,]
#             else: return curve.evaluate_single(1.)
#         else:
#             print("Bspline assumption fail")
#
#     if (localExtrema and (len(extremaUnique) == 1)) or not localExtrema:  # if there is only one extrema, localextrema is moot
#         if uv_xyz: return [extremaUnique,] # return single value in list for compatibility
#         else: return [np.array(curve.evaluate_single(extremaUnique)),]
#     else:
#         if localExtrema:
#             if uv_xyz: return extremaUnique
#             else: return curve.evaluate_list(extremaUnique)
#         else: # return single maxima
#             extremaUniquePoint = curve.evaluate_list(extremaUnique)
#             dispsExtrema = [np.linalg.norm(np.array(e) - p) for e in extremaUniquePoint]
#             if maxSearch: # return single value in list for compatibility
#                 if uv_xyz: return [extremaUnique[dispsExtrema.index(max(dispsExtrema))],]
#                 else: return [np.array(extremaUniquePoint[dispsExtrema.index(max(dispsExtrema))]),]
#             else: # minsearch
#                 if uv_xyz: return [extremaUnique[dispsExtrema.index(min(dispsExtrema))],]
#                 else: return [np.array(extremaUniquePoint[dispsExtrema.index(min(dispsExtrema))]),]


def BsplineCurveExtremaDisp(C, p,
                            maxSearch=True,
                            localExtrema=False,
                            curvatureTest=False,
                            uv_xyz=True,
                            eps1=0.0001,
                            eps2=0.0005,
                            deltaFactor=4,
                            eps_bspline = 1E-10):

    # based on Nurbs Book, Piegl & Tiller p.230
    # same idea to find orthogonal tangent, but at maximum/minimum displacement from a centroid point
    # revised to test minima segment candidates using 3-point defined circle parameters

    # def radiusCentre3points2(p1, p2, p3):
    #     # radius + centre from 3 point circle
    #
    #     #collinear test
    #     coords = np.array([p1, p2, p3])
    #     coords -= coords[0]  # offset for collinear points to intersect the origin
    #     if np.linalg.matrix_rank(coords, tol=None) == 1:
    #         return np.inf, []
    #
    #     t = p2 - p1
    #     u = p3 - p1
    #     v = p3 - p2
    #
    #     w = np.cross(t, u)  # triangle normal
    #     wsl = np.linalg.norm(w)
    #     if (wsl < 10e-14):
    #         return False  # triangle area too small
    #
    #     wsl = np.dot(w, w)
    #     iwsl2 = 1. / (2. * wsl)
    #     tt = np.dot(t, t)
    #     uu = np.dot(u, u)
    #
    #     circCenter = p1 + (u * tt * (np.dot(u, v)) - t * uu * (np.dot(t, v))) * iwsl2
    #     circRadius = np.sqrt(tt * uu * (np.dot(v, v)) * iwsl2 * 0.5)
    #     # circAxis   = w / np.sqrt(wsl)
    #
    #     return circRadius, circCenter

    # eps1 = 0.0001   # Euclidean distance measure
    # eps2 = 0.0005   # zero cosine measure
    # eps_bspline = 1E-10
    # minU = C.knotvector[0]
    # maxU = C.knotvector[-1]

    C.delta = 1 / len(C.knotvector) * deltaFactor

    def closedC():
        return np.isclose(np.linalg.norm(np.array(C.ctrlpts[0]) - np.array(C.ctrlpts[-1])), eps_bspline)

    def localDispTest():
        #localDisp = [0] * len(pts)
        localExtremaU = []

        # get discrete curvature values and point displacement
        for i in range(1 - closedC(), len(pts) - 1 + closedC()):
            if (i == 0):
                p0 = np.array(pts[-2])
                p1 = np.array(pts[0])
                p2 = np.array(pts[1])

            elif (i == len(pts) - 1):
                p0 = np.array(pts[-2])
                p1 = np.array(pts[0])
                p2 = np.array(pts[1])

            else:
                p0 = np.array(pts[i - 1])
                p1 = np.array(pts[i])
                p2 = np.array(pts[i + 1])

            if i == 0 or i == 1 or i == len(pts) - 1:
                dp0 = np.linalg.norm(p0 - p)
                dp1 = np.linalg.norm(p1 - p)
                dp2 = np.linalg.norm(p2 - p)

            if (i > 1) and (i < len(pts) - 1):
                dp0 = dp1
                dp1 = dp2
                dp2 = np.linalg.norm(p2 - p)

            if maxSearch:
                if (dp1 >= dp2) and (dp1 >= dp0):
                    localExtremaU.append(kvs[i])
            else:  # minS
                if (dp1 <= dp2) and (dp1 <= dp0):
                    # where a point is orthogonal to a planar surface -> sphere radius test, or accept minima
                    localExtremaU.append(kvs[i])

            #localDisp[i] = dp1

        return localExtremaU

    def localCurvatureTest():
        discreteK = [np.inf] * len(pts)
        localExtremaU = []

        # get discrete curvature values and point displacement
        for i in range(1 - closedC(), len(pts) - 1 + closedC()):
            if (i == 0):
                p0 = np.array(pts[-2])
                p1 = np.array(pts[0])
                p2 = np.array(pts[1])

            elif (i == len(pts) - 1):
                p0 = np.array(pts[-2])
                p1 = np.array(pts[-1])
                p2 = np.array(pts[1])

            else:
                p0 = np.array(pts[i - 1])
                p1 = np.array(pts[i])
                p2 = np.array(pts[i + 1])

            pur, pucc = radiusCentre3points(p0, p1, p2)
            if not pur:
                break
            if (pur == np.inf):
                break
            else:
                # p is on the same side of the surface as the mean curvature
                if np.dot(p - p1, pucc - p1) / np.linalg.norm(pucc - p1) > 0:
                    discreteK[i] = pur
                else:
                    discreteK[i] = -pur

        # get local max curvature values
        for i in range(1 - closedC(), len(discreteK) - 1 + closedC()):
            if (i == 0):
                k0 = discreteK[-2]
                k1 = discreteK[0]
                k2 = discreteK[1]

            elif (i == len(discreteK) - 1):
                k0 = discreteK[-2]
                k1 = discreteK[-1]
                k2 = discreteK[0]

            else:
                k0 = discreteK[i - 1]
                k1 = discreteK[i]
                k2 = discreteK[i + 1]

            if (k0 != np.inf) and (k1 != np.inf) and (k2 != np.inf):
                if (np.abs(k1) <= np.abs(k2)) and (np.abs(k1) <= np.abs(k0)):
                    if (k1 > 0) and maxSearch:
                        localExtremaU.append(kvs[i])
                    elif (k1 < 0) and not maxSearch:
                        # where a point is orthogonal to a planar surface -> sphere radius test, or accept minima
                        localExtremaU.append(kvs[i])

        return localExtremaU

    def subSearchU(C, mpU, tol):
        # create simple subdivisions around a minimum point on curve as simple hillclimb search
        tolFlag = 0
        divU = C.delta
        dpulocal = np.linalg.norm(p - C.evaluate_single(mpU))

        while not tolFlag:
            divU /= 2
            subDivs = [mpU - divU, mpU, mpU + divU]
            pSubDivs = C.evaluate_list(subDivs)

            dpu_L = np.linalg.norm(p - pSubDivs[0])
            dpu_R = np.linalg.norm(p - pSubDivs[1])

            dispU = [dpu_L, dpulocal, dpu_R]

            if maxSearch:
                if np.abs(max(dispU) - dpulocal) >= tol:
                    maxUindex = dispU.index(max(dispU))
                    mpU = subDivs[maxUindex]
                    dpulocal = max(dispU)
                else:
                    tolFlag += 1
            else:
                if np.abs(min(dispU) - dpulocal) >= tol:
                    minUindex = dispU.index(min(dispU))
                    mpU = subDivs[minUindex]
                    dpulocal = min(dispU)
                else:
                    tolFlag += 1
        return mpU

    def NewtonRaphson(cu, maxits=5):

        def n(u2, e1, d):
            #   Newton's method: 	 u* = u - f / f'
            #   use product rule to form derivative, f':   f' = C"(u) * ( C(u) - p ) + C'(u) * C'(u)
            #   d:  ( C(u) - p )
            return u2 - (np.dot(e1[1], d) / (np.dot(e1[2], d) + np.dot(e1[1], e1[1])))

        i = 0
        while (i < maxits):
            # Newton iteration to find orthogonal tangent and is max/min agnostic
            e = np.array(C.derivatives(cu, order=2))  # f(cu)
            dif = e[0] - p                      # C(u) - p
            c1v = np.linalg.norm(dif)           # |C(u) - p|
            c2n = np.dot(e[1], dif)             # C'(u) * (C(u) - P)
            c2d = np.linalg.norm(e[1]) * c1v    # |C'(u)||C(u) - P|
            c2v = c2n / c2d                     # |C'(u) * (C(u) - P)| / |C'(u)||C(u) - P|
            # c2v = np.dot(e[1], dif) / (np.linalg.norm(e[1]) * np.linalg.norm(dif))

            if (c1v < eps1) and (np.abs(c2v) < eps2):
                return cu
            # ct = n(cu, e, dif)                           #   u* = u - f / f'
            ct = cu - (np.dot(e[1], dif) / (np.dot(e[2], dif) + np.dot(e[1], e[1])))

            #  correct for exceeding bounds
            if ct < C.knotvector[0]:  # [ maxu - ( ct[0] - minu ), ct[1] ]
                if closedC(): ct = C.knotvector[0] + ct % (C.knotvector[-1] - C.knotvector[0])
                #if closedC(): ct = C.knotvector[-1] - (C.knotvector[0] - ct) # ct = C.knotvector[-1] - (ct - C.knotvector[0]) # NURBS book
                else: ct = C.knotvector[0] + eps_bspline

            elif ct > C.knotvector[-1]:
                #if closedC(): ct = C.knotvector[0] + (ct - C.knotvector[-1])
                if closedC(): ct = C.knotvector[-1] - ct % (C.knotvector[-1] - C.knotvector[0])
                else: ct = C.knotvector[-1] - eps_bspline

            c3v = np.linalg.norm(np.multiply(ct - cu, e[1]))
            if c3v < eps1:
                return cu

            cu = ct
            i += 1

        if i == maxits:  # Newton-Raphson fails
            return np.inf
        return cu

    # numSamples = C.ctrlpts_size * C.degree * CSampleFactor
    # span = (C.knotvector[-1] - C.knotvector[0]) / (numSamples - 1)
    # kvs = np.array([C.knotvector[0] + (span * i) for i in range(0, numSamples)])
    # pts = C.evaluate_list(kvs)

    #pts, kvs = splineToPolyline(C, curveSampleFactor=delta) todo 1/C.delta?
    pts, kvs = splineToPolyline(C, curveSampleFactor=deltaFactor)

    # original version fails on points of equal minimal curvature

    # 2-part selection for global maxima using both displacement and curve inflection
    # curve inflection determined from local curvature minima - prob doesn't work for self intersecting curves
    # get set of local minima curvature - use for curve characterisation
    # from set, find minima/maxima disp from point-of-interest to curve point tangent

    # curvature approach fails without C2 continuity at minima/maxima?
    # not much of an issue with STEP spline limitations
    #
    # localDisp = [0] * len(pts)
    # localExtremaU = []
    #
    # # get discrete curvature values and point displacement
    # for i in range(1-closedC(), len(pts) - 1 + closedC()):
    #     if (i == 0):
    #         p0 = np.array(pts[-1])
    #         p1 = np.array(pts[0])
    #         p2 = np.array(pts[1])
    #
    #     elif (i == len(pts)-1):
    #         p0 = np.array(pts[-2])
    #         p1 = np.array(pts[-1])
    #         p2 = np.array(pts[0])
    #
    #     else:
    #         p0 = np.array(pts[i - 1])
    #         p1 = np.array(pts[i])
    #         p2 = np.array(pts[i + 1])
    #
    #     if i==0 or i==1 or i==len(pts)-1:
    #         dp0 = np.linalg.norm(p0 - p)
    #         dp1 = np.linalg.norm(p1 - p)
    #         dp2 = np.linalg.norm(p2 - p)
    #
    #     if (i>1) and (i<len(pts)-1):
    #         dp0 = dp1
    #         dp1 = dp2
    #         dp2 = np.linalg.norm(p2 - p)
    #
    #     if maxSearch:
    #         if (dp1 >= dp2) and (dp1 >= dp0):
    #             localExtremaU.append(kvs[i])
    #     else:  # minS
    #         if (dp1 <= dp2) and (dp1 <= dp0):
    #             # where a point is orthogonal to a planar surface -> sphere radius test, or accept minima
    #             localExtremaU.append(kvs[i])
    #
    #     localDisp[i] = dp1
    #
    #     if curvatureTest:
    #         pur, pucc = radiusCentre3points_2(p0, p1, p2)
    #         if (pur == np.inf):
    #             break
    #         else:
    #             # p is on the same side of the surface as the mean curvature
    #             if np.dot(p - p1, pucc - p1)/np.linalg.norm(pucc - p1) > 0:
    #                 discreteK[i] = pur
    #             else:
    #                 discreteK[i] = -pur

    if curvatureTest:  # discrete grid summing curvatures along U and V axes
        localExtremaU = localCurvatureTest()
    else: # localDispTest()
        localExtremaU = localDispTest()

    if len(localExtremaU) == 0: return []

    extremaU = []
    for lu in localExtremaU:
        mU = NewtonRaphson(lu)
        if mU != np.inf:
            extremaU.append(mU)
        else:
            # Newton-Raphson fail, try hillclimb
            mU = subSearchU(C, lu, eps_bspline)
            extremaU.append(mU)

    # filter out identical values
    extremaUnique = []
    for m in extremaU:
        # displacement between any 2 points < eps1
        # compare u, v individually to reduce comparison pool
        if not any(np.isclose(m, u, eps1).all() for u in extremaUnique):
            extremaUnique.append(m)

    if (localExtrema and (len(extremaUnique) == 1)) or not localExtrema:  # if there is only one extrema, localextrema is moot
        if uv_xyz: return extremaUnique # return single value in list for compatibility
        else: return [np.array(C.evaluate_single(extremaUnique[0])),]
    else:
        if localExtrema:
            if uv_xyz: return extremaUnique
            else: return C.evaluate_list(extremaUnique)
        else: # return single maxima
            extremaUniquePoint = C.evaluate_list(extremaUnique)
            dispsExtrema = [np.linalg.norm(np.array(e) - p) for e in extremaUniquePoint]
            if maxSearch: # return single value in list for compatibility
                if uv_xyz: return [extremaUnique[dispsExtrema.index(max(dispsExtrema))],]
                else: return [np.array(extremaUniquePoint[dispsExtrema.index(max(dispsExtrema))]),]
            else: # minsearch
                if uv_xyz: return [extremaUnique[dispsExtrema.index(min(dispsExtrema))],]
                else: return [np.array(extremaUniquePoint[dispsExtrema.index(min(dispsExtrema))]),]

    # if len(extremaU) == 0: #return []
    #     # minima/maxima at curve ends
    #     if all([localDisp[1] < ll for ll in localDisp[2:-2]]): # u = 0
    #         if uv_xyz: return [0.,]
    #         else: return C.evaluate_single(0.)
    #     elif all([localDisp[-2] > ll for ll in localDisp[1:-1:-3]]): # u = 1
    #         if uv_xyz: return [1.,]
    #         else: return C.evaluate_single(1.)
    #     else:
    #         print("Bspline assumption fail")

    # if (localExtrema and (len(extremaUnique) == 1)) or not localExtrema:  # if there is only one extrema, localextrema is moot
    #     if uv_xyz: return [extremaUnique,] # return single value in list for compatibility
    #     else: return [np.array(C.evaluate_single(extremaUnique)),]
    # else:
    #     if localExtrema:
    #         if uv_xyz: return extremaUnique
    #         else: return C.evaluate_list(extremaUnique)
    #     else: # return single maxima
    #         extremaUniquePoint = C.evaluate_list(extremaUnique)
    #         dispsExtrema = [np.linalg.norm(np.array(e) - p) for e in extremaUniquePoint]
    #         if maxSearch: # return single value in list for compatibility
    #             if uv_xyz: return [extremaUnique[dispsExtrema.index(max(dispsExtrema))],]
    #             else: return [np.array(extremaUniquePoint[dispsExtrema.index(max(dispsExtrema))]),]
    #         else: # minsearch
    #             if uv_xyz: return [extremaUnique[dispsExtrema.index(min(dispsExtrema))],]
    #             else: return [np.array(extremaUniquePoint[dispsExtrema.index(min(dispsExtrema))]),]




def pointInArc(p, v1, v2, _refDir, _auxDir, _normDir, aCentreP, rotSym=True):
    # (note: if VOR, reverse v1, v2 entries)
    # test if points are on arc between vertex1 & vertex2
    # convert aCentreP, p, v1 & v2 to local coordinates with arcAxisDir as z
    # p may be point or list, returns boolean
    # if not rotSym, no checks for equal radius (e.g. ellipse)

    rotM = np.array([_refDir, _auxDir, _normDir])

    v1UV = np.matmul(rotM, v1 - aCentreP)
    v1UV_angle = np.arctan2(v1UV[1], v1UV[0])

    v2UV = np.matmul(rotM, v2 - aCentreP)
    v2UV_angle = np.arctan2(v2UV[1], v2UV[0])

    if isinstance(p, list):
        pUV_angle = []
        for pp in p:
            ppUV = np.matmul(rotM, pp - aCentreP)
            ppUV_angle = np.arctan2(ppUV[1], ppUV[0])
            pUV_angle.append(ppUV_angle)
    elif isinstance(p, np.ndarray):
        pUV = np.matmul(rotM, p - aCentreP)
        pUV_angle = np.arctan2(pUV[1], pUV[0])

    # sanity check that Z-values are identical
    if isinstance(p, list):
        zTest = [pp[2] for pp in pUV]
        zTest = zTest + [v1UV[2], v2UV[2]]
    else:
        zTest = [pUV[2], v1UV[2], v2UV[2]]

    # if not np.isclose(max(zTest), min(zTest)):
    planeTestFail = 0
    if max(zTest) - min(zTest) > eps_STEP_AP21:
        planeTestFail += 1
        #print("pointInArc() transform error: " + str(max(zTest) - min(zTest)))
        #raise RuntimeError("error")

    # check if vertex order reversed before calling function
    if isinstance(p, list):
        returnList = []
        for ppUV_angle in pUV_angle:
            if (((ppUV_angle > v1UV_angle) and (ppUV_angle < v2UV_angle)) or
                    (v1UV_angle == v2UV_angle)):
                returnList.append(True)
            else:
                returnList.append(False)

        if any(returnList):
            print("pointInArc() transform error: " + str(max(zTest) - min(zTest)))

        return returnList

    else:
        returnBool = (((pUV_angle >= v1UV_angle) and (pUV_angle <= v2UV_angle)) or
                (v1UV_angle == v2UV_angle))

        if returnBool and not planeTestFail:
            print("pointInArc() transform deviation: " + str(max(zTest) - min(zTest)))

        return returnBool


def pointOrderInArc(p, v1, v2, _refDir, _auxDir, _normDir, aCentreP, rotSym=True):
    # (note: if VOR, reverse v1, v2 entries)
    # determine the order of a list of cartesian points around a supplied axis
    # convert aCentreP, p, v1 & v2 to local coordinates with arcAxisDir as z
    # no requirement for equal radius (e.g. ellipse)

    print("pointOrderInArc() undertested")

    if not isinstance(p, list):
        p = [p,]
        print("pointOrderInArc() input point(s) in list")

    rotM = np.array([_refDir, _auxDir, _normDir])

    v1UV = np.matmul(rotM, v1 - aCentreP)
    v1UV_angle = np.arctan2(v1UV[1], v1UV[0])

    v2UV = np.matmul(rotM, v2 - aCentreP)
    v2UV_angle = np.arctan2(v2UV[1], v2UV[0])

    if np.abs(v1UV_angle) < eps_STEP_AP21:
        v1UV_angle = 0.0
    if (v1UV_angle - v2UV_angle) < eps_STEP_AP21:
        v2UV_angle = v1UV_angle + (np.pi * 2)

    # pUV_angle = []
    # if isinstance(p, list):
    #     for pp in p:
    #         ppUV = np.matmul(rotM, pp - aCentreP)
    #         ppUV_angle = np.arctan2(ppUV[1], ppUV[0])
    #         pUV_angle.append(ppUV_angle)
    # elif isinstance(p, np.ndarray):
    #     pUV = np.matmul(rotM, p - aCentreP)
    #     pUV_angle.append(np.arctan2(pUV[1], pUV[0]))

    pUV_angle = []
    for pp in p:
        # if pp is None:
        #     _1=1
        ppUV = np.matmul(rotM, pp - aCentreP)
        ppUV_angle = np.arctan2(ppUV[1], ppUV[0])
        ppUV_angle -= v1UV_angle
        if ppUV_angle < 0: ppUV_angle += (2 * np.pi)
        pUV_angle.append(ppUV_angle % (2 * np.pi))


    # # sanity check that Z-values are identical
    # if isinstance(p, list):
    #     zTest = [pp[2] for pp in pUV]
    #     zTest = zTest + [v1UV[2], v2UV[2]]
    # else:
    #     zTest = [pUV[2], v1UV[2], v2UV[2]]
    #
    # # if not np.isclose(max(zTest), min(zTest)):
    # planeTestFail = 0
    # if max(zTest) - min(zTest) > eps_STEP_AP21:
    #     planeTestFail += 1
    #     #print("pointInArc() transform error: " + str(max(zTest) - min(zTest)))
    #     #raise RuntimeError("error")

    # check if vertex order reversed before calling function

    #if (v1UV_angle < v2UV_angle): ???
    withinVertex1andVertex2 = [((pUV_a >= v1UV_angle) and (pUV_a <= v2UV_angle)) for pUV_a in pUV_angle]

    return withinVertex1andVertex2, np.argsort(pUV_angle), [pUV_a/(2 * np.pi) for pUV_a in pUV_angle]


def pointProjectAxis(p, axP, axN): #todo is this also projection to surface via normal?
    '''
    project point p to axis defined by point axP and normal axN
    p: point to project
    axP: axis point
    axN: axis normal vector
    '''
    vpp = p - axP
    vpn = axP - axN
    return vpn * np.dot(vpp, vpn) / np.dot(vpn, vpn)


def pointCircleMinMaxDisp(p, centrePoint, normAxis, radius, interior=False):
    # Point on circle (including interior points) furthest/closest to point "p"

    # normAxis = auxDir / np.linalg.norm(normAxis)  # unit normal vector

    # distance d of point p to the plane is dot product of plane unit normal, normAxis
    # with the difference vector from the point in the plane P and S: d = (S − P) · u
    # distance is signed: positive when S is on the side of the plane where u faces and
    # negative on the other side. zero, S is in the plane
    # d = (p - plane[0]) * u

    d = np.dot((p - centrePoint), normAxis)

    # point S′ is S projected to the plane, by subtracting d · u from S:
    # S′ = S − d · u = S − ((S − P) · u) · u

    pUV = p - np.dot(d, normAxis)  # point projected to circle along orthogonal
    ppCentreDisp = np.linalg.norm(pUV - centrePoint)

    # if point pUV, projected to circle plane, falls outside circle radius, replace with edge point.

    #centreNormal = False
    if np.isclose(ppCentreDisp, 0):
        # p lies on normal through circle centrePoint,
        return centrePoint, None

    pUVdir = (pUV - centrePoint)/ppCentreDisp

    nUVmin = centrePoint + radius * pUVdir
    nUVmax = centrePoint - radius * pUVdir

    #nUVmin = centrePoint + radius / (ppCentreDisp * (pUV - centrePoint))
    #nUVmax = centrePoint - radius / (ppCentreDisp * (pUV - centrePoint))

    if ppCentreDisp >= radius: # and not interior:
        return nUVmin, nUVmax
    else:
        # if ppCentreDisp < eps_STEP_AP21:
        #     return centrePoint, centrePoint
        # else:
        centreOffset = pUV - centrePoint
        centreOffset = centreOffset/np.linalg.norm(centreOffset)
        nUVmax = centrePoint - radius * centreOffset
        #
        if interior:
            nUVmin = pUV
        else:
            nUVmin = centrePoint + radius * centreOffset

    return nUVmin, nUVmax,


def pointEllipseMinMaxDisp( p,
                            eCentre,
                            eLocalXaxis,
                            eLocalYaxis,
                            eNormalAxis,
                            eMajorRadius,
                            eMinorRadius,
                            interior=False):
    # Robert Nurnberg method, see http://wwwf.imperial.ac.uk/~rn/distance2ellipse.pdf
    # Move origin to ellipse centre, and rotate point coordinate to local coordinates
    # based on ellipse major & minor axis.
    # Assuming direction_ratios of AXIS2_PLACEMENT_3D are normalised

    # eFeatureAxis = arrayTypeCast(eFeatureAxis)
    # eLocalXYaxis = arrayTypeCast(eLocalXYaxis)
    # eCentre = arrayTypeCast(eCentre)

    # cLocalYaxis = np.cross(eLocalXaxis - eCentre, eNormalAxis - eCentre) # STEP AP21 gives axes wrt local origin
    # eLocalYaxis = np.cross(eLocalXaxis, eNormalAxis)
    # eLocalYaxis = eLocalYaxis / np.linalg.norm(eLocalYaxis)  # unit normal vector

    rotM = np.array([eLocalXaxis, eLocalYaxis, eNormalAxis])
    pUV = np.matmul(rotM, p - eCentre)

    if np.linalg.norm(p - eCentre) < eps:
        print("should be 2x maxima/minima, write some code") # todo: ellipse dual min/max
        # maxima will be +/- eMajorRadius points along major axis
        # minima +/- eMinorRadius points on minor axis

    # project to ellipse plane
    pUV = np.array([pUV[0], pUV[1], 0])

    theta = np.arctan2(eMajorRadius * pUV[1], eMinorRadius * pUV[0])
    i = 0
    n0 = pUV
    radiusConst = eMajorRadius**2 - eMinorRadius**2
    pUVdisp = np.linalg.norm(pUV)

    while i < 100:
        i += 1
        f = (
            radiusConst * np.cos(theta) * np.sin(theta)
            - pUV[0] * eMajorRadius * np.sin(theta)
            + pUV[1] * eMinorRadius * np.cos(theta)
        )

        f_ = (
            radiusConst * (np.cos(theta) ** 2 - np.sin(theta) ** 2)
            - pUV[0] * eMajorRadius * np.cos(theta)
            - pUV[1] * eMinorRadius * np.sin(theta)
        )

        theta = theta - f / f_

        n = np.array([eMajorRadius * np.cos(theta), eMinorRadius * np.sin(theta), 0])

        if np.allclose(n0, n):  # tolerance = 1E-12
            if (np.linalg.norm(n) > pUVdisp) and not interior:  # test if p < n, inside ellipse
                nUVmin = np.matmul(rotM.T, pUV) + eCentre
            else:
                nUVmin = np.matmul(rotM.T, n) + eCentre

            # furthest point on ellipse must be edge point on line that contains ellipse centre and closest point
            nUVmax = np.matmul(rotM.T, np.array([-n[0], -n[1], 0])) + eCentre

        n0 = n

    # if interior:
    #     nUVmin = pUV
    # else:
    #     nUVmin = centrePoint + radius * centreOffset

    return nUVmin, nUVmax

# print('p = Draft.make_point( {:1.8f}, {:1.8f}, {:1.8f})'.format(xyz[0], xyz[0], xyz[2]))


def intersectSegmentPlane(vertex0,
                          vertex1,
                          planeNorm,
                          planePoint,
                          precision=eps_STEP_AP21):
    """
    Return intersection point of segment and plane or None if no intersection.

    :param vertex0: segment end point
    :param vertex1: second segment end point
    :param planePoint: point on plane
    :param planeNorm: normal vector defining plane direction (does not need to be normalized)
    :param precision: constant for STEP numerical float comparison
    :return: point of segment plane intersection or none
    """
    s = vertex1 - vertex0
    SprojP = np.dot(planeNorm, s)
    if abs(SprojP) > precision:
        F = -np.dot(planeNorm, (vertex0 - planePoint)) / SprojP
        if 0.0 <= F <= 1.0:
            return vertex0 + np.dot(s, F)  # point intersects segment
        else:
            return None  # intersection beyond segment
    return None  #   segment is parallel to plane


def intersectPlaneCircle(pPoint, pNorm, cPoint, cNorm, cRadius, tol=eps_STEP_AP21):
    """Cintersection of a plane and a circle.
    plane point
    plane norm
    circle point
    circle norm
    circle radius

    There are 4 cases of plane-circle intersection:

    1. intersect in 2 points (secant),
    2. intersect in 1 point (tangent),
    3. do not intersect, or
    4. coincide (circle.plane == plane).
    """
    if np.abs(np.dot(pNorm, cNorm)) >= 1 - tol:
        return None

    d = np.cross(pNorm, cNorm)  # direction of intersection line
    # vector in plane 1 perpendicular to the direction of the intersection line
    v1 = np.cross(d, pNorm)
    p1 = pPoint + v1    # point on plane 1
    L = p1 - pPoint
    cosl = np.dot(cNorm, L)
    if np.abs(cosl) <= tol:
        # if the dot product (cosine of the angle between segment and plane)
        # is close to zero the line and the normal are almost perpendicular
        return None
    L = L * (-np.dot(cNorm, pPoint - cPoint) / cosl)
    l1 = pPoint + L
    l2 = l1 + d
    if (l1 is None) or (l2 is None):
        return None

    a = (l2[0] - l1[0]) ** 2 + (l2[1] - l1[1]) ** 2 + (l2[2] - l1[2]) ** 2
    b = 2.0 * (
        (l2[0] - l1[0]) * (l1[0] - cPoint[0]) +
        (l2[1] - l1[1]) * (l1[1] - cPoint[1]) + (
                l2[2] - l1[2]) * (l1[2] - cPoint[2])
    )

    c = (
        cPoint[0] ** 2
        + sPoint[1] ** 2
        + sPoint[2] ** 2
        + l1[0] ** 2
        + l1[1] ** 2
        + l1[2] ** 2
        - 2.0 * (cPoint[0] * l1[0] + cPoint[1] * l1[1] + cPoint[2] * l1[2])
        - cRadius**2
    )

    i = b * b - 4.0 * a * c
    if i < 0.0:  # case 3: no intersection
        return None
    elif i == 0.0:  # case 2: one intersection
        mu = -b / (2.0 * a)
        ipt = (
            l1[0] + mu * (l2[0] - l1[0]),
            l1[1] + mu * (l2[1] - l1[1]),
            l1[2] + mu * (l2[2] - l1[2]),
        )
        return ipt
    elif i > 0.0:  # case 1: two intersections
        # 1.
        mu = (-b + np.sqrt(i)) / (2.0 * a)
        ipt1 = (
            l1[0] + mu * (l2[0] - l1[0]),
            l1[1] + mu * (l2[1] - l1[1]),
            l1[2] + mu * (l2[2] - l1[2]),
        )
        # 2.
        mu = (-b - np.sqrt(i)) / (2.0 * a)
        ipt2 = (
            l1[0] + mu * (l2[0] - l1[0]),
            l1[1] + mu * (l2[1] - l1[1]),
            l1[2] + mu * (l2[2] - l1[2]),
        )
        return [ipt1, ipt2]


def intersectArcPlane( planeNormal,
                        planePoint,
                        arcCentrePoint,
                        arcRefDir,
                        arcAuxDir,
                        arcNormal,
                        arcRadius,
                        arcVertex1,
                        arcVertex2):

    # planePoint = centroidProjAxisPoint for most calls

    # points within the edge
    validatedPoints = []

    # determine if circle and plane intersect, if so return intersection line/segment (cosine projection?)
    circleIntersectPoints = intersectPlaneCircle(planePoint, planeNormal, arcCentrePoint, arcNormal, arcRadius)
    if circleIntersectPoints is None:
        return []
    if len(circleIntersectPoints) == 0:
        return [] # no intersection
    if len(circleIntersectPoints) == 1:
        print("tangent exception")
        pass
    # check projected segment is between arc vertices,
    for cip in circleIntersectPoints:

        if pointInArc(cip, arcVertex1, arcVertex2, arcRefDir, arcAuxDir, arcNormal, arcCentrePoint, rotSym=True):
            validatedPoints.append(cip)

    return validatedPoints


def pointSegmentMinDisp(p, s0, s1):
    #   https://web.archive.org/web/20220121145748/http://geomalgorithms.com/index.html
    # final two boolean values indicate whether point is at or beyond endpoints of segments
    v = s1 - s0
    w = p - s0

    c1 = np.dot(w, v)
    if ( c1 <= 0 ):
      return s0, np.linalg.norm(w), True, False

    c2 = np.dot(v, v)
    if ( c2 <= c1 ):
      return s1, np.linalg.norm(p - s1), False, True

    b = c1 / c2
    pb = s0 + b * v
    return pb, np.linalg.norm(p - pb), False, False


# def pointSegmentMinDisp(p, a, b):
#     # https://web.archive.org/web/20220121145748/http://geomalgorithms.com/index.html
#     s = b - a
#     w = p - a
#     ps = np.dot(w, s)
#     if ps <= 0:
#         return a, np.linalg.norm(w), True
#     l2 = np.dot(s, s)
#     if ps >= l2:
#         closest = b
#         vertexEnd = True
#     else:
#         closest = a + ps / l2 * s
#         vertexEnd = False
#     return closest, np.linalg.norm(p - closest), vertexEnd


# def planeMinMaxPoint(p, V, minmax='min'):
#     import pymesh
#     def pointTriangleMinDistance(TRI, P):
#         # return distance between a point and triangle
#         # SYNTAX
#         #   dist = pointTriangleDistance(TRI,P)
#         #   [dist,PP0] = pointTriangleDistance(TRI,P)
#         #
#         # DESCRIPTION
#         #   Calculate the distance of a given point P from a triangle TRI.
#         #   Point P is a row vector of the form 1x3. The triangle is a matrix
#         #   formed by three rows of points TRI = [P1;P2;P3] each of size 1x3.
#         #   dist = pointTriangleDistance(TRI,P) returns the distance of the point P
#         #   to the triangle TRI.
#         #   [dist,PP0] = pointTriangleDistance(TRI,P) additionally returns the
#         #   closest point PP0 to P on the triangle TRI.
#         #
#         # Author: Gwolyn Fischer
#         # Release: 1.0
#         # Release date: 09/02/02
#
#         # The algorithm is based on
#         # "David Eberly, 'Distance Between Point and Triangle in 3D',
#         # Geometric Tools, LLC, (1999)"
#         # http:\\www.geometrictools.com/Documentation/DistancePoint3Triangle3.pdf
#         #
#         #        ^t
#         #  \     |
#         #   \reg2|
#         #    \   |
#         #     \  |
#         #      \ |
#         #       \|
#         #        *P2
#         #        |\
#         #        | \
#         #  reg3  |  \ reg1
#         #        |   \
#         #        |reg0\
#         #        |     \
#         #        |      \ P1
#         # -------*-------*------->s
#         #        |P0      \
#         #  reg4  | reg5    \ reg6
#         # rewrite triangle in normal form
#         B = TRI[0, :]
#         E0 = TRI[1, :] - B
#         # E0 = E0/np.sqrt(sum(E0.^2)); %normalize vector
#         E1 = TRI[2, :] - B
#         # E1 = E1/np.sqrt(sum(E1.^2)); %normalize vector
#         D = B - P
#         a = np.dot(E0, E0)
#         b = np.dot(E0, E1)
#         c = np.dot(E1, E1)
#         d = np.dot(E0, D)
#         e = np.dot(E1, D)
#         f = np.dot(D, D)
#
#         # print "{0} {1} {2} ".format(B,E1,E0)
#         det = a * c - b * b
#         s = b * e - c * d
#         t = b * d - a * e
#
#         # Teribble tree of conditionals to determine in which region of the diagram
#         # shown above the projection of the point into the triangle-plane lies.
#         if (s + t) <= det:
#             if s < 0.0:
#                 if t < 0.0:
#                     # region4
#                     if d < 0:
#                         t = 0.0
#                         if -d >= a:
#                             s = 1.0
#                             sqrDistance = a + 2.0 * d + f
#                         else:
#                             s = -d / a
#                             sqrDistance = d * s + f
#                     else:
#                         s = 0.0
#                         if e >= 0.0:
#                             t = 0.0
#                             sqrDistance = f
#                         else:
#                             if -e >= c:
#                                 t = 1.0
#                                 sqrDistance = c + 2.0 * e + f
#                             else:
#                                 t = -e / c
#                                 sqrDistance = e * t + f
#
#                                 # of region 4
#                 else:
#                     # region 3
#                     s = 0
#                     if e >= 0:
#                         t = 0
#                         sqrDistance = f
#                     else:
#                         if -e >= c:
#                             t = 1
#                             sqrDistance = c + 2.0 * e + f
#                         else:
#                             t = -e / c
#                             sqrDistance = e * t + f
#                             # of region 3
#             else:
#                 if t < 0:
#                     # region 5
#                     t = 0
#                     if d >= 0:
#                         s = 0
#                         sqrDistance = f
#                     else:
#                         if -d >= a:
#                             s = 1
#                             sqrDistance = a + 2.0 * d + f
#                             # GF 20101013 fixed typo d*s ->2*d
#                         else:
#                             s = -d / a
#                             sqrDistance = d * s + f
#                 else:
#                     # region 0
#                     invDet = 1.0 / det
#                     s = s * invDet
#                     t = t * invDet
#                     sqrDistance = (
#                             s * (a * s + b * t + 2.0 * d) + t * (b * s + c * t + 2.0 * e) + f
#                     )
#         else:
#             if s < 0.0:
#                 # region 2
#                 tmp0 = b + d
#                 tmp1 = c + e
#                 if tmp1 > tmp0:  # minimum on edge s+t=1
#                     numer = tmp1 - tmp0
#                     denom = a - 2.0 * b + c
#                     if numer >= denom:
#                         s = 1.0
#                         t = 0.0
#                         sqrDistance = a + 2.0 * d + f
#                         # GF 20101014 fixed typo 2*b -> 2*d
#                     else:
#                         s = numer / denom
#                         t = 1 - s
#                         sqrDistance = (
#                                 s * (a * s + b * t + 2 * d) + t * (b * s + c * t + 2 * e) + f
#                         )
#
#                 else:  # minimum on edge s=0
#                     s = 0.0
#                     if tmp1 <= 0.0:
#                         t = 1
#                         sqrDistance = c + 2.0 * e + f
#                     else:
#                         if e >= 0.0:
#                             t = 0.0
#                             sqrDistance = f
#                         else:
#                             t = -e / c
#                             sqrDistance = e * t + f
#                             # of region 2
#             else:
#                 if t < 0.0:
#                     # region6
#                     tmp0 = b + e
#                     tmp1 = a + d
#                     if tmp1 > tmp0:
#                         numer = tmp1 - tmp0
#                         denom = a - 2.0 * b + c
#                         if numer >= denom:
#                             t = 1.0
#                             s = 0
#                             sqrDistance = c + 2.0 * e + f
#                         else:
#                             t = numer / denom
#                             s = 1 - t
#                             sqrDistance = (
#                                     s * (a * s + b * t + 2.0 * d)
#                                     + t * (b * s + c * t + 2.0 * e)
#                                     + f
#                             )
#
#                     else:
#                         t = 0.0
#                         if tmp1 <= 0.0:
#                             s = 1
#                             sqrDistance = a + 2.0 * d + f
#                         else:
#                             if d >= 0.0:
#                                 s = 0.0
#                                 sqrDistance = f
#                             else:
#                                 s = -d / a
#                                 sqrDistance = d * s + f
#                 else:
#                     # region 1
#                     numer = c + e - b - d
#                     if numer <= 0:
#                         s = 0.0
#                         t = 1.0
#                         sqrDistance = c + 2.0 * e + f
#                     else:
#                         denom = a - 2.0 * b + c
#                         if numer >= denom:
#                             s = 1.0
#                             t = 0.0
#                             sqrDistance = a + 2.0 * d + f
#                         else:
#                             s = numer / denom
#                             t = 1 - s
#                             sqrDistance = (
#                                     s * (a * s + b * t + 2.0 * d)
#                                     + t * (b * s + c * t + 2.0 * e)
#                                     + f
#                             )
#
#         # account for numerical round-off error
#         if sqrDistance < 0:
#             sqrDistance = 0
#
#         dist = np.sqrt(sqrDistance)
#
#         PP0 = B + s * E0 + t * E1
#         return dist, PP0
#
#     disp = []
#     vertices = []  # np.array([])
#     #p = arrayTypeCast(p)
#     for v in V:
#         #v = arrayTypeCast(v)
#         vertices.append(v)
#         if minmax == 'max':
#             d = np.linalg.norm(v - p)
#             disp.append(d)
#
#     if minmax == 'max':
#         maxDisp = max(disp)
#         maxPoint = vertices[disp.index(maxDisp)]
#         return maxPoint, maxDisp
#
#     tri = pymesh.triangle()
#     tri.points = vertices  # np.array(vertices)
#     tri.split_boundary = False
#     tri.verbosity = 0
#     tri.run()  # Execute triangle
#     minPointIntersect = []
#     for t in tri.faces:
#         tt = np.array([vertices[t[0]], vertices[t[1]], vertices[t[2]]])
#         d, pd = pointTriangleMinDistance(tt, p)
#         disp.append(d)
#         minPointIntersect.append(pd)
#
#     minDisp = min(disp)
#     minPoint = minPointIntersect[disp.index(minDisp)]
#     return minPoint, minDisp


def cleanSubRefs(refStr):
    """
    flatten parameter sets and extract #d string references
    """
    flattenList = (
        lambda irregularL: [e for i in irregularL for e in flattenList(i)]
        if type(irregularL) is list
        else [irregularL]
    )
    sp = flattenList(refStr)  # flatten sublists
    sp = [i for i in sp if isinstance(i, str)]
    sp = [re.match("#[0-9]+", i) for i in sp]
    sp = [i.string for i in sp if i is not None]
    return sp


def curveEnclosingRectangle(v1, v2, centreP, normV, radius):
    # in order to establish union of partial ellipse/circle, create box encompassing circle/ellipse
    # in the case of ellipse, use eMajorRadius
    # vertex midpoint
    midP = (v2 + v1)/2
    midPv = np.cross(v2 - v1, normV)
    # midpoint - centroid vector, falls apart if coincident with centre point
    #midPv = midP - centreP
    midPv = midPv / np.linalg.norm(midPv)
    farP = centreP + midPv * radius
    orthoPv = np.cross(normV, midPv)
    orthoPv = orthoPv / np.linalg.norm(orthoPv)
    p1 = midP + radius * orthoPv
    p2 = midP - radius * orthoPv
    p3 = farP - radius * orthoPv
    p4 = farP + radius * orthoPv
    return [p1, p2, p3, p4]


def CP2point(STEPobject, pointRefString):
    """
    return np.array of STEP AP21 CARTESIAN_POINT, DIRECTION, VECTOR
    """
    # assert STEPobject[ref2index(pointRefString)].type_name == "CARTESIAN_POINT"
    #R3 = STEPobject[ref2index(pointRefString)].params[1]
    R3 = STEPobject[ref2index(pointRefString)].params[-1]
    return np.array([R3[0], R3[1], R3[2]])


def axis2Placement3D(strRef, STEPobj):
    # process STEP AXIS2_PLACEMENT_3D entity
    assert STEPobj[ref2index(strRef)].type_name == 'AXIS2_PLACEMENT_3D'
    # D = {}

    if '#' in STEPobj[ref2index(strRef)].params[0]:  # check for name string
        # not a robust approach as certain values are optional
        offset = 0
    else:
        offset = 1

    axisPoint = STEPobj[ref2index(strRef)].params[offset]
    assert STEPobj[ref2index(axisPoint)].type_name == 'CARTESIAN_POINT'
    axisPoint = CP2point(STEPobj, axisPoint)
    # D['axisPoint'] = axisPoint

    auxDir = STEP_entities[ref2index(strRef)].params[offset+1]
    assert STEP_entities[ref2index(auxDir)].type_name == 'DIRECTION'
    auxDir = CP2point(STEP_entities, auxDir)
    # D['auxDir'] = auxDir

    refDir = STEP_entities[ref2index(strRef)].params[offset+2]
    assert STEP_entities[ref2index(refDir)].type_name == 'DIRECTION'
    refDir = CP2point(STEP_entities, refDir)
    # D['refDir'] = refDir

    return axisPoint, auxDir, refDir


def axis2Placement3D_2(strRef, STEPobj):
    # process STEP AXIS2_PLACEMENT_3D entity
    # axis: the Direction that defines the second axis of the Axis_placement. (Y or V, not normal)
    # The value of this attribute need not be specified.
    # ref_direction: the direction used to determine the direction of the local X axis. (or U)
    # The value of this attribute need not be specified.
    # If axis or ref_direction is omitted, these directions are taken from the geometric coordinate system
    # If both axis and ref_direction are provided then the vector product of axis and ref_direction shall not be a null vector.

    if STEPobj[ref2index(strRef)].type_name != 'AXIS2_PLACEMENT_3D':
        print("axis2Placement3D_2 assignment failure: " + STEPobj[ref2index(strRef)].type_name)
        return None

    axisPoint = None
    normDir = None
    refDir = None
    subRefList = cleanSubRefs(STEPobj[ref2index(strRef)].params)

    if (STEPobj[ref2index(subRefList[0])].type_name == 'CARTESIAN_POINT'):
        axisPoint = CP2point(STEPobj, subRefList[0])

        if len(subRefList) > 1:
            if (STEPobj[ref2index(subRefList[1])].type_name == 'DIRECTION'):
                normDir = CP2point(STEPobj, subRefList[1])

                # relying on order specified
                # https://www.steptools.com/stds/smrl/data/modules/elemental_geometric_shape/sys/4_info_reqs.htm#elemental_geometric_shape_arm.axis_placement
                if len(subRefList) > 2:
                    if (STEPobj[ref2index(subRefList[2])].type_name == 'DIRECTION'):
                        refDir = CP2point(STEPobj, subRefList[2])

    return axisPoint, normDir, refDir


# 189 = ADVANCED_FACE( '', ( #409, #410 ), #411, .T. )
# 409 = FACE_OUTER_BOUND( '', #1293, .T. );
# 1293 = EDGE_LOOP( '', ( #2202 ) );
# 2202 = ORIENTED_EDGE( '', *, *, #2631, .F. );
# 2631 = EDGE_CURVE( '', #2959, #2959, #2960, .F. );

# The geometry used in the definition of the face shall be restricted.
# The face geometry shall be an elementary surface, swept surface, or b-spline surface;
# The geometry of all bounding edges of the face shall be fully defined as edge curves;
# The types of curve used to define the geometry of edges shall be restricted to lines,
# conics, polylines, surface curves, or b spline curves;
# All vertices used in the face definition shall be of type vertex point with geometry defined by a cartesian point;
# The use of oriented paths in the definition of the edge loops of the advanced face is prohibited;
# If the face geometry is a swept surface then the swept curve used in the definition shall be a line,
# conic, polyline, or a b-spline curve;
# For any vertex loop used to bound the face, the loop vertex shall be a
# vertex whose geometry shall be defined by a cartesian point;

# The face bounds shall be defined by either edge loops or vertex loops;

# If a surface curve is used as part of a face bound then the associated geometry attribute shall
# reference pcurves not surfaces;
# If a polyline is used either to define a swept surface or as part of a face bound,
# it shall contain at least three points;
# Any instance of advanced face which has the geometry of a complete spherical surface shall be bounded by a vertex loop
# located at the point where the z axis of the placement coordinate system leaves the surface of the sphere,
# (that is, at the North Pole of the sphere).


def splineToPolyline(c, curveSampleFactor=2):
    numSamples = c.ctrlpts_size * c.degree * curveSampleFactor
    span = (c.knotvector[-1] - c.knotvector[0]) / (numSamples - 1)
    kvs = np.array([c.knotvector[0] + (span * i) for i in range(0, numSamples)])
    pts = c.evaluate_list(kvs)
    return (pts, kvs)


def insideOutsideSurfaceTest(localCentroid, mPoint, AFSdata):
    # project ray from localCentroid toward mPoint in order to determine whether this ray intersects the surface defined in AFS
    # the angle of edge vertices of an intercepted face are calculated with respect to this point and summed
    # return true if point within surface edges ("Angle Sum Algorithm")

    def vertexAngle(mPoint, v1, v2):
        # project vertex points to a plane defined by mPoint and sNorm
        m_v1 = v1 - mPoint
        p_v1 = v1 - np.dot(m_v1, sNorm) * sNorm

        m_v2 = v2 - mPoint
        p_v2 = v2 - np.dot(m_v2, sNorm) * sNorm

        mppv1 = p_v1 - mPoint
        mppv2 = p_v2 - mPoint

        sign = np.sign(np.cross(mppv1, mppv2).dot(sNorm))

        v1v2cos = np.dot(mppv1, mppv2) / (np.linalg.norm(mppv1) * np.linalg.norm(mppv2)) # cosine

        if np.abs(v1v2cos) < eps_STEP_AP21:
            print("insideOutsideSurfaceTest(): edge warning")

        # zero value implies mPoint lies on edge
        return(sign * np.arccos(np.clip(v1v2cos, -1, 1)))

    sNormDen = np.linalg.norm(mPoint - localCentroid)
    if sNormDen > eps_STEP_AP21:
        sNorm = (mPoint - localCentroid) / sNormDen
    else:
        # point is at localCentroid, find normal from provided surface parameters
        print("insideOutsideSurfaceTest(): median localCentroid at surface, surface normal substituted")
        # add offset to everything and try again?
        # mirror mPoint around surface normal? just use surface normal instead? -> requires earlier calc of axis2Placement3D_2
        #_1=1
        sNorm = AFSdata['ParsedSurface']['normDir']

    edgeTotalAngle = 0
    vertexLoopCount = 0
    for edge in AFSdata['outerBoundEdgeLoop']:
        v1 = edge['vertex1']
        v2 = edge['vertex2']

        # #if not np.isclose(v1, v2).all():
        # if edge['vertex1ref'] == edge['vertex2ref']: # loop, discard
        #     if edge['typeName'] == 'CIRCLE' or edge['typeName'] == 'B_SPLINE_CURVE_WITH_KNOTS' or edge['typeName'] == 'ELLIPSE':
        #         vertexLoopCount +=1 # should be discarded if point is not within loop?

        # catch spline and convert to polyline vertices
        if edge['typeName'] == 'B_SPLINE_CURVE_WITH_KNOTS':
            if edge['vertex1ref'] == edge['vertex2ref']:  # loop, discard
                vertexLoopCount += 1  # should be discarded if point is not within loop?
            else:
                splinePolyline, splinePolylineU = splineToPolyline(edge['BsplineKnotCurve'], curveSampleFactor=1)
                # find closest point on spline and ensure that this is not mPoint

                # nearestSplinePointU = splineCurveMinMaxPointDisp(edge['BsplineKnotCurve'], mPoint, maxSearch=False)

                # if mPoint[0]==0: # and mPoint[1]== -0.30397087 and mPoint[2]==-16.61759168:
                #     _1=1
                nearestSplinePointU = BsplineCurveExtremaDisp(edge['BsplineKnotCurve'], mPoint, maxSearch=False, uv_xyz=True)
                nearestSplinePoint = np.array(edge['BsplineKnotCurve'].evaluate_list(nearestSplinePointU))

                if np.linalg.norm(mPoint - nearestSplinePoint) < eps_STEP_AP21:
                    print('insideOutsideSurfaceTest(): spline edge warning - ignore polyline warnings')
                if not any([np.isclose(nearestSplinePointU, splU) for splU in splinePolylineU]):
                    # has to be ordered correctly
                    splinePolylineU = np.insert(splinePolylineU,splinePolylineU.searchsorted(nearestSplinePointU),nearestSplinePointU)
                    iu = np.argmin(np.abs(splinePolylineU - nearestSplinePointU))
                    splinePolyline = [np.array(spl) for spl in splinePolyline]
                    splinePolyline = np.insert(splinePolyline, iu, nearestSplinePoint, axis=0)
                for i in range(len(splinePolyline)-1):
                    edgeTotalAngle += vertexAngle(mPoint, splinePolyline[i], splinePolyline[i+1])
                #_1=1

        # account for circle or ellipse arcs, find nearest point on arc to mPoint,
        # add a point there, effectively converting arc into two segments
        elif (edge['typeName'] == 'CIRCLE'):
            if edge['vertex1ref'] == edge['vertex2ref']:  # loop, discard
                vertexLoopCount += 1  # should be discarded if point is not within loop?
            else:
                nearArcPoint, _ = pointCircleMinMaxDisp(mPoint,
                                                       edge['axisPoint'],
                                                       edge['normDir'],
                                                       edge['radius'])
                if pointInArc(nearArcPoint,
                              v1, v2,
                              edge['refDir'],
                              edge['auxDir'],
                              edge['normDir'],
                              edge['axisPoint'],
                              rotSym=False):
                    edgeTotalAngle += vertexAngle(mPoint, v1, nearArcPoint)
                    edgeTotalAngle += vertexAngle(mPoint, nearArcPoint, v2)
                    print("untested circle arc division")

        elif edge['typeName'] == 'ELLIPSE':
            if edge['vertex1ref'] == edge['vertex2ref']:  # loop, discard
                vertexLoopCount += 1  # should be discarded if point is not within loop?
            else:
                nearArcPoint, _ = pointEllipseMinMaxDisp(mPoint,
                                       edge['axisPoint'],
                                       edge['refDir'],
                                       edge['auxDir'],
                                       edge['normDir'],
                                       edge['majorRadius'],
                                       edge['minorRadius'],
                                       interior=False)
                if pointInArc(nearArcPoint,
                              v1, v2,
                              edge['refDir'],
                              edge['auxDir'],
                              edge['normDir'],
                              edge['axisPoint'],
                              rotSym=False):
                    edgeTotalAngle += vertexAngle(mPoint, v1, nearArcPoint)
                    edgeTotalAngle += vertexAngle(mPoint, nearArcPoint, v2)
                    print("untested ellipse arc division")

        else:
            # project vertex points to a plane defined by mPoint and sNorm
            edgeTotalAngle += vertexAngle(mPoint, v1, v2)


    # surfaces that curve > 90deg can give false negative, assume inside/outside test is to detect nearest surface

    # exception for cylinder, conic, sphere
    if (AFS['SurfaceTypeName'] in ['CYLINDRICAL_SURFACE',]) or (AFS['SurfaceTypeName'] in ['CONICAL_SURFACE',]):
        # edges angle sum to zero from 2x loops and seam edge ---------------------needs testing
        if (np.abs(edgeTotalAngle % (2 * np.pi)) < eps_STEP_AP21) and (vertexLoopCount >= 1): # (len(AFSdata['outerBoundEdgeLoop']) < 4):
            # only need to determine whether point lies within section of cone axis within cone as circle or ellipse loops at ends signify
            #   fully enclosed surface
            # vertex angles that sum to zero will still indicate a crossing, for either CW, or ACW direction configuration

            # if method is uniquely used for closest surface point on a plane encompassing projection and cylinder axis
            # can then ignore far-side intersection

            # ?? find nearest points on circles/ellipses/bsplines at each curved edge, then test if both points are to one side??
            curvedEdgeNearestPoint = []
            normDir = np.array(AFSdata['ParsedSurface']['normDir'])
            for edge in AFSdata['outerBoundEdgeLoop']:
                if edge['typeName'] == 'CIRCLE':
                    nearArcPoint, _ = pointCircleMinMaxDisp(mPoint,
                                                            edge['axisPoint'],
                                                            edge['normDir'],
                                                            edge['radius'])
                    curvedEdgeNearestPoint.append(np.array(nearArcPoint))

                elif edge['typeName'] == 'ELLIPSE':
                    nearArcPoint, _ = pointEllipseMinMaxDisp(mPoint,
                                                             edge['axisPoint'],
                                                             edge['refDir'],
                                                             edge['auxDir'],
                                                             edge['normDir'],
                                                             edge['majorRadius'],
                                                             edge['minorRadius'])
                    curvedEdgeNearestPoint.append(np.array(nearArcPoint))

                elif edge['typeName'] == 'B_SPLINE_CURVE_WITH_KNOTS':

                    # should go to a plane-curve intersection function

                    # define plane through axisPoint, mPoint, normDir
                    axisPoint = np.array(AFSdata['ParsedSurface']['axisPoint'])
                    #normDir = np.array(AFSdata['ParsedSurface']['normDir'])
                    planeNorm = np.cross((mPoint - axisPoint), normDir)
                    orthoPlane = np.cross(planeNorm, normDir)
                    mSign = np.dot(axisPoint - mPoint, orthoPlane) > 0
                    sPolyline, sPolylineU = splineToPolyline(edge['BsplineKnotCurve'], curveSampleFactor=1)
                    nearArcPointSet = []
                    U = []

                    for i in range(0, len(sPolyline)-1):
                        nearArcPoint = intersectSegmentPlane(np.array(sPolyline[i]),
                                                             np.array(sPolyline[i+1]),
                                                             planeNorm,
                                                             mPoint)
                        if nearArcPoint is not None:
                            nearArcPointSet.append(nearArcPoint)
                            U.append(i)

                    # determine intersection on mPoint side of plane
                    # test whether vector from mPoint to intersection is parallel with normDir
                    if len(nearArcPointSet) == 0:
                        break
                    elif len(nearArcPointSet) == 1:
                        nearArcPoint = nearArcPointSet[0]
                        iu = U[0]
                    else:
                        for i_naps, naps in enumerate(nearArcPointSet):
                            napsSign = np.dot(axisPoint - naps, orthoPlane) > 0
                            if napsSign == mSign:
                            #if np.isclose(np.cross(mPoint - naps, normDir), np.array([0., 0., 0.]), eps_STEP_AP21).all():
                                nearArcPoint = naps
                                iu = U[i_naps]

                    # binary search over u-interval
                    if iu+1 > len(sPolylineU):
                        _1=1
                    hiU = sPolylineU[iu+1]
                    hiP = sPolyline[iu+1]
                    loU = sPolylineU[iu]
                    loP = sPolyline[iu]

                    while (hiU-loU) > eps_STEP_AP21: #1E-10: #==================================================
                        midU = loU + (hiU - loU)/2
                        midP = np.array(edge['BsplineKnotCurve'].evaluate_single(midU))
                        nearArcPoint = intersectSegmentPlane(loP, midP, planeNorm, mPoint)
                        if nearArcPoint is not None:
                            hiP = midP
                            hiU = midU
                        else:
                            nearArcPoint = intersectSegmentPlane(midP, hiP, planeNorm, mPoint)
                            if nearArcPoint is not None:
                                loP=midP
                                loU=midU
                            else:
                                print("fail")

                    curvedEdgeNearestPoint.append(np.array(nearArcPoint))

            if len(curvedEdgeNearestPoint) > 2:
                print("insideOutsideSurfaceTest cylinder/conical fail")
                print(curvedEdgeNearestPoint)
                print(mPoint)
            if len(curvedEdgeNearestPoint) < 2:
                #print("insideOutsideSurfaceTest cylinder/conical fail")
                return False
            # project intersections to plane where axis is normal (normDir), through mPoint
            # if all points have same sign, all are on same side
            if not all([np.dot(mPoint - cenp, normDir) > 0 for cenp in curvedEdgeNearestPoint]):
                return True
            else:
                return False



    # if AFS['SurfaceTypeName'] in ['CONICAL_SURFACE',]:
    #     # edges angle sum to zero from 2x loops and seam edge ---------------------needs testing
    #     if (np.abs(edgeTotalAngle) < eps_STEP_AP21) and (vertexLoopCount == 2): # (len(AFSdata['outerBoundEdgeLoop']) < 4):
    #         # only need to determine whether point lies within section of cone axis within cone
    #         return True
    #     else:
    #         return False

    if AFS['SurfaceTypeName'] in ['PLANE', ]: # full circle
        if (np.abs(edgeTotalAngle) < eps_STEP_AP21) and (len(AFSdata['outerBoundEdgeLoop']) < 2):
            return True
        elif ((np.abs(edgeTotalAngle) % (2*np.pi)) < eps_STEP_AP21) and (len(AFSdata['outerBoundEdgeLoop']) > 1):
            return True
        else:
            return False

    # may have to take VOR into account depending on orientation of surface,
    # e.g. concave or convex spherical surface
    # assume that minima point is always closest to centroid?
    #if not edge['VOR']:
    if not AFSdata['SurfaceNormalOutwards']:
        if (edgeTotalAngle % (2 * np.pi)) < eps_STEP_AP21 and (edgeTotalAngle > 0):
            return True
        else:
            return False
    else:
        if (edgeTotalAngle % (2 * np.pi)) < eps_STEP_AP21 and (edgeTotalAngle < 0):
            return True
        else:
            return False


def segmentParse(localCentroid, ParsedEdge, STEP_entities_):
    # ENTITY Line
    #   SUBTYPE OF (Curve);
    #   point : Cartesian_point;
    #   line_direction : Direction;

    # if ParsedEdge.get('subEdgeType') is not None:
    #     edgeType_ = ParsedEdge['subEdgeType']
    # else:
    edgeType_ = ParsedEdge['edgeType']

    cleanEdgeType = cleanSubRefs(edgeType_)
    for cet in cleanEdgeType:
        if (STEP_entities_[ref2index(cet)].type_name == 'CARTESIAN_POINT'):
            linePoint = CP2point(STEP_entities_, cet)
            ParsedEdge['linePoint'] = linePoint

        if (STEP_entities_[ref2index(cet)].type_name == 'VECTOR'):
            lineVectorDisp = STEP_entities_[ref2index(cet)].params[-1]
            ParsedEdge['lineVectorDisp'] = lineVectorDisp
            lineVectorDir = STEP_entities_[ref2index(cet)].params[-2]
            if (STEP_entities_[ref2index(lineVectorDir)].type_name == 'DIRECTION'):
                lineVectorDir = CP2point(STEP_entities_, lineVectorDir)
                ParsedEdge['lineVectorDir'] = lineVectorDir

    minPoint, minPointCentroidDisp, v1ext, v2ext = pointSegmentMinDisp(localCentroid, ParsedEdge['vertex1'], ParsedEdge['vertex2'])
    # todo, if there are multiple minima, return list? store minpoint as list?

    if not v1ext and not v2ext:
        ParsedEdge['pointFeature']['xyz'].insert(1, minPoint)
        ParsedEdge['pointFeature']['centroidDisp'].insert(1, minPointCentroidDisp)
        u = np.linalg.norm(minPoint - ParsedEdge['vertex1'])/np.linalg.norm(ParsedEdge['vertex2'] - ParsedEdge['vertex1'])
        ParsedEdge['pointFeature']['u'].insert(1, u)


def circleParse(localCentroid, ParsedEdge, STEP_entities_):

    # if ParsedEdge.get('subEdgeType') is not None:
    #     edgeType_ = ParsedEdge['subEdgeType']
    # else:
    edgeType_ = ParsedEdge['edgeType']

    radius = edgeType_[-1]
    ParsedEdge['radius'] = radius

    axisPoint, normDir, refDir = axis2Placement3D_2(edgeType_[-2], STEP_entities_)
    ParsedEdge['axisPoint'] = axisPoint
    ParsedEdge['normDir'] = normDir
    ParsedEdge['refDir'] = refDir
    # axisDir, refDir guaranteed unit normals?
    auxDir = np.cross(normDir, refDir)
    auxDir = auxDir / np.linalg.norm(auxDir)
    ParsedEdge['auxDir'] = auxDir

    # test if cylinder is rotationally symmetrical, if localCentroid is close to axisDir through axisPoint
    #rotSymDisp = np.linalg.norm((localCentroid - axisPoint) - np.dot((localCentroid - axisPoint), normDir) * normDir)

    # why doesn't the below work?-------------------------------------------------------------------------
    #rotSymDisp = np.linalg.norm(localCentroid - pointProjectAxis(localCentroid, axisPoint, normDir))

    # # project localCentroid to disc plane
    # # distance d of point p to the plane is dot product of plane unit normal, normAxis
    # # with the difference vector from the point in the plane P and S: d = (S − P) · u
    # # distance is signed: positive when S is on the side of the plane where u faces and
    # # negative on the other side. zero, S is in the plane

    Cdisp = np.dot((localCentroid - axisPoint), normDir)
    Cproj = localCentroid - np.dot(Cdisp, normDir)  # point projected to plane along orthogonal
    rotSymDisp = np.linalg.norm(Cproj - axisPoint)
    # print("rotSymDisp: " + str(rotSymDisp))

    # min/max point on arc wrt centroid
    minPoint, maxPoint = pointCircleMinMaxDisp(
        localCentroid,
        axisPoint,
        normDir,
        radius,
        interior=False
    )

    v1 = ParsedEdge['vertex1']
    v2 = ParsedEdge['vertex2'] # v1 always equals v2 for full circle
    #ParsedEdge['minPoint'] = minPoint

    extrema = []
    if maxPoint is not None: extrema.append(np.array(maxPoint))
    if minPoint is not None: extrema.append(np.array(minPoint))
    #extrema = [maxPoint, minPoint]
    pointsInArc, pointOrderIndex, uArc = pointOrderInArc(extrema, v1, v2, refDir, auxDir, normDir, axisPoint)
    pointsInArc = [pointsInArc[poi] for poi in pointOrderIndex]
    extrema = [extrema[poi] for poi in pointOrderIndex]
    uArc = [uArc[poi] for poi in pointOrderIndex]
    # drop points not in arc
    extrema = [i for (i, v) in zip(extrema, pointsInArc) if v]

    # test whether extrema values already coincide with vertex values,
    for ex_i, ex in enumerate(extrema):
        if not np.isclose(v1, ex).all() and not np.isclose(v2, ex).all():
            if not any([np.isclose(uArc[ex_i], uu) for uu in ParsedEdge['pointFeature']['u']]):
                ParsedEdge['pointFeature']['xyz'].insert(-1, ex)
                ParsedEdge['pointFeature']['centroidDisp'].insert(-1, np.linalg.norm(ex - localCentroid))
                ParsedEdge['pointFeature']['u'].insert(-1, uArc[ex_i]) # check u proximity?

    # centroid orthogonal to centrePoint
    if rotSymDisp < eps_STEP_AP21:
        # case of minPoint equidistant from circle centre - edge case
        # assign rotSymCentre feature point at centre

        ParsedEdge['rotSymFeature'] = dict()
        ParsedEdge['rotSymFeature']['rotSymCentre'] = axisPoint
        ParsedEdge['rotSymFeature']['rotSymRadius'] = radius
        ParsedEdge['rotSymFeature']['radiusCentroidDisp'] = np.sqrt(radius ** 2 + np.linalg.norm(axisPoint - localCentroid) ** 2)


def ellipseParse(localCentroid, ParsedEdge, STEP_entities_):
    # first_semi_axis: half the length of the first diameter of the Ellipse.
    # second_semi_axis: half the length of the second diameter of the Ellipse.

    # if ParsedEdge.get('subEdgeType') is not None:
    #     edgeType_ = ParsedEdge['subEdgeType']
    # else:
    edgeType_ = ParsedEdge['edgeType']

    cleanEdgeType = cleanSubRefs(edgeType_)
    majorRadius = cleanEdgeType[-2]
    ParsedEdge['majorRadius'] = majorRadius

    minorRadius = cleanEdgeType[-1]
    ParsedEdge['minorRadius'] = minorRadius

    axisPoint, normDir, refDir = axis2Placement3D_2(edgeType_[-2], STEP_entities_)
    ParsedEdge['axisPoint'] = axisPoint
    ParsedEdge['normDir'] = normDir
    ParsedEdge['refDir'] = refDir
    # axisDir, refDir guaranteed unit normals?
    auxDir = np.cross(normDir, refDir)
    auxDir = auxDir / np.linalg.norm(auxDir)
    ParsedEdge['auxDir'] = auxDir

    minPoint, maxPoint = pointEllipseMinMaxDisp(
        localCentroid,
        axisPoint,
        refDir,
        auxDir,
        normDir,
        majorRadius,
        minorRadius,
        interior=False
    )

    # test if minPoint, maxPoint is on segment between vertex1 & vertex2
    v1 = ParsedEdge['vertex1']
    v2 = ParsedEdge['vertex2']

    extrema = [maxPoint, minPoint]
    pointsInArc, pointOrderIndex, uArc = pointOrderInArc(extrema,
                                                         v1,
                                                         v2,
                                                         refDir,
                                                         auxDir,
                                                         normDir,
                                                         axisPoint)
    pointsInArc = [pointsInArc[poi] for poi in pointOrderIndex]
    extrema = [extrema[poi] for poi in pointOrderIndex]
    uArc = [uArc[poi] for poi in pointOrderIndex]
    # drop points not in arc
    extrema = [i for (i, v) in zip(extrema, pointsInArc) if v]

    # test whether extrema values already coincide with vertex values,
    for ex_i, ex in enumerate(extrema):
        if not np.isclose(v1, ex).all() and not np.isclose(v2, ex).all():
            ParsedEdge['pointFeature']['xyz'].insert(-1, ex)
            ParsedEdge['pointFeature']['centroidDisp'].insert(-1, np.linalg.norm(ex - localCentroid))
            ParsedEdge['pointFeature']['u'] .insert(-1, uArc[ex_i]) # check u proximity?



    # if pointInArc(minPoint, v1, v2, refDir, auxDir, normDir, axisPoint, rotSym=False):
    #     ParsedEdge['minPoint'] = minPoint
    #     ParsedEdge['vertexExtremaMin'] = False
    # else:  # one of the vertices is a minima
    #     ParsedEdge['vertexExtremaMin'] = True
    #     if ParsedEdge['vertex1centroidDisp'] < ParsedEdge['vertex2centroidDisp']:
    #         ParsedEdge['minPoint'] = ParsedEdge['vertex1']
    #         ParsedEdge['minPointCentroidDisp'] = ParsedEdge['vertex1centroidDisp']
    #     else:
    #         ParsedEdge['minPoint'] = ParsedEdge['vertex2']
    #         ParsedEdge['minPointCentroidDisp'] = ParsedEdge['vertex2centroidDisp']
    #
    # if pointInArc(maxPoint, v1, v2, refDir, auxDir, normDir, axisPoint, rotSym=True):
    #     ParsedEdge['maxPoint'] = maxPoint
    #     ParsedEdge['vertexExtremaMax'] = False
    # else:  # one of the vertices is a maxima
    #     ParsedEdge['vertexExtremaMax'] = True
    #     if ParsedEdge['vertex1centroidDisp'] > ParsedEdge['vertex2centroidDisp']:
    #         ParsedEdge['maxPoint'] = ParsedEdge['vertex1']
    #         ParsedEdge['maxPointCentroidDisp'] = ParsedEdge['vertex1centroidDisp']
    #     else:
    #         ParsedEdge['maxPoint'] = ParsedEdge['vertex2']
    #         ParsedEdge['maxPointCentroidDisp'] = ParsedEdge['vertex2centroidDisp']

    # if ParsedEdge['vertex1ref'] == ParsedEdge['vertex2ref']:  # full ellipse? untested
    #     if np.linalg.norm(minPoint - v1) < eps_STEP_AP21:
    #         ParsedEdge['vertex1extremaMin'] = True
    #         ParsedEdge['vertex2extremaMin'] = True
    #         ParsedEdge['vertex1extremaMax'] = False
    #         ParsedEdge['vertex2extremaMax'] = False
    #     if np.linalg.norm(maxPoint - v1) < eps_STEP_AP21:
    #         ParsedEdge['vertex1extremaMin'] = False
    #         ParsedEdge['vertex2extremaMin'] = False
    #         ParsedEdge['vertex1extremaMax'] = True
    #         ParsedEdge['vertex2extremaMax'] = True
    #
    # else:  # partial ellipse arc
    #     if pointInArc(minPoint, v1, v2, refDir, auxDir, normDir, axisPoint, rotSym=True):
    #         ParsedEdge['minPoint'] = minPoint
    #         ParsedEdge['minPointCentroidDisp'] = np.linalg.norm(minPoint - localCentroid)
    #         if np.linalg.norm(minPoint - v1) < eps_STEP_AP21:
    #             ParsedEdge['vertex1extremaMin'] = True
    #             ParsedEdge['vertex2extremaMin'] = False
    #         if np.linalg.norm(minPoint - v2) < eps_STEP_AP21:
    #             ParsedEdge['vertex1extremaMin'] = False
    #             ParsedEdge['vertex2extremaMin'] = True
    #
    #     if pointInArc(maxPoint, v1, v2, refDir, auxDir, normDir, axisPoint, rotSym=True):
    #         ParsedEdge['maxPoint'] = maxPoint
    #         ParsedEdge['maxPointCentroidDisp'] = np.linalg.norm(maxPoint - localCentroid)
    #         if np.linalg.norm(maxPoint - v1) < eps_STEP_AP21:
    #             ParsedEdge['vertex1extremaMax'] = True
    #             ParsedEdge['vertex2extremaMax'] = False
    #         if np.linalg.norm(maxPoint - v2) < eps_STEP_AP21:
    #             ParsedEdge['vertex1extremaMax'] = False
    #             ParsedEdge['vertex2extremaMax'] = True


        # if pointInArc(minPoint, v1, v2, refDir, auxDir, normDir, axisPoint, rotSym=True):
        #     ParsedEdge['minPoint'] = minPoint
        #     ParsedEdge['minPointCentroidDisp'] = np.linalg.norm(minPoint - centroid)
        #     ParsedEdge['vertex1extremaMin'] = False
        #     ParsedEdge['vertex2extremaMin'] = False
        # else:  # one of the vertices is a minima (only valid for straight line)
        #     if ParsedEdge['vertex1centroidDisp'] < ParsedEdge['vertex2centroidDisp']:
        #         ParsedEdge['minPoint'] = ParsedEdge['vertex1']
        #         ParsedEdge['minPointCentroidDisp'] = ParsedEdge['vertex1centroidDisp']
        #         ParsedEdge['vertex1extremaMin'] = True
        #         ParsedEdge['vertex2extremaMin'] = False
        #     else:
        #         ParsedEdge['minPoint'] = ParsedEdge['vertex2']
        #         ParsedEdge['minPointCentroidDisp'] = ParsedEdge['vertex2centroidDisp']
        #         ParsedEdge['vertex1extremaMin'] = False
        #         ParsedEdge['vertex2extremaMin'] = True
        #
        # if pointInArc(maxPoint, v1, v2, refDir, auxDir, normDir, axisPoint, rotSym=True):
        #     ParsedEdge['maxPoint'] = maxPoint
        #     ParsedEdge['maxPointCentroidDisp'] = np.linalg.norm(maxPoint - centroid)
        #     ParsedEdge['vertex1extremaMax'] = True
        #     ParsedEdge['vertex2extremaMax'] = True
        # else:  # one of the vertices is a maxima (only valid for straight line)
        #     if ParsedEdge['vertex1centroidDisp'] > ParsedEdge['vertex2centroidDisp']:
        #         ParsedEdge['maxPoint'] = ParsedEdge['vertex1']
        #         ParsedEdge['maxPointCentroidDisp'] = ParsedEdge['vertex1centroidDisp']
        #         ParsedEdge['vertex1extremaMax'] = True
        #         ParsedEdge['vertex2extremaMax'] = False
        #     else:
        #         ParsedEdge['maxPoint'] = ParsedEdge['vertex2']
        #         ParsedEdge['maxPointCentroidDisp'] = ParsedEdge['vertex2centroidDisp']
        #         ParsedEdge['vertex1extremaMax'] = False
        #         ParsedEdge['vertex2extremaMax'] = True


def BsplineCurveWithKnotsParse(localCentroid, ParsedEdge, STEP_entities_):
    # ENTITY B_Spline_Curve
    # 	SUPERTYPE OF ((ONEOF (Uniform_Curve, B_Spline_Curve_With_Knots, Quasi_Uniform_Curve, Bezier_Curve) ANDOR Rational_B_Spline_Curve))
    # 	SUBTYPE OF (Bounded_Curve);
    # 	degree : INTEGER;
    # 	control_points_list : LIST [2:?] OF Cartesian_Point;
    # 	curve_form : B_Spline_Curve_Form;
    # 	closed_curve : LOGICAL;
    # 	self_intersect : LOGICAL;
    # ENTITY B_Spline_Curve_With_Knots
    # 	SUBTYPE OF (B_Spline_Curve);
    # 	knot_multiplicities : LIST [2:?] OF INTEGER;
    # 	knots : LIST [2:?] OF Parameter_Value;
    # 	knot_spec : Knot_Type;

    # if ParsedEdge.get('subEdgeType') is not None:
    #     edgeType_ = ParsedEdge['subEdgeType']
    # else:
    edgeType = ParsedEdge['edgeType']

    if type(edgeType[0]) == str:  # check for name string
        offset = 1
    else:
        offset = 0
    curveDegree = edgeType[offset]
    controlPointsRefs = edgeType[offset + 1]
    curveForm = edgeType[offset + 2]
    closedCurve = 'T' in edgeType[offset + 3]
    selfIntersect = 'T' in edgeType[offset + 4]
    knotMultiplicities = edgeType[offset + 5]
    knots = edgeType[offset + 6]
    knotSpec = edgeType[offset + 7]

    ParsedEdge['curveDegree'] = curveDegree
    ParsedEdge['controlPointsRefs'] = controlPointsRefs
    ParsedEdge['curveForm'] = curveForm
    ParsedEdge['closedCurve'] = closedCurve
    ParsedEdge['selfIntersect'] = selfIntersect
    ParsedEdge['knotMultiplicities'] = knotMultiplicities
    ParsedEdge['knots'] = knots
    ParsedEdge['knotSpec'] = knotSpec

    # extract control points
    controlPoints = []
    for cpl in controlPointsRefs:
        if (STEP_entities_[ref2index(cpl)].type_name == 'CARTESIAN_POINT'):
            controlPoint = CP2point(STEP_entities_, cleanSubRefs(cpl)[0])
            controlPoints.append(controlPoint)
    ParsedEdge['controlPoints'] = controlPoints

    knotvector = []
    for kml in range(len(knotMultiplicities)):
        for i in range(knotMultiplicities[kml]):
            knotvector.append(knots[kml])
    ParsedEdge['STEPknotvector'] = knotvector

    if (len(knotvector) - len(controlPoints) - curveDegree) != 1:
        print("maldefined B-spline!")

    #  rule for closed periodic curves is :
    #  number of input control points + curve degree = the total number of control points required.
    # (Reflect.field(Math,"fabs")(((vec[i] if i >= 0 and i < len(vec) else None) - rep)) > verb_core_Constants.EPSILON)
    # NURBS curve is rational with non-unity weights

    # Creates a rational B_spline curve on the basis <Knots, Multiplicities> of degree <Degree>.
    # Raises ConstructionError subject to the following conditions 0 < Degree <= MaxDegree.
    # Weights.Length() == Poles.Length()
    # Knots.Length() == Mults.Length() >= 2
    # Knots(i) < Knots(i+1) (Knots are increasing)
    # 1 <= Mults(i) <= Degree
    #
    # On a non periodic curve the first and last multiplicities may be Degree+1
    # (this is even recommended if you want the curve to start and finish on the first and last pole).
    # On a periodic curve the first and the last multicities must be the same.
    # on non-periodic curves
    # Poles.Length() == Sum(Mults(i)) - Degree - 1 >= 2
    #
    # on periodic curves
    # Poles.Length() == Sum(Mults(i)) except the first or last

    # The knot vector must be non-decreasing and of length (degree + 1) * 2 or greater
    # [ (degree + 1 copies of the first knot), internal non-decreasing knots, (degree + 1 copies of the last knot) ]

    # BsplineKnotCurve = BSpline.Curve(normalize_kv=True)
    # BsplineKnotCurve.degree = curveDegree
    # BsplineKnotCurve.ctrlpts = [list(c) for c in controlPoints]
    # BsplineKnotCurve.knotvector = _knotvector

    BsplineKnotCurve = NURBS.Curve()
    BsplineKnotCurve.degree = curveDegree
    BsplineKnotCurve.set_ctrlpts(compatibility.combine_ctrlpts_weights(controlPoints, weights=None))

    # Auto-generate knot vector
    BsplineKnotCurve.knotvector = utilities.generate_knot_vector(BsplineKnotCurve.degree, len(BsplineKnotCurve.ctrlpts))
    #BsplineKnotCurve.evaluate()

    ParsedEdge['BsplineKnotCurve'] = BsplineKnotCurve

    # OpenCascade
    # Creates a rational B_spline curve on the basis <Knots, Multiplicities> of degree <Degree>.
    # Raises ConstructionError subject to the following conditions:
    # 0 < Degree <= MaxDegree.
    # Weights.Length() == Poles.Length()
    # Knots.Length() == Mults.Length() >= 2
    # Knots(i) < Knots(i+1) (Knots are increasing)
    # 1 <= Mults(i) <= Degree
    #
    # On a non periodic curve the first and last multiplicities may be Degree+1
    # (this is even recommanded if you want the curve to start and finish on the first and last pole).
    # On a periodic curve the first and the last multicities must be the same.
    # on non-periodic curves
    # Poles.Length() == Sum(Mults(i)) - Degree - 1 >= 2
    # on periodic curves
    # Poles.Length() == Sum(Mults(i)) except the first or last

    # K, the knots sequence of this BSpline curve.
    # In this sequence, knots with a multiplicity greater than 1 are repeated.
    # In the case of a non-periodic curve the length of the sequence must be equal to the sum of the NbKnots multiplicities of the knots of the curve (where NbKnots is the number of knots of this BSpline curve). This sum is also equal to : NbPoles + Degree + 1 where NbPoles is the number of poles and Degree the degree of this BSpline curve. In the case of a periodic curve, if there are k periodic knots, the period is Knot(k+1) - Knot(1). The initial sequence is built by writing knots 1 to k+1, which are repeated according to their corresponding multiplicities. If Degree is the degree of the curve, the degree of continuity of the curve at the knot of index 1 (or k+1) is equal to c = Degree + 1 - Mult(1). c knots are then inserted at the beginning and end of the initial sequence:
    #
    # the c values of knots preceding the first item Knot(k+1) in the initial sequence are inserted at the beginning; the period is subtracted from these c values;
    # the c values of knots following the last item Knot(1) in the initial sequence are inserted at the end; the period is added to these c values. The length of the sequence must therefore be equal to: NbPoles + 2*Degree - Mult(1) + 2. Example For a non-periodic BSpline curve of degree 2 where:
    # the array of knots is: { k1 k2 k3 k4 },
    # with associated multiplicities: { 3 1 2 3 }, the knot sequence is: K = { k1 k1 k1 k2 k3 k3 k4 k4 k4 } For a periodic BSpline curve of degree 4 , which is "C1" continuous at the first knot, and where :
    # the periodic knots are: { k1 k2 k3 (k4) } (3 periodic knots: the points of parameter k1 and k4 are identical, the period is p = k4 - k1),
    # with associated multiplicities: { 3 1 2 (3) }, the degree of continuity at knots k1 and k4 is: Degree + 1 - Mult(i) = 2. 2 supplementary knots are added at the beginning and end of the sequence:
    # at the beginning: the 2 knots preceding k4 minus the period; in this example, this is k3 - p both times;
    # at the end: the 2 knots following k1 plus the period; in this example, this is k2 + p and k3 + p. The knot sequence is therefore: K = { k3-p k3-p k1 k1 k1 k2 k3 k3 k4 k4 k4 k2+p k3+p } Exceptions Standard_DimensionError if the array K is not of the appropriate length.Returns the knots sequence.

    # for each spline you have to satisfy the equation: m = n + p + 1, where
    # m + 1 is the number of the knots (each knot counted separately, no multiplicity)
    # n + 1 is the number of control points
    # p is the curve degree
    #
    # OCC documentation for Geom_BSplieCurve:
    # Knot(i+k) = Knot(i) + period
    # Pole(i+p) = Pole(i)
    # exclude the repeating control point from description of the spline and the 'redundant' knots.

    # Going from B-Splines to NURBS means the addition of weights and non-uniform knots.
    # Weights are straightforward since they correspond directly to points.
    # As for knots, an unclosed NURBS spline requires n + degree + 1 knots, where n is the number of points.
    # A closed NURBS curve requires only n + 1 knots. The periodicity is defined by equivalence of the first and last knot,
    # i.e. knot[0] == knot[-1]

    # an unclosed NURBS spline requires n + degree + 1 knots, where n is the number of points.
    # A closed NURBS curve requires only n + 1 knots.

    # STEP files mostly use NURBS when defining curves.
    # But the "b_spline_curve_with_knots" command allows for uniform b splines too.
    # It does not discern between the two.

    # STEP files almost only use "open b splines", which means the curve starts and ends at the first and last control points.
    # So the task was to find the control points in between, which there are n-1 of. But there are only n-3 interpolating points,
    # so I input the last two equations as the tangents at the start and end.
    # This is now a linear system of n-1 vector equations, which is easily solved.

    # constructing closed B-spline curves by wrapping knots.
    # to construct a closed B-spline curve C(u) of degree p defined by n+1 control points P0, P1, ..., Pn.
    # The following is the construction procedure:
    # Add a new control point Pn+1 = P0. Therefore, the number of control points is n+2.
    # Find an appropriate knot sequence of n+1 knots u0, u1, ..., un. These knots are not necessarily uniform, an advantage over the method discussed above.
    # Add p+2 knots and wrap around the first p+2 knots: un+1 = u0, un+2 = u1, ..., un+p = up-1, un+p+1 = up, un+p+2 = up+1 as shown in the following diagram. In this way, we have n+p+2 = (n+1) + p + 1 knots

    # https://github.com/FreeCAD/FreeCAD/blob/69097667df47b2cc86d8688d2dbb545319e33e68/src/Mod/Sketcher/App/Sketch.cpp#L1384

    #     // If periodic, startpoint and endpoint do not play a role in the solver, this can remove
    #     // unnecessary DoF of determining where in the curve the start and the stop should be. However,
    #     // since start and end points are placed above knots, removing them leads to that knot being
    #     // unusable.

    #     // If periodic, startpoint and endpoint do not play a role in the solver, this can remove
    #     // unnecessary DoF of determining where in the curve the start and the stop should be. However,
    #     // since start and end points are placed above knots, removing them leads to that knot being
    #     // unusable.

    #     // WARNING: This is only valid where the multiplicity of the endpoints conforms with a BSpline
    #     // only then the startpoint is the first control point and the endpoint is the last control
    #     // point accordingly, it is never the case for a periodic BSpline. NOTE: For an external
    #     // B-spline (i.e. fixed=true) we must not set the coincident constraints as the points are not
    #     // movable anyway. See #issue 0003176: Sketcher: always over-constrained when referencing
    #     // external B-Spline

# https://git.dev.opencascade.org/gitweb/?p=occt.git;a=blob;f=src/BSplCLib/BSplCLib.cxx;h=1414f5008b3b2a79583a0af67e0a05fcd52569cb;hb=HEAD
    # 1917   // index is the first pole of the current curve for insertion schema
    # 1918
    # 1919   if (Periodic) index = -Mults(Mults.Lower());
    # 1920   else          index = -Degree-1;

    # 1922   // on Periodic curves the first knot and the last knot are inserted later
    # 1923   // (they are the same knot)
    # 1924   firstmult = 0;  // multiplicity of the first-last knot for periodic

    maxPointsU = BsplineCurveExtremaDisp(BsplineKnotCurve,
                                        localCentroid,
                                        localExtrema=True,
                                        #curvatureTest=True,
                                        maxSearch=True,
                                        uv_xyz=True)

    maxPoints = BsplineKnotCurve.evaluate_list(maxPointsU)

    minPointsU = BsplineCurveExtremaDisp(BsplineKnotCurve,
                                         localCentroid,
                                         localExtrema=True,
                                         #curvatureTest=True,
                                         maxSearch=False,
                                         uv_xyz=True)

    minPoints = BsplineKnotCurve.evaluate_list(minPointsU)

    # normalise u to [0, 1] - unrequired where normalize_kv=True
    #knotRange = _knotvector[-1] - _knotvector[0]
    #maxPointsU = [(mpu - _knotvector[0])/knotRange for mpu in maxPointsU]
    #minPointsU = [(mpu - _knotvector[0])/knotRange for mpu in minPointsU]

    maximaU = maxPointsU + minPointsU
    maximaPoints = maxPoints + minPoints
    if len(maximaPoints) > 1:
        extremaUindex = np.argsort(maximaU)
        maximaU = [maximaU[eui] for eui in extremaUindex]
        maximaPoints = [maximaPoints[eui] for eui in extremaUindex]

        # because Newton-Raphson will converge on a tangent, separate max/min values may be identical
        maxima_truth = [(maximaU[mu + 1] - maximaU[mu]) > eps_STEP_AP21 for mu in range(0, len(maximaU)-1)]
        maxima_truth = maxima_truth + [True,]
        maximaU = [mu for (mu, mt) in zip(maximaU, maxima_truth) if mt]
        maximaPoints = [mp for (mp, mt) in zip(maximaPoints, maxima_truth) if mt]

        # if vertex1 centroidDisp is close to u[0],
        # or vertex2 centroidDisp is close to u[-1],
        # remove u[0], u[-1]
        centroidDisp = [np.linalg.norm(mp - localCentroid) for mp in maximaPoints]
        vertex1centroidDisp = np.linalg.norm(ParsedEdge['vertex1'] - localCentroid)
        vertex2centroidDisp = np.linalg.norm(ParsedEdge['vertex2'] - localCentroid)
        if np.abs(vertex1centroidDisp - centroidDisp[0]) < eps_STEP_AP21:
            maximaU.pop(0)
            maximaPoints.pop(0)
        else:
            centroidDisp.insert(0, vertex1centroidDisp)
        if np.abs(vertex2centroidDisp - centroidDisp[-1]) < eps_STEP_AP21:
            maximaU.pop(-1)
            maximaPoints.pop(-1)
        else:
            centroidDisp.append(vertex2centroidDisp)

    #maximaPoints.insert(0, ParsedEdge['vertex1']) # ParsedEdge['pointFeature']['xyz'][0])
    #maximaPoints.insert(-1, ParsedEdge['vertex2']) # ParsedEdge['pointFeature']['xyz'][-1])
    ParsedEdge['pointFeature']['xyz'] = np.array([ParsedEdge['vertex1'],] + maximaPoints + [ParsedEdge['vertex2'],])
    ParsedEdge['pointFeature']['u'] = [0,] + maximaU + [1]
    ParsedEdge['pointFeature']['centroidDisp'] = centroidDisp

    # no point adding maxima & minima fields here

    # if len(maximaPoints) > 1:
    #     ParsedEdge['maximaPoints'] = maximaPoints
    #     maximaPointsCentroidDisp = [np.linalg.norm(np.array(e) - localCentroid) for e in maximaPoints]
    #     ParsedEdge['maximaPointsCentroidDisp'] = maximaPointsCentroidDisp
    #     maxPoint = maximaPoints[maximaPointsCentroidDisp.index(max(maximaPointsCentroidDisp))]
    #     ParsedEdge['maxPoint'] = maxPoint
    #     ParsedEdge['maxPointCentroidDisp'] = np.linalg.norm(maxPoint - localCentroid)
    # if len(maximaPoints) == 1:
    #     ParsedEdge['maxPoint'] = maximaPoints[0]
    #     ParsedEdge['maxPointCentroidDisp'] = np.linalg.norm(maxPoint - localCentroid)
    #     ParsedEdge['maximaPointsCentroidDisp'] = None
    #     ParsedEdge['maximaPoints'] = None
    # else:
    #     ParsedEdge['maximaPointsCentroidDisp'] = None
    #     ParsedEdge['maximaPoints'] = None
    #     ParsedEdge['maxPointCentroidDisp'] = None
    #     ParsedEdge['maxPoint'] = None


    # if len(minimaPoints) > 1:
    #     ParsedEdge['minimaPoints'] = minimaPoints
    #     minimaPointsCentroidDisp = [np.linalg.norm(np.array(e) - localCentroid) for e in minimaPoints]
    #     ParsedEdge['minimaPointsCentroidDisp'] = minimaPointsCentroidDisp
    #     minPoint = minimaPoints[minimaPointsCentroidDisp.index(min(minimaPointsCentroidDisp))]
    #     ParsedEdge['maxPoint'] = maxPoint
    #     ParsedEdge['maxPointCentroidDisp'] = np.linalg.norm(minPoint - localCentroid)
    # if len(minimaPoints) == 1:
    #     ParsedEdge['minPoint'] = minimaPoints[0]
    #     ParsedEdge['minPointCentroidDisp'] = np.linalg.norm(minPoint - localCentroid)
    #     ParsedEdge['minimaPointsCentroidDisp'] = None
    #     ParsedEdge['minimaPoints'] = None
    # else:
    #     ParsedEdge['minimaPointsCentroidDisp'] = None
    #     ParsedEdge['maximaPoints'] = None
    #     ParsedEdge['minPointCentroidDisp'] = None
    #     ParsedEdge['minPoint'] = None

    # minPointCentroidDisp = np.linalg.norm(minPoint - localCentroid)
    # ParsedEdge['minPointCentroidDisp'] = minPointCentroidDisp
    # ParsedEdge['minPoint'] = minPoint
    # maxPointCentroidDisp = np.linalg.norm(maxPoint - localCentroid)
    # ParsedEdge['maxPointCentroidDisp'] = maxPointCentroidDisp
    # ParsedEdge['maxPoint'] = maxPoint

    # v1 = ParsedEdge['vertex1']
    # v2 = ParsedEdge['vertex2']

    # # independent check for vertices,
    # ParsedEdge['vertex1extremaMin'] = False
    # ParsedEdge['vertex2extremaMin'] = False
    # ParsedEdge['vertex1extremaMax'] = False
    # ParsedEdge['vertex2extremaMax'] = False
    #
    # if ParsedEdge['minPointCentroidDisp'] is not None:
    #     if ParsedEdge['vertex1centroidDisp'] < ParsedEdge['minPointCentroidDisp']:
    #         ParsedEdge['minPoint'] = ParsedEdge['vertex1']
    #         ParsedEdge['minPointCentroidDisp'] = ParsedEdge['vertex1centroidDisp']
    #         ParsedEdge['vertex1extremaMin'] = True
    #
    # if ParsedEdge['minPointCentroidDisp'] is not None:
    #     if ParsedEdge['vertex2centroidDisp'] < ParsedEdge['minPointCentroidDisp']:
    #         ParsedEdge['minPoint'] = ParsedEdge['vertex2']
    #         ParsedEdge['minPointCentroidDisp'] = ParsedEdge['vertex2centroidDisp']
    #         ParsedEdge['vertex2extremaMin'] = True
    #
    # if ParsedEdge['maxPointCentroidDisp'] is not None:
    #     if ParsedEdge['vertex1centroidDisp'] > ParsedEdge['maxPointCentroidDisp']:
    #         ParsedEdge['maxPoint'] = ParsedEdge['vertex1']
    #         ParsedEdge['maxPointCentroidDisp'] = ParsedEdge['vertex1centroidDisp']
    #         ParsedEdge['vertex1extremaMax'] = True
    #
    # if ParsedEdge['maxPointCentroidDisp'] is not None:
    #     if ParsedEdge['vertex2centroidDisp'] > ParsedEdge['maxPointCentroidDisp']:
    #         ParsedEdge['maxPoint'] = ParsedEdge['vertex2']
    #         ParsedEdge['maxPointCentroidDisp'] = ParsedEdge['vertex2centroidDisp']
    #         ParsedEdge['vertex2extremaMax'] = True

    # #BsplineKnotCurve = BSpline.Curve(normalize_kv=False)
    # BsplineKnotCurve = NURBS.Curve()
    # BsplineKnotCurve.degree = curveDegree
    # #BsplineKnotCurve.ctrlpts = [list(c) for c in controlPoints]
    # #BsplineKnotCurve.knotvector = STEPknotvector
    # BsplineKnotCurve.set_ctrlpts(compatibility.combine_ctrlpts_weights(controlPoints, weights=None))

    # detect rotationally symmetric spline surfaces, spline surface cylinder
    # get the mean point of rings/arcs of control points
    splineMeanPoint = np.array(BsplineKnotCurve.ctrlpts).mean(axis=0)

    # test for deviation from mean point
    ctrlptsDelta = [np.linalg.norm(splineMeanPoint - sc) for sc in BsplineKnotCurve.ctrlpts]

    # low ctrlptsDelta values suggest rotational symmetry,
    if (max(np.abs(ctrlptsDelta)) < eps_STEP_AP21): # rotSymLimit constant --------------------------------------------
        ParsedEdge['rotSymFlag'] = 1

    ParsedEdge['BsplineKnotCurve'] = BsplineKnotCurve


# def subEdgeParse(edgeInstance_, ParsedEdge, STEP_entities_):
#     # SEAM_EDGE,
#     ParsedEdge['subTypeName'] = STEP_entities_[ref2index(edgeInstance_)].type_name


def edgeSTEPparse(edgeInstance_, STEP_entities_):
    # extract data common to all STEP edge instances
    ParsedEdge = {}
    if (STEP_entities_[ref2index(edgeInstance_)].type_name == 'ORIENTED_EDGE'):
        # if not hasattr(STEP_entities_[ref2index(edgeLoopInstance)].type_name, 'ORIENTED_EDGE'): ----------------------
        VOR = 'F' in STEP_entities_[ref2index(edgeInstance_)].params[-1]  # vertexOrderReversed
        ParsedEdge['VOR'] = VOR

    edgeCurveRef = STEP_entities_[ref2index(edgeInstance_)].params[-2]
    assert STEP_entities_[ref2index(edgeCurveRef)].type_name == 'EDGE_CURVE'
    # edge_geometry: the Curve defining the geometric shape of the edge.
    # same_sense: a BOOLEAN variable giving the relationship between the topological sense
    #             of the edge and the parametric sense of the curve.
    edgeCurveParams = STEP_entities_[ref2index(edgeCurveRef)].params
    # edgeSameSense = 'T' in edgeCurveParams[-1]; ParsedEdge['sameSense'] = edgeSameSense
    edgeCurveParams = cleanSubRefs(edgeCurveParams)

    if (STEP_entities_[ref2index(edgeCurveParams[0])].type_name == 'VERTEX_POINT'):
        vertex1 = STEP_entities_[ref2index(edgeCurveParams[0])].params
        vertex1 = CP2point(STEP_entities_, cleanSubRefs(vertex1)[0])
        if not VOR:
            ParsedEdge['vertex1'] = np.array(vertex1)
            ParsedEdge['vertex1ref'] = STEP_entities_[ref2index(edgeCurveParams[0])].ref
            # vertex1centroidDisp = np.linalg.norm(centroid - vertex1)
            # ParsedEdge['vertex1centroidDisp'] = vertex1centroidDisp
        else:
            ParsedEdge['vertex2'] = np.array(vertex1)
            ParsedEdge['vertex2ref'] = STEP_entities_[ref2index(edgeCurveParams[0])].ref
            # vertex2centroidDisp = np.linalg.norm(centroid - vertex1)
            # ParsedEdge['vertex2centroidDisp'] = vertex2centroidDisp

    if (STEP_entities_[ref2index(edgeCurveParams[1])].type_name == 'VERTEX_POINT'):
        vertex2 = STEP_entities_[ref2index(edgeCurveParams[1])].params
        vertex2 = CP2point(STEP_entities_, cleanSubRefs(vertex2)[0])
        if not VOR:
            ParsedEdge['vertex2'] = np.array(vertex2)
            ParsedEdge['vertex2ref'] = STEP_entities_[ref2index(edgeCurveParams[1])].ref
            # vertex2centroidDisp = np.linalg.norm(centroid - vertex2)
            # ParsedEdge['vertex2centroidDisp'] = vertex2centroidDisp
        else:
            ParsedEdge['vertex1'] = np.array(vertex2)
            ParsedEdge['vertex1ref'] = STEP_entities_[ref2index(edgeCurveParams[1])].ref
            # vertex2centroidDisp = np.linalg.norm(centroid - vertex2)
            # ParsedEdge['vertex1centroidDisp'] = vertex2centroidDisp

    ParsedEdge['pointFeature'] = {}
    ParsedEdge['pointFeature']['xyz'] = [ParsedEdge['vertex1'], ParsedEdge['vertex2']]
    ParsedEdge['pointFeature']['centroidDisp'] = [np.linalg.norm(centroid - vertex1),
                                                     np.linalg.norm(centroid - vertex2)]
    ParsedEdge['pointFeature']['u'] = [0, 1] # normalise to [0, 1]

    edgeType = STEP_entities_[ref2index(edgeCurveParams[2])].params
    edgeTypeName = STEP_entities_[ref2index(edgeCurveParams[2])].type_name
    edgeRef = STEP_entities_[ref2index(edgeCurveParams[2])].ref
    ParsedEdge['typeName'] = edgeTypeName
    ParsedEdge['edgeRef'] = edgeRef
    ParsedEdge['edgeType'] = edgeType

    return ParsedEdge

    # a proper object initiation would create extra terms to avoid messy ".get('x') is None" clauses
    # while scope is unsettled this is parked here for edge object, presumably surface requires the same

    # 'subTypeName'
    # 'subEdgeType'
    # 'normDir'
    # 'radius'
    # 'axisPoint'
    #
    # 'rotSymRadius'
    # 'rotSymCentre'
    # 'rotSymMin'
    # 'rotSymMax'


def edgeParse(c, ParsedEdge, STEP_entities_):
    #print("simpleEdgeParse edge type: " + ParsedEdge['typeName'])

    if (ParsedEdge['typeName'] == 'LINE'):
        segmentParse(c, ParsedEdge, STEP_entities_)

    elif (ParsedEdge['typeName'] == 'CIRCLE'):
        circleParse(c, ParsedEdge, STEP_entities_)

    elif (ParsedEdge['typeName'] == 'ELLIPSE'):
        ellipseParse(c, ParsedEdge, STEP_entities_)

    elif (ParsedEdge['typeName'] == 'B_SPLINE_CURVE_WITH_KNOTS'):
        BsplineCurveWithKnotsParse(c, ParsedEdge, STEP_entities_)

    else: print("simpleEdgeParse unknown edge type: " + ParsedEdge['typeName'])


def sphereSurfaceParse(AFS, localCentroid, minimaExtrema=False):
    # ENTITY Spherical_surface
    #   SUBTYPE OF (Surface);
    #   position : Axis_placement_3d;
    #   radius : positive_length_measure;

    # Attribute definitions:
    # position: an Axis_placement_3d that defines the location and orientation of the surface for the purposes of parameterization.
    # The centre of the Spherical_surface is at the location
    # radius: the radius of the Spherical_surface.

    #  The axis is the placement Z axis direction and the ref_direction is an approximation to
    # the placement X axis direction.= normDir


    if AFS.get('ParsedSurface') is not None: # recalculation of max/min
        ParsedSurface = AFS['ParsedSurface']
        centrePoint = ParsedSurface['axisPoint']
        normDir = ParsedSurface['normDir'] # Z
        refDir = ParsedSurface['refDir'] # X
        auxDir = ParsedSurface['auxDir']  # Y
    else:
        centrePoint, normDir, refDir = axis2Placement3D_2(AFS['SurfaceParams'][1], STEP_entities)
        auxDir = np.cross(normDir, refDir)
        auxDir = auxDir / np.linalg.norm(auxDir)
        radius = AFS['SurfaceParams'][-1]

        ParsedSurface = {}
        ParsedSurface['radius'] = radius
        ParsedSurface['axisPoint'] = centrePoint
        ParsedSurface['normDir'] = normDir  # Z
        ParsedSurface['refDir'] = refDir  # X
        ParsedSurface['auxDir'] = auxDir

        ParsedSurface['pointFeature'] = {}
        ParsedSurface['pointFeature']['xyz'] = []
        ParsedSurface['pointFeature']['centroidDisp'] = []
        ParsedSurface['pointFeature']['maxima'] = []
        ParsedSurface['pointFeature']['minima'] = []

    AFS['ParsedSurface'] = ParsedSurface
    if not minimaExtrema: # not much point
        # return ParsedSurface
        return

    # sphere surface maxima & minima exist on a vector between centroid and sphere centre point
    # minima/maxima = centre point -/+ radius

    # "The local z-axis corresponds to the normal of planar and spherical surfaces and to the axis of cylindrical,
    # conical and toroidal surfaces."
    # Jaider Oussama et al Int. Journal of Engineering Research and Applications www.ijera.com
    # ISSN : 2248-9622, Vol. 4, Issue 5( Version 6), May 2014, pp.14-25
    # refDir, auxDir doesn't mean much in stock implementaions.

    sphereCentroidOffset = np.linalg.norm(centrePoint - localCentroid)
    if sphereCentroidOffset < eps_STEP_AP21: # determine if centroid is coincident with sphere centre
        ParsedSurface['sphereFeatureFlag'] = True

        ParsedSurface['rotSymFeature'] = dict()
        ParsedSurface['rotSymFeature']['rotSymCentre'] = centrePoint
        ParsedSurface['rotSymFeature']['rotSymRadius'] = radius

    else:
        sphereCentreCentroidDir = (centrePoint - localCentroid) / sphereCentroidOffset

        # determine if maxPoint/minPoint are inside or outside an edge_loop defined boundary
        # CCW directionality of edge vertices determines outward surface normal
        # surface minPoint/maxPoint always orthonormal to localCentroid

        # sphere sector has at most one maximum and one minimum surface point.

        maxPoint = centrePoint + sphereCentreCentroidDir * radius
        minPoint = centrePoint - sphereCentreCentroidDir * radius

        if insideOutsideSurfaceTest(localCentroid, maxPoint, AFS): # maxima not among edges
            ParsedSurface['pointFeature']['xyz'].append(maxPoint)
            ParsedSurface['pointFeature']['centroidDisp'].append(np.linalg.norm(maxPoint - localCentroid))
            ParsedSurface['pointFeature']['maxima'].append(False)
            ParsedSurface['pointFeature']['minima'].append(True)

        if insideOutsideSurfaceTest(localCentroid, minPoint, AFS): # maxima not among edges
            ParsedSurface['pointFeature']['xyz'].append(minPoint)
            ParsedSurface['pointFeature']['centroidDisp'].append(np.linalg.norm(minPoint - localCentroid))
            ParsedSurface['pointFeature']['maxima'].append(True)
            ParsedSurface['pointFeature']['minima'].append(False)

    AFS['ParsedSurface'] = ParsedSurface
    #return ParsedSurface


def toroidSurfaceParse(AFS, localCentroid, minimaExtrema=False):
    # ENTITY Toroidal_surface
    #   SUBTYPE OF (Surface);
    #   position : Axis_placement_3d;
    #   radius : positive_length_measure;
    #   minor_radius : positive_length_measure;

    # Attribute definitions:
    # position: an Axis_placement_3d that defines the location and orientation of the surface.
    # The centre of the Toroidal_surface is at the location. The plane of the ref_direction and the axis is a
    # central plane of symmetry of the Toroidal_surface. The normal to this plane is the central axis of the surface.
    # radius: the distance from the central axis of the Toroidal_surface to the centre of one of the circles
    # produced by intersecting the Toroidal_surface with a plane containing the central axis.
    # The surface can be produced by sweeping a circle of radius minor_radius along a circle of radius radius.
    # minor_radius: the radius of one of the circles produced by intersecting the Toroidal_surface with a plane
    # containing the central axis.

    if AFS.get('ParsedSurface') is not None: # recalculation of max/min
        ParsedSurface = AFS['ParsedSurface']
        axisPoint = ParsedSurface['axisPoint']
        normDir = ParsedSurface['normDir'] # Z
        refDir = ParsedSurface['refDir'] # X
        auxDir = ParsedSurface['auxDir']  # Y
    else:
        ParsedSurface = {}
        axisPoint, normDir, refDir = axis2Placement3D_2(AFS['SurfaceParams'][1], STEP_entities)
        auxDir = np.cross(normDir, refDir)
        auxDir = auxDir / np.linalg.norm(auxDir)
        minorRadius = AFS['SurfaceParams'][-1]
        majorRadius = AFS['SurfaceParams'][-2]

        ParsedSurface['axisPoint'] = axisPoint
        ParsedSurface['normDir'] = normDir  # Z
        ParsedSurface['refDir'] = refDir  # X
        ParsedSurface['auxDir'] = auxDir

        ParsedSurface['minorRadius'] = minorRadius
        ParsedSurface['majorRadius'] = majorRadius

        ParsedSurface['pointFeature'] = {}
        ParsedSurface['pointFeature']['xyz'] = []
        ParsedSurface['pointFeature']['centroidDisp'] = []
        ParsedSurface['pointFeature']['maxima'] = []
        ParsedSurface['pointFeature']['minima'] = []

    AFS['ParsedSurface'] = ParsedSurface
    if not minimaExtrema:
        # return ParsedSurface
        return

    # localCentroid = np.array([1, 1, 0])
    # axisPoint = np.array([0, 0, 10])
    # axisDir = np.array([0, 0, 1])
    # refDir = np.array([1, 0, 0])
    # tMajorRadius = 10
    # tMinorRadius = 1

    # test if toroid is rotationally symmetrical, if localCentroid is close to normDir through axisPoint

    rotSymDisp = np.linalg.norm(
        (centroid - axisPoint) - np.dot((centroid - axisPoint), normDir) * normDir
    )

    # need a precision factor that reflects increasing accuracy arising from iterative calculations of localCentroid --------------------------------------<<<<<<<<<<<<<<<<<
    # does one find a localCentroid through axis-intersection?
    # also scale factor affecting numerical precision
    # also STEP seems to be E-6 decimal places at best
    # iterativeScaledPrecision = 1e-4  # for testing

    if rotSymDisp < eps_STEP_AP21:
        ParsedEdge['rotSymFeature'] = dict()
        ParsedEdge['rotSymFeature']['rotSymCentre'] = axisPoint
        #ParsedEdge['rotSymFeature']['rotSymRadius'] = radius
        #ParsedSurface['rotSymCentre'] = axisPoint
    else:
        # create axes for circles based at points of majorMinPoint, majorMaxPoint
        # minima case, create new axis at majorMinPoint
        # get min, max of major radius
        majorMinPoint, majorMaxPoint = pointCircleMinMaxDisp(
            localCentroid,
            axisPoint,
            normDir,
            majorRadius
        )

        # vector from toroid centre to minima point on majorRadius
        majorMinRadialDir = (majorMinPoint - axisPoint) / np.linalg.norm(
            majorMinPoint - axisPoint
        )
        # vector tangential to major radius
        majorMinTangentDir = np.cross(majorMinRadialDir, normDir)
        majorMinTangentDir = majorMinTangentDir / np.linalg.norm(
            majorMinTangentDir
        )

        # point on minor circle cross-section at minimum disp from localCentroid
        minorMinPoint, _ = pointCircleMinMaxDisp(
            localCentroid,
            majorMinPoint,
            majorMinTangentDir,
            minorRadius,
        )

        #ParsedSurface['minPoint'] = minorMinPoint

        # same for maxima
        majorMaxRadialDir = (majorMaxPoint - axisPoint) / np.linalg.norm(
            majorMaxPoint - axisPoint
        )
        majorMaxTangentDir = np.cross(majorMaxRadialDir, auxDir)
        majorMaxTangentDir = majorMaxTangentDir / np.linalg.norm(
            majorMaxTangentDir
        )

        minorMaxPoint, _ = pointCircleMinMaxDisp(
            localCentroid,
            majorMaxPoint,
            majorMaxTangentDir,
            minorRadius,
        )

        # np.linalg.norm(centroid - tMinorMinPoint) < np.linalg.norm(centroid - tMinorMaxPoint)
        #ParsedSurface['maxPoint'] = minorMaxPoint

        # determine if maxPoint/minPoint are inside or outside an edge_loop defined boundary
        # CCW directionality of edge vertices determines outward surface normal
        # surface minPoint/maxPoint always orthonormal to localCentroid

        if insideOutsideSurfaceTest(localCentroid, minorMaxPoint, AFS): # maxima is among edges
            ParsedSurface['pointFeature']['xyz'].append(minorMaxPoint)
            ParsedSurface['pointFeature']['centroidDisp'].append(np.linalg.norm(minorMaxPoint - localCentroid))
            ParsedSurface['pointFeature']['maxima'].append(False)
            ParsedSurface['pointFeature']['minima'].append(True)

        if insideOutsideSurfaceTest(localCentroid, minorMinPoint, AFS): # minima is among edges
            ParsedSurface['pointFeature']['xyz'].append(minorMinPoint)
            ParsedSurface['pointFeature']['centroidDisp'].append(np.linalg.norm(minorMinPoint - localCentroid))
            ParsedSurface['pointFeature']['maxima'].append(True)
            ParsedSurface['pointFeature']['minima'].append(False)

    AFS['ParsedSurface'] = ParsedSurface
    #return ParsedSurface


def cylinderSurfaceParse(AFS, localCentroid, minimaExtrema=False):
    '''
    maximaExtrema flag, exit function without expensive minima calculations.
    Used during centroid iteration
    '''
    # ENTITY Cylindrical_surface
    #   SUBTYPE OF (Surface);
    #   position : Axis_placement_3d;
    #   radius : positive_length_measure;

    # position: an Axis_placement_3d that defines the location and orientation of the surface.
    # The axis of the Cylindrical_surface passes through the location and is normal to the plane of ref_direction and axis.
    # radius: the radius of the circular curve of intersection between the surface of the cylinder and a plane
    # perpendicular to the axis of the Cylindrical_surface.

    if AFS.get('ParsedSurface') is not None: # recalculation of max/min
        ParsedSurface = AFS['ParsedSurface']
        axisPoint = ParsedSurface['axisPoint']
        normDir = ParsedSurface['normDir'] # Z
        refDir = ParsedSurface['refDir'] # X
        auxDir = ParsedSurface['auxDir']  # Y
        radius = ParsedSurface['radius']
    else:
        axisPoint, normDir, refDir = axis2Placement3D_2(AFS['SurfaceParams'][1], STEP_entities)
        auxDir = np.cross(normDir, refDir)
        auxDir = auxDir / np.linalg.norm(auxDir)
        radius = AFS['SurfaceParams'][-1]

        ParsedSurface = {}
        ParsedSurface['radius'] = radius
        ParsedSurface['axisPoint'] = axisPoint
        ParsedSurface['normDir'] = normDir  # Z # FreeCAD STEP generator reverses normal wrt to cylinder end planes.. ignore?
        ParsedSurface['refDir'] = refDir  # X
        ParsedSurface['auxDir'] = auxDir

        ParsedSurface['pointFeature'] = {}
        ParsedSurface['pointFeature']['xyz'] = []
        ParsedSurface['pointFeature']['centroidDisp'] = []
        ParsedSurface['pointFeature']['maxima'] = []
        ParsedSurface['pointFeature']['minima'] = []

    AFS['ParsedSurface'] = ParsedSurface
    if not minimaExtrema:
        # return ParsedSurface
        return

    # test if cylinder is rotationally symmetrical, if localCentroid is close to axisDir through axisPoint
    rotSymDisp = np.linalg.norm(
        (localCentroid - axisPoint) - np.dot((localCentroid - axisPoint), normDir) * normDir
    )

    if rotSymDisp < eps_STEP_AP21:
        minSurfaceDispPoint = localCentroid  # for creating a minima plane intersection
        #centroidProjAxisPoint = centroid
        # maxima is circular edge centre, determined from vertices
        # minima is orthogonal to centroid
        # shouldn't there also be a radius measure?? check: not in doctorate schema

        # todo, update concept where the region between a ridge and groove is a sphere
        # todo: truth determined via wandering hillclimb point path, or grad

        # test if any edge crosses cylinder midsection
        # edges should all have been calculated prior to surface

        # circle edge cannot be declared groove or ridge prior to discovery of adjacent surfaces
        # rotationally-symmetric edge must be assigned indeterminate status with centroid & radius

        # insideOutsideSurfaceTest() doesn't work for objects curved more than 180 degrees

        # test if any edges intersect a plane through centroid and normal to axis
        # edges must contain a minimum radius value
        # minimum value of all cylinder edge must equal radius at (points)/2 = 0 if surface contains minimum radius feature

        edgeMinimaCount = 0
        for edge in AFS['outerBoundEdgeLoop']:
            for cd in edge['pointFeature']['centroidDisp']:
                if np.isclose(cd, radius, eps):
                    edgeMinimaCount +=1

        if edgeMinimaCount % 2 == 0:
            ParsedSurface['rotSymFeature'] = dict()
            ParsedSurface['rotSymFeature']['rotSymCentre'] = localCentroid
            ParsedSurface['rotSymFeature']['rotSymRadius'] = radius

        # if edgeMinimaCount == 1: can't be rotational minima if only one point

        # featureExtrema may also encompass radial symmetric feature,
        # ['xyCentre'] = ['xy']
        # ['centreCentroidDisp']??
        # ['radiusCentroidDisp']

        #         # project centroid to symmetric axis to find rotSymCentre---HUH???
        #         centroidProjAxisPoint = pointProjectAxis(localCentroid, axisPoint, normDir)
        #         ParsedSurface['rotSymCentre'] = centroidProjAxisPoint
        #         # minPointCentroidDisp/maxPointCentroidDisp exist where centroid is on rotSym axis
        #         ParsedSurface['minPointCentroidDisp'] = np.sqrt(np.linalg.norm(centroidProjAxisPoint - localCentroid)**2 + radius**2)

    else:
        # cylinder/conic maxima is a search of greatest disp of vertices from localCentroid
        # if localCentroid is outside full cylindrical surface, edges contain maxima/minima
        # circle edges are maxima where circle normal contains local centroid,
        # cylinder minima -> test for intersection with surface on orthogonal projection from axis to localCentroid
        # if localCentroid within surface, |centroid - projAxisDir| < cRadius

        # test:
        # axisPoint = np.array([1, 0, 0])
        # localCentroid = np.array([2, 2, 0])
        # axisDir = np.array([1.5, 0, 0])
        # cRadius = 1

        # orthogonal projection of localCentroid to cylinder axis
        # vCA = localCentroid - axisPoint
        # vNA = axisPoint - normDir
        # centroidProjAxisPoint = vNA*np.dot(vCA, vNA)/np.dot(vNA, vNA)

        centroidProjAxisPoint = pointProjectAxis(localCentroid, axisPoint, normDir)

        # centroidProjAxisPoint = axisPoint + (
        #             np.dot((centroid - axisPoint), normDir) / np.dot(normDir, normDir)) * normDir
        centroidAxisDir = centroidProjAxisPoint - localCentroid
        centroidAxisDisp = np.linalg.norm(centroidAxisDir)
        centroidAxisDir = centroidAxisDir / centroidAxisDisp

        smp = ((centroidAxisDisp - radius) * centroidAxisDir)

        if np.linalg.norm(localCentroid - smp) > np.linalg.norm(localCentroid + smp):
            surfaceMinPoint = localCentroid - smp
        else:
            surfaceMinPoint = localCentroid + smp

        # can't determine in advance whether rotSymMax or rotSymCentre => requires subsequent comparison to adjacent nodes
        
        if insideOutsideSurfaceTest(localCentroid, surfaceMinPoint, AFS):  # minima is among edges
            ParsedSurface['pointFeature']['xyz'].append(surfaceMinPoint)
            ParsedSurface['pointFeature']['centroidDisp'].append(np.linalg.norm(surfaceMinPoint - localCentroid))
            ParsedSurface['pointFeature']['maxima'].append(False)
            ParsedSurface['pointFeature']['minima'].append(True)

    AFS['ParsedSurface'] = ParsedSurface


def coneSurfaceParse(AFS, localCentroid, minimaExtrema=False):
    # ENTITY Conical_surface
    #   SUBTYPE OF (Surface);
    #   position : Axis_placement_3d;
    #   radius : positive_length_measure;
    #   semi_angle : plane_angle_measure;

    # position: an Axis_placement_3d that defines the location and orientation of the surface.
    # The axis of the Conical_surface passes through the location and is normal to the plane of ref_direction and axis.
    # radius: the radius of the circular curve of intersection between the cone and a plane perpendicular to the
    # axis of the cone passing through the location. This will have value 0.0 if the location is at the apex of the cone.
    # semi_angle: the cone semi-angle, this is the angle between the axis of the Conical_surface and the generating line.

    # note that normal is defined in apex->location direction
    # note 2 that the radial surface may define the top of a frustum rather than the base, need to check enclosing edges
    # https://quaoar.su/files/standards/Standard%20-%202003%20-%20ISO%2010303-42.pdf p88

    if AFS.get('ParsedSurface') is not None: # recalculation of max/min
        ParsedSurface = AFS['ParsedSurface']
        axisPoint = ParsedSurface['axisPoint']
        normDir = ParsedSurface['normDir'] # Z
        refDir = ParsedSurface['refDir'] # X
        auxDir = ParsedSurface['auxDir']  # Y
        radius = ParsedSurface['radius']
        semiAngle = ParsedSurface['semiAngle']

    else:
        axisPoint, normDir, refDir = axis2Placement3D_2(AFS['SurfaceParams'][1], STEP_entities)
        auxDir = np.cross(normDir, refDir)
        auxDir = auxDir / np.linalg.norm(auxDir)
        semiAngle = AFS['SurfaceParams'][-1]
        radius = AFS['SurfaceParams'][-2]

        ParsedSurface = {}
        ParsedSurface['axisPoint'] = axisPoint
        ParsedSurface['normDir'] = normDir  # Z
        ParsedSurface['refDir'] = refDir  # X
        ParsedSurface['auxDir'] = auxDir
        ParsedSurface['semiAngle'] = semiAngle
        ParsedSurface['radius'] = radius

        ParsedSurface['pointFeature'] = {}
        ParsedSurface['pointFeature']['xyz'] = []
        ParsedSurface['pointFeature']['centroidDisp'] = []
        ParsedSurface['pointFeature']['maxima'] = []
        ParsedSurface['pointFeature']['minima'] = []

        AFS['ParsedSurface'] = ParsedSurface # for the purpose of extracting axisPoint, normDir for inside/outside point detection

    AFS['ParsedSurface'] = ParsedSurface
    if not minimaExtrema:
        # return ParsedSurface
        return

    # test if cone is rotationally symmetrical, if localCentroid is close to axisDir through axisPoint
    rotSymDisp = np.linalg.norm(
        (localCentroid - axisPoint) - np.dot((localCentroid - axisPoint), normDir) * normDir
    )

    #rotSymDisp = pointProjectAxis(localCentroid, axisPoint, normDir)

    axisPointApexDisp = radius / np.tan(semiAngle)
    # find point at cone/triangle apex
    apexPoint = axisPoint + axisPointApexDisp * (-normDir)


    # circle edge cannot be declared groove or ridge prior to discovery of adjacent surfaces
    # rotationally-symmetric edge must be assigned indeterminate status with centroid & radius

    # test whether centroid is on the far side of the apex
    if np.linalg.norm(centroid - apexPoint) < np.linalg.norm(centroid - axisPoint):
        if insideOutsideSurfaceTest(localCentroid, apexPoint, AFS):
            ParsedEdge['rotSymFeature'] = dict()
            ParsedEdge['rotSymFeature']['rotSymCentre'] = apexPoint
            ParsedEdge['rotSymFeature']['rotSymRadius'] = 0

            #ParsedSurface['rotSymCentre'] = apexPoint
            #ParsedSurface['rotSymRadius'] = 0

    else:

        if rotSymDisp > eps_STEP_AP21:
            # get normal to plane containing centroid & cone axis, cross product of (apex - centroid) X (apex - coneLocation)
            minPlaneNorm = np.cross((apexPoint - localCentroid), (apexPoint - axisPoint))
            minPlaneNorm = minPlaneNorm / np.linalg.norm(minPlaneNorm)

            coneEdgeCW = np.dot(rotationMatrix(minPlaneNorm, semiAngle),(apexPoint - axisPoint))
            coneEdgeCW = coneEdgeCW / np.linalg.norm(coneEdgeCW)
            coneMinCW = pointProjectAxis(localCentroid, apexPoint, coneEdgeCW)

            coneEdgeCCW = np.dot(rotationMatrix(minPlaneNorm, -semiAngle),(apexPoint - axisPoint))
            coneEdgeCCW = coneEdgeCCW / np.linalg.norm(coneEdgeCCW)
            coneMinCCW = pointProjectAxis(localCentroid, apexPoint, coneEdgeCCW)

            if np.linalg.norm(localCentroid - coneMinCW) >= np.linalg.norm(localCentroid - coneMinCCW):
                coneMin = coneMinCCW
            else:
                coneMin = coneMinCW

            if insideOutsideSurfaceTest(localCentroid, coneMin, AFS):
                ParsedSurface['pointFeature']['xyz'].append(coneMin)
                ParsedSurface['pointFeature']['centroidDisp'].append(np.linalg.norm(coneMin - localCentroid))
                ParsedSurface['pointFeature']['maxima'].append(False)
                ParsedSurface['pointFeature']['minima'].append(True)

        else:
            # there is a band around a cone corresponding to a minima
            coneMinimaCentreDisp = np.linalg.norm(centroid - apexPoint) * np.cos(semiAngle)
            coneMinimaCentrePoint = apexPoint + (coneMinimaCentreDisp * normDir)

            # check is band within edges,
            rotSymCentroidDisp = np.linalg.norm(centroid - apexPoint) * np.sin(semiAngle)

            edgeMinimaCount = 0
            for edge in AFS['outerBoundEdgeLoop']:
                for cd in edge['pointFeature']['centroidDisp']:
                    if np.isclose(cd, rotSymCentroidDisp, eps_STEP_AP21):
                        edgeMinimaCount +=1

            if edgeMinimaCount % 2 == 0:
                ParsedSurface['rotSymFeature'] = dict()
                ParsedSurface['rotSymFeature']['rotSymCentre'] = coneMinimaCentrePoint
                ParsedSurface['rotSymFeature']['rotSymRadius'] = np.linalg.norm(coneMinimaCentrePoint - apexPoint) * np.tan(semiAngle)
                #ParsedSurface['rotSymRadius'] = np.linalg.norm(coneMinimaCentrePoint - apexPoint) * np.tan(semiAngle)
                #ParsedSurface['rotSymCentre'] = coneMinimaCentrePoint

    # else:
    #
    #     # project centroid to nearest side, i.e.
    #     # construct 2 rays through apex at half angles from cone axis within same plane as centroid
    #     # shortest orthogonal projection to either line is surface minima,
    #
    #     # project centroid to symmetric axis to find rotSymCentre
    #
    #     # cylinder/conic maxima is a search of greatest disp of vertices from localCentroid
    #     # if localCentroid is outside full cylindrical/conical surface radius,
    #     # test for intersection with surface on orthogonal projection from axis to localCentroid
    #     # if localCentroid within surface, |centroid - projAxisDir| < cRadius
    #
    #     # test:
    #     # axisPoint = np.array([1, 0, 0])
    #     # localCentroid = np.array([2, 2, 0])
    #     # axisDir = np.array([1.5, 0, 0])
    #     # cRadius = 1
    #
    #     # orthogonal projection of localCentroid to cylinder axis
    #     # vCA = localCentroid - axisPoint
    #     # vNA = axisPoint - normDir
    #     # centroidProjAxisPoint = vNA*np.dot(vCA, vNA)/np.dot(vNA, vNA)
    #
    #     #centroidProjAxisPoint = pointProjectAxis(localCentroid, axisPoint, normDir)-------------------------------------broken??
    #
    #     centroidProjAxisPoint = axisPoint + ( np.dot((centroid - axisPoint), normDir) / np.dot(normDir, normDir)) * normDir
    #     centroidAxisDir = centroidProjAxisPoint - localCentroid
    #     centroidAxisDisp = np.linalg.norm(centroidAxisDir)
    #     centroidAxisDir = centroidAxisDir / centroidAxisDisp
    #
    #     coneMinimaCentreDisp = axisPointApexDisp / np.cos(semiAngle)
    #     coneMinimaCentrePoint = apexPoint + (coneMinimaCentreDisp * normDir)
    #
    #     # calculate cone radius at min
    #     coneMinRadius = np.linalg.norm(apexPoint - coneMinimaCentrePoint) * np.tan(semiAngle)
    #
    #     smp1 = coneMinimaCentrePoint + (coneMinRadius * -centroidAxisDir)
    #     smp2 = coneMinimaCentrePoint - (coneMinRadius * -centroidAxisDir)
    #
    #     if np.linalg.norm(localCentroid - smp1) > np.linalg.norm(localCentroid - smp2):
    #         surfaceMinPoint = smp1
    #     else:
    #         surfaceMinPoint = smp2
    #
    #     if insideOutsideSurfaceTest(localCentroid, surfaceMinPoint, AFS):  # minima is among edges
    #         ParsedSurface['pointFeature']['xyz'].append(surfaceMinPoint)
    #         ParsedSurface['pointFeature']['centroidDisp'].append(np.linalg.norm(surfaceMinPoint - localCentroid))

    AFS['ParsedSurface'] = ParsedSurface


def BSplineSurfaceWithKnotsParse(AFS, localCentroid, STEP_entities_, minimaExtrema=False):

    # ENTITY Surface_with_explicit_knots
    #   SUBTYPE OF (B_spline_surface);
    #   u_knot_multiplicities : LIST[2:?] OF INTEGER;
    #   u_knot_values : LIST[2:?] OF parameter_value;
    #   v_knot_multiplicities : LIST[2:?] OF INTEGER;
    #   v_knot_values : LIST[2:?] OF parameter_value;
    # WHERE
    #   WR1: SIZEOF(u_knot_multiplicities) = SIZEOF(u_knot_values);
    #   WR2: SIZEOF(v_knot_multiplicities) = SIZEOF(v_knot_values);

    # Attribute definitions:
    # u_knot_multiplicities: the list of multiplicities for the u_knot_values, these are in the first parametric direction for the surface.
    # u_knot_values: the list of knot values used to define the B-spline basis functions in the first parametric direction of the surface.
    # v_knot_multiplicities: the list of multiplicities for the v_knot_values, these are in the second parametric direction for the surface.
    # v_knot_values: the list of knot values used to define the B-spline basis functions in the second parametric direction of the surface.

    # u_multiplicities: The multiplicities of the knots in the u parameter direction.
    # v_multiplicities: The multiplicities of the knots in the v parameter direction.
    # u_knots: The list of the distinct knots in the u parameter direction.
    # v_knots: The list of the distinct knots in the v parameter direction.
    # knot_spec: The description of the knot type.
    # knot_u_upper: The number of distinct knots in the u parameter direction.
    # knot_v_upper: The number of distinct knots in the v parameter direction.
    # SELFnb_spline_surface.u_degree: Algebraic degree of basis functions in u.
    # SELFnb_spline_surface.v_degree: Algebraic degree of basis functions in v.
    # SELFnb_spline_surface.control_points_list: This is a list of lists of control points.
    # SELF\b_spline_surface.surface_form: Indicator of special surface types. (See 4.3.4.)
    # SELF\b_spline_surface.u_closed: Indication of whether the surface is closed in the u direction; this
    # is for information only.
    # SELFnb_spline_surface.v_closed: Indication of whether the surface is closed in the v direction; this is
    # for information only.
    # SELFnb_spline_surface.self_intersect: Flag to indicate whether, or not, surface is self-intersecting;
    # this is for information only.
    # SELFnb_spline_surface.u_upper: Upper index on control points in u direction.
    # SELFnb_spline_surface.v_upper: Upper index on control points in v direction.
    # SELFnb_spline_surface.control_points: Array (two-dimensional) of control points defining surface
    # geometry. This array is constructed from the control points list

    #       u_degree : INTEGER;
    #       v_degree : INTEGER;
    #       control_points_list : LIST [2:?] OF LIST [2:?] OF cartesian_point;
    #       surface_form : b_spline_surface_form;
    #       u_closed : LOGICAL;
    #       v_closed : LOGICAL;
    #       self_intersect : LOGICAL;

    if AFS.get('ParsedSurface') is not None:
        ParsedSurface = AFS['ParsedSurface']
        surfaceUdegree = ParsedSurface['surfaceUdegree']
        surfaceVdegree = ParsedSurface['surfaceVdegree']
        controlPointsRefs = ParsedSurface['controlPointsRefs']
        surfaceForm = ParsedSurface['surfaceForm']
        closedU = ParsedSurface['closedU']
        closedV = ParsedSurface['closedV']
        selfIntersect = ParsedSurface['selfIntersect']
        knotUmultiplicities = ParsedSurface['knotUmultiplicities']
        knotVmultiplicities = ParsedSurface['knotVmultiplicities']
        knotsU = ParsedSurface['knotsU']
        knotsV = ParsedSurface['knotsV']
        knotSpec = ParsedSurface['knotSpec']

        controlPoints = ParsedSurface['controlPoints']
        STEPknotUvector = ParsedSurface['STEPknotUvector']
        STEPknotVvector = ParsedSurface['STEPknotVvector']
        BsplineKnotSurface = ParsedSurface['BsplineKnotCurve']

    else:
        if type(AFS['SurfaceParams'][0]) == str:  # check for name string
            offset = 1
        else:
            offset = 0

        surfaceUdegree = AFS['SurfaceParams'][offset]
        surfaceVdegree = AFS['SurfaceParams'][offset + 1]
        controlPointsRefs = AFS['SurfaceParams'][offset + 2] # need to flatten
        surfaceForm = AFS['SurfaceParams'][offset + 3]
        closedU = 'T' in AFS['SurfaceParams'][offset + 4]
        closedV = 'T' in AFS['SurfaceParams'][offset + 5]
        selfIntersect = 'T' in AFS['SurfaceParams'][offset + 6]
        knotUmultiplicities = AFS['SurfaceParams'][offset + 7]
        knotVmultiplicities = AFS['SurfaceParams'][offset + 8]
        knotsU = AFS['SurfaceParams'][offset + 9]
        knotsV = AFS['SurfaceParams'][offset + 10]
        knotSpec = AFS['SurfaceParams'][offset + 11]

        ParsedSurface = {}
        ParsedSurface['pointFeature'] = {}
        ParsedSurface['pointFeature']['xyz'] = []
        ParsedSurface['pointFeature']['centroidDisp'] = []
        ParsedSurface['pointFeature']['maxima'] = []
        ParsedSurface['pointFeature']['minima'] = []

        ParsedSurface['surfaceUdegree'] = surfaceUdegree
        ParsedSurface['surfaceVdegree'] = surfaceVdegree
        ParsedSurface['controlPointsRefs'] = controlPointsRefs
        ParsedSurface['surfaceForm'] = surfaceForm
        ParsedSurface['closedU'] = closedU
        ParsedSurface['closedV'] = closedV
        ParsedSurface['selfIntersect'] = selfIntersect
        ParsedSurface['knotUmultiplicities'] = knotUmultiplicities
        ParsedSurface['knotVmultiplicities'] = knotVmultiplicities
        ParsedSurface['knotsU'] = knotsU
        ParsedSurface['knotsV'] = knotsV
        ParsedSurface['knotSpec'] = knotSpec

        # controPointsListRefs structured as U control points length list of V control points length list

        controlPointsLenU = len(controlPointsRefs)
        controlPointsLenV = len(controlPointsRefs[0])
        #controlPointsRefs = cleanSubRefs(controlPointsRefs)

        # extract control points
        # controlPoints = []
        # for cplU in controlPointsRefs:
        #     cpU = []
        #     for cplV in cplU:
        #         if (STEP_entities_[ref2index(cplV)].type_name == 'CARTESIAN_POINT'):
        #             cpV = CP2point(STEP_entities_, cleanSubRefs(cplV)[0])
        #             cpU.append(cpV)
        #     controlPoints.append(cpU)

        controlPoints = []
        for cplU in controlPointsRefs:
            for cplV in cplU:
                if (STEP_entities_[ref2index(cplV)].type_name == 'CARTESIAN_POINT'):
                    controlPoints.append(CP2point(STEP_entities_, cleanSubRefs(cplV)[0]))

        ParsedSurface['controlPoints'] = controlPoints

        # construct full U & V knotvectors
        STEPknotUvector = []
        for kml in range(len(knotUmultiplicities)):
            for i in range(knotUmultiplicities[kml]):
                STEPknotUvector.append(knotsU[kml])
        ParsedSurface['STEPknotUvector'] = STEPknotUvector

        if (len(STEPknotUvector) - len(controlPoints) - surfaceUdegree) != 1:
            print("maldefined B-spline surface (U) !")

        # p.106 https://quaoar.su/files/standards/Standard%20-%202003%20-%20ISO%2010303-42.pdf

        STEPknotVvector = []
        for kml in range(len(knotVmultiplicities)):
            for i in range(knotVmultiplicities[kml]):
                STEPknotVvector.append(knotsV[kml])
        ParsedSurface['STEPknotVvector'] = STEPknotVvector

        # note _knotXvector values not used as Piegel+Tiller algorithms don't always work with explicit knots

        if (len(STEPknotVvector) - len(controlPoints) - surfaceVdegree) != 1:
            print("maldefined B-spline surface (V) !")

        #BsplineKnotSurface = BSpline.Surface(normalize_kv=True)
        BsplineKnotSurface = NURBS.Surface()
        BsplineKnotSurface.degree_u = surfaceUdegree
        BsplineKnotSurface.degree_v = surfaceVdegree

        BsplineKnotSurface.ctrlpts_size_u = controlPointsLenU
        BsplineKnotSurface.ctrlpts_size_v = controlPointsLenV

        ctrlptsw = compatibility.combine_ctrlpts_weights(controlPoints, weights=None)
        BsplineKnotSurface.set_ctrlpts(ctrlptsw, controlPointsLenU, controlPointsLenV)

        #BsplineKnotSurface.ctrlpts2d = controlPoints
        #BsplineKnotSurface.knotvector_u = STEPknotUvector
        #BsplineKnotSurface.knotvector_v = STEPknotVvector

        BsplineKnotSurface.knotvector_u = utilities.generate_knot_vector(surfaceUdegree,
                                                                         controlPointsLenU)
        BsplineKnotSurface.knotvector_v = utilities.generate_knot_vector(surfaceVdegree,
                                                                         controlPointsLenV)

        ParsedSurface['BsplineKnotCurve'] = BsplineKnotSurface

        # No compelling reason to set evaluation delta & evaluate surface points as evaluation seems incorrect
        # BsplineKnotSurface.delta = 0.025  # this seems to be the minima delta under adaptive tesselation
        # BsplineKnotSurface.evaluate()

    if not minimaExtrema:
        AFS['ParsedSurface'] = ParsedSurface
        # return ParsedSurface
        return

    # local minima/maxima
    maxPointsUV = rationalSurfaceExtremaParam_5(BsplineKnotSurface,
                                                 localCentroid,
                                                 maxSearch=True,
                                                 localExtrema=True,
                                                 curvatureTest=False,
                                                 uv_xyz=True)

    maxPoints = BsplineKnotSurface.evaluate_list(maxPointsUV)

    minPointsUV = rationalSurfaceExtremaParam_5(BsplineKnotSurface,
                                              localCentroid,
                                              maxSearch=False,
                                              localExtrema=True,
                                              curvatureTest=False,
                                              uv_xyz=True)

    minPoints = BsplineKnotSurface.evaluate_list(minPointsUV)

    # normalise u,v to [0, 1] - not required with geomdl generated knot vectors
    #knotUrange = STEPknotUvector[-1] - STEPknotUvector[0]
    #knotVrange = STEPknotVvector[-1] - STEPknotVvector[0]
    #maxPointsUV = [((mpu[0] - STEPknotUvector[0])/knotUrange, (mpu[1] - STEPknotVvector[0])/knotVrange ) for mpu in maxPointsUV]
    #minPointsUV = [((mpu[0] - STEPknotUvector[0])/knotUrange, (mpu[1] - STEPknotVvector[0])/knotVrange ) for mpu in minPointsUV]

    maxima = [True, ] * len(maxPoints) + [False, ] * len(minPoints)
    minima = [False, ] * len(maxPoints) + [True, ] * len(minPoints)

    maximaUV = np.array(maxPointsUV + minPointsUV)
    maximaPoints = np.array(maxPoints + minPoints)
    if len(maximaPoints) > 1:

        # create explicit dtype fields to permit sorting via u, then v
        maximaUV_field = maximaUV.ravel().view(dtype=[('u', maximaUV.dtype), ('v', maximaUV.dtype)])
        extremaUVindex = np.argsort(maximaUV_field, order=('u', 'v'))
        maximaUV = [maximaUV[euvi] for euvi in extremaUVindex]
        maximaPoints = [maximaPoints[euvi] for euvi in extremaUVindex]
        maxima = [maxima[euvi] for euvi in extremaUVindex]
        minima = [minima[euvi] for euvi in extremaUVindex]

        # because Newton-Raphson will converge on a tangent, separate max/min values may be identical
        maxima_truth = [np.allclose(maximaUV[mu + 1], maximaUV[mu], eps_STEP_AP21) for mu in range(0, len(maximaUV)-1)]
        maxima_truth = maxima_truth + [False,]
        maximaUV = [mu for (mu, mt) in zip(maximaUV, maxima_truth) if not mt]
        maximaPoints = [mp for (mp, mt) in zip(maximaPoints, maxima_truth) if not mt]

        # if vertex1 centroidDisp is close to u[0],
        # or vertex2 centroidDisp is close to u[-1],
        # remove u[0], u[-1]

        # centroidDisp = [np.linalg.norm(mp - localCentroid) for mp in maximaPoints]
        # vertex1centroidDisp = np.linalg.norm(ParsedEdge['vertex1'] - localCentroid)
        # vertex2centroidDisp = np.linalg.norm(ParsedEdge['vertex2'] - localCentroid)
    #     if np.abs(vertex1centroidDisp - centroidDisp[0]) < eps_STEP_AP21:
    #         maximaU.pop(0)
    #         maximaPoints.pop(0)
    #     else:
    #         centroidDisp.insert(0, vertex1centroidDisp)
    #     if np.abs(vertex2centroidDisp - centroidDisp[-1]) < eps_STEP_AP21:
    #         maximaU.pop(-1)
    #         maximaPoints.pop(-1)
    #     else:
    #         centroidDisp.append(vertex2centroidDisp)
    #
    # #maximaPoints.insert(0, ParsedEdge['vertex1']) # ParsedEdge['pointFeature']['xyz'][0])
    # #maximaPoints.insert(-1, ParsedEdge['vertex2']) # ParsedEdge['pointFeature']['xyz'][-1])
    # ParsedEdge['pointFeature']['xyz'] = [ParsedEdge['vertex1'],] + maximaPoints + [ParsedEdge['vertex2'],]
    # ParsedEdge['pointFeature']['u'] = [0,] + maximaU + [1]
    # ParsedEdge['pointFeature']['centroidDisp'] = centroidDisp


    ParsedSurface['pointFeature']['xyz'] = maximaPoints
    ParsedSurface['pointFeature']['uv'] = maximaUV
    ParsedSurface['pointFeature']['centroidDisp'] = [np.linalg.norm(mp - localCentroid) for mp in maximaPoints]
    ParsedSurface['pointFeature']['maxima'] = maxima
    ParsedSurface['pointFeature']['minima'] = minima

    # detect rotationally symmetric spline surfaces, spline surface cylinder
    # get the median point of rings/arcs of control points
    spinePoints = np.array([np.array(s).mean(axis=0) for s in BsplineKnotSurface.ctrlpts2d])
    # test for deviation from mean point
    rowDelta = []
    for s in range(0, len(spinePoints) - 1):
        ctrlptsRowDisp = [np.linalg.norm(spinePoints[s] - sc) for sc in BsplineKnotSurface.ctrlpts2d[s]]
        rowDelta.append(max(ctrlptsRowDisp) - min(ctrlptsRowDisp))

    # low rowDelta values suggest rotational symmetry,
    # similar rowDelta values suggest consistent cross-section (with possible helicity):
    # consistentCrossSection = np.array(rowDelta).var()
    # should show up with edge maxima/minima

    #spinePointDisps = [np.linalg.norm(spinePoints[s] - BsplineKnotSurface.ctrlpts2d[s]) for s in range(0, len(BsplineKnotSurface.ctrlpts2d))]
    spinePointsMean = spinePoints.mean(axis=0)
    uu, dd, vv = np.linalg.svd(spinePoints - spinePointsMean)

    # vv[0] contains the first principal component, i.e. the direction
    # vector of the 'best fit' line in the least squares sense.
    # project mean control point rows to derived PCA spine and assess spine axis straightness
    spineDir = vv[0] - spinePointsMean
    spinePointDir = [s - spinePointsMean for s in spinePoints]
    spineIP = np.dot(spineDir, spineDir)
    projSpinePointDir = [np.dot(s, spineDir) / spineIP for s in spinePointDir]

    projSpinePoint = [spinePointsMean + pspd * spineDir for pspd in projSpinePointDir]
    # test for centroid disp from spine axis, giving minima rotation

    spineDeviations = [np.linalg.norm(projSpinePoint[n] - spinePoints[n]) for n in range(0, len(spinePoints - 1))]

    if (max(rowDelta) < eps_STEP_AP21) and (max(spineDeviations) < eps_STEP_AP21): # rotSymLimit constant --------------------------------------------
        #ParsedSurface['rotSymFlag'] = 1
        ParsedSurface['rotSymFeature'] = dict()
        #ParsedSurface['rotSymFeature']['rotSymCentre'] = ?? centroid minima within
        ParsedSurface['rotSymFeature']['rotSymRadius'] = spinePointsMean

    AFS['ParsedSurface'] = ParsedSurface


def planeSurfaceParse_2(AFS, localCentroid, minimaExtrema=False):
    # ENTITY Plane
    #   SUBTYPE OF (Surface);
    #   position : Axis_placement_3d;

    # with a planar surface, closest point is that point closest to orthogonal intersection of ray through
    # localCentroid with plane of surface

    if AFS.get('ParsedSurface') is not None: # recalculation of max/min
        ParsedSurface = AFS['ParsedSurface']
        axisPoint = ParsedSurface['axisPoint']
        normDir = ParsedSurface['normDir'] # Z
        #refDir = ParsedSurface['refDir'] # X
        #auxDir = ParsedSurface['auxDir']  # Y

    else:
        ParsedSurface = {}
        axisPoint, normDir, refDir = axis2Placement3D_2(AFS['SurfaceParams'][1], STEP_entities)
        ParsedSurface['axisPoint'] = axisPoint
        ParsedSurface['normDir'] = normDir  # Z
        ParsedSurface['refDir'] = refDir  # X

        ParsedSurface['pointFeature'] = {}
        ParsedSurface['pointFeature']['xyz'] = []
        ParsedSurface['pointFeature']['centroidDisp'] = []
        ParsedSurface['pointFeature']['maxima'] = []
        ParsedSurface['pointFeature']['minima'] = []

    if not minimaExtrema:
        AFS['ParsedSurface'] = ParsedSurface
        # return ParsedSurface
        return

    # project localCentroid to plane defined by normDir and axisPoint
    surfaceMinPoint = localCentroid - np.dot(localCentroid - axisPoint, normDir) * normDir

    if insideOutsideSurfaceTest(localCentroid, surfaceMinPoint, AFS):  # minima is among edges
        # replace not append on every calculation
        ParsedSurface['pointFeature']['xyz'] = [surfaceMinPoint]
        ParsedSurface['pointFeature']['centroidDisp'] = [np.linalg.norm(surfaceMinPoint - localCentroid)]
        ParsedSurface['pointFeature']['maxima'] = [False,]
        ParsedSurface['pointFeature']['minima'] = [True,]

    AFS['ParsedSurface'] = ParsedSurface


def surfaceParse(AFS, c, STEP_entities, minimaExtrema=True):
    if AFS['SurfaceTypeName'] not in [
        'SPHERICAL_SURFACE',
        'TOROIDAL_SURFACE',
        'CYLINDRICAL_SURFACE',
        'CONICAL_SURFACE',
        'PLANE',
        'B_SPLINE_SURFACE_WITH_KNOTS'
    ]:
        print("Untreated surface type: " + AFS['SurfaceTypeName'])
    else:
        if AFS['SurfaceTypeName'] == 'SPHERICAL_SURFACE':
            sphereSurfaceParse(AFS, c, minimaExtrema)

        if AFS['SurfaceTypeName'] == 'TOROIDAL_SURFACE':
            toroidSurfaceParse(AFS, c, minimaExtrema)

        if (AFS['SurfaceTypeName'] == 'CYLINDRICAL_SURFACE'):
            cylinderSurfaceParse(AFS, c, minimaExtrema)

        if (AFS['SurfaceTypeName'] == 'CONICAL_SURFACE'): # still must test with simple STEP file
            coneSurfaceParse(AFS, c, minimaExtrema)

        if AFS['SurfaceTypeName'] == 'PLANE':
            planeSurfaceParse_2(AFS, c, minimaExtrema)

        if AFS['SurfaceTypeName'] == 'B_SPLINE_SURFACE_WITH_KNOTS':
            BSplineSurfaceWithKnotsParse(AFS, c, STEP_entities, minimaExtrema)


AdvancedFaceSurfaces = []
for se in STEP_entities:
    if hasattr(se, 'type_name'):  # stepcode.Part21.ComplexEntity type missing attribute
        if (se.type_name == 'ADVANCED_FACE') or (se.type_name == 'FACE_SURFACE'):
            SurfaceNormalOutwards = 'T' in se.params[-1]
            afRefs = cleanSubRefs(se.params)
            SurfaceClass = {
                'SurfaceNormalOutwards': SurfaceNormalOutwards,
                'SurfaceTypeName': None,
                'SurfaceRef': None,
                'SurfaceParams': None,
                'EdgeLoopList': [],
            }
            for se2 in afRefs:
                if hasattr(STEP_entities[ref2index(se2)], 'type_name'): # should prolly assume [0] is FACE_*-----------------<<<<<<
                    afTypeName = STEP_entities[ref2index(se2)].type_name
                    if (afTypeName == 'FACE_OUTER_BOUND') or (afTypeName == 'FACE_BOUND'):  # no risk of complex entities from here on in
                        # todo assume 'FACE_INNER_BOUND' or 'HOLE' etc can run the same edge loop from here ----------------------<<<<<<
                        se3 = STEP_entities[ref2index(se2)].params
                        se3 = cleanSubRefs(se3)[0]
                        if STEP_entities[ref2index(se3)].type_name == 'EDGE_LOOP':
                            SurfaceClass['EdgeLoopList'] = STEP_entities[ref2index(se3)].params[-1]
                    else:
                        # print(afTypeName)
                        SurfaceClass['SurfaceTypeName'] = afTypeName
                        SurfaceClass['SurfaceRef'] = se2
                        SurfaceClass['SurfaceParams'] = STEP_entities[
                            ref2index(se2)
                        ].params

                elif not hasattr(STEP_entities[ref2index(se2)], 'type_name'):
                    print('complex entity @todo')
            AdvancedFaceSurfaces.append(SurfaceClass)

# axis: the Direction that defines the second axis of the Axis_placement. The value of this attribute need not be specified.
# ref_direction: the direction used to determine the direction of the local X axis.
# The value of this attribute need not be specified. If axis or ref_direction is omitted, these directions are taken from the geometric coordinate system

# orientation: a BOOLEAN flag. If TRUE, the topological orientation as used coincides with the orientation,
# from start vertex to end vertex, of the edge_definition, if FALSE the vertices are reversed in order.
# edge_start: the start vertex of the Oriented_edge. This is derived from the vertices of the edge_definition after taking account of the orientation.
# edge_end: the end vertex of the Oriented_edge. This is derived from the vertices of the edge_definition after taking account of the orientation.

# extract edge data and convert into useful format

#
#10 = ADVANCED_BREP_SHAPE_REPRESENTATION('',(#11,#15),#113);
#11 = AXIS2_PLACEMENT_3D('',#12,#13,#14);
#12 = CARTESIAN_POINT('',(0.,0.,0.));
#13 = DIRECTION('',(0.,0.,1.));
#14 = DIRECTION('',(1.,0.,-0.));

outermostPoints = []
# need to exclude Advanced_brep_shape_representation which seems to contain a [0,0,0] origin point
ABSRpointRef = None
for ABSR in STEP_entities:
    if hasattr(ABSR, 'type_name'):  # stepcode.Part21.ComplexEntity type missing attribute
        if ABSR.type_name == 'ADVANCED_BREP_SHAPE_REPRESENTATION':
            # get
            cleanABSR = cleanSubRefs(ABSR.params)
            if STEP_entities[ref2index(cleanABSR[0])].type_name == 'AXIS2_PLACEMENT_3D':
                ABSRpointRef = cleanSubRefs(STEP_entities[ref2index(cleanABSR[0])].params)[0]
                #_1=1


    for vp in STEP_entities:
        if hasattr(vp, 'type_name'):  # stepcode.Part21.ComplexEntity type missing attribute
            if vp.type_name == 'CARTESIAN_POINT':
                if (len(vp.params[-1]) == 3):
                    if (vp.ref == ABSRpointRef):
                        pass
                    else:
                        outermostPoints.append(np.array([vp.params[-1][0], vp.params[-1][1], vp.params[-1][2]]))



            #
#
centroid = medianPoint(outermostPoints)
# continue to ignore local maxima at both radial extents and NURB surface maxima?
# for all elements, surfaces and bounding edges, iterate maxima discovery relative to centroid

for AFS in AdvancedFaceSurfaces:
    EdgeLoops = AFS['EdgeLoopList']
    # testing a specific edge loop instance 44CONE 0SPLINE
    # EdgeLoops = AdvancedFaceSurfaces[0]['EdgeLoopList']

    ParsedEdgeLoop = []
    for edgeRefInstance in EdgeLoops:

        ParsedEdge = edgeSTEPparse(edgeRefInstance, STEP_entities)

        edgeParse(centroid, ParsedEdge, STEP_entities)
        ParsedEdgeLoop.append(ParsedEdge)

        # if ParsedEdge['vertex1ref'] == ParsedEdge['vertex2ref']:
        #     _1=1

        if ParsedEdge['typeName'] in ['SEAM_CURVE', 'SURFACE_CURVE']:
            #   SEAM_CURVE (not sure if this is worthwhile retaining)
            #   associated geometry in following list does not seem to be defined elsewhere

            #   Attribute	                Type	                                        Defined By
            #   name	                    label (STRING)	                                representation_item
            #   curve_3d	                curve (ENTITY)	                                surface_curve
            #   associated_geometry	        LIST OF pcurve_or_surface (SELECT)	            surface_curve
            #   master_representation	    preferred_surface_curve_representation (ENUM)	surface_curve

            # todo: just create a superTypeName flag?

            cleanEdgeType = cleanSubRefs(ParsedEdge['edgeType'])
            edgeTypeName = STEP_entities[ref2index(cleanEdgeType[0])].type_name
            ParsedEdge['superTypeName'] = ParsedEdge['typeName']

            edgeRef = STEP_entities[ref2index(cleanEdgeType[0])].ref

            # if edgeRef == '#16':
            #     _1=1

            ParsedEdge['superEdgeRef'] = ParsedEdge['edgeRef']
            ParsedEdge['edgeRef'] = edgeRef
            ParsedEdge['superEdgeType'] = ParsedEdge['edgeType']
            ParsedEdge['edgeType'] = STEP_entities[ref2index(cleanEdgeType[0])].params # todo: change edgeParams
            ParsedEdge['superTypeName'] = ParsedEdge['typeName']
            ParsedEdge['typeName'] = STEP_entities[ref2index(edgeRef)].type_name
            edgeParse(centroid, ParsedEdge, STEP_entities)

            if ParsedEdge['edgeRef'] not in [pel['edgeRef'] for pel in ParsedEdgeLoop]:
                ParsedEdgeLoop.append(ParsedEdge)

            for cet in cleanEdgeType[1:]: # get associated surfaces, LIST OF pcurve_or_surface
                if STEP_entities[ref2index(cet)].type_name == 'PCURVE':
                    # extract surface data from second parameter term
                    surfaceRef = STEP_entities[ref2index(cet)].params[1]

                    # todo:  shouldn't recalculate on every pass?
                    # todo:  create function collateField(surface?) to .. dict data

                    # surfaceRefCollection = []
                    # for AFS2 in AdvancedFaceSurfaces:
                    #     surfaceRefCollection.append(AFS2['SurfaceRef'])
                    # if surfaceRef not in surfaceRefCollection:
                    if surfaceRef not in [AFS2['SurfaceRef'] for AFS2 in AdvancedFaceSurfaces]:
                        _1=1
                        print('undetected surface! PCURVE')
                else:
                    # todo: code for other surface option missing
                    print('undetermined surfacetype failure line2824')
                    _1 = 1

    AFS['outerBoundEdgeLoop'] = ParsedEdgeLoop
    # doesn't seem much point in parsing surface minima until centroid is stable
    surfaceParse(AFS, centroid, STEP_entities)

    # The standard CSG primitives are the cone, eccentric_cone, cylinder, sphere, torus,
    # block, right_angular_wedge, ellipsoid, tetrahedron and pyramid.

    #        b_spline_surface,
    #        b_spline_surface_with_knots,
    #        bezier_surface,
    #        conical_surface,
    #        curve_bounded_surface,
    #        cylindrical_surface,
    #        degenerate_toroidal_surface,
    #        offset_surface,
    #        quasi_uniform_surface,
    #        rational_b_spline_surface,
    #        rectangular_composite_surface,
    #        rectangular_trimmed_surface,
    #        spherical_surface,
    #        surface,
    #        surface_of_linear_extrusion,
    #        surface_of_revolution,
    #        surface_replica,
    #        swept_surface,
    #        toroidal_surface,
    #        uniform_surface,

# iteratively calculate max/min points and median centroid point
# extract max values for all surfaces to add to median vertex calculation for centroid

def medianOfFeaturePoints(Surfaces):
    """get barycentre of all AFS feature points"""

    outermostPoints = []

    for AFS in Surfaces:
        if AFS['ParsedSurface'].get('pointFeature') is not None:
            if AFS['ParsedSurface']['pointFeature'].get('xyz') is not None:
                [outermostPoints.append(mp) for mp in AFS['ParsedSurface']['pointFeature']['xyz']]
        # if AFS['ParsedSurface'].get('axisPoint') is not None:
        #     outermostPoints.append(AFS['ParsedSurface']['axisPoint'])
            # print('AFS[ParsedSurface][axisPoint]')
            # print(AFS['ParsedSurface']['axisPoint'])
        for edge in AFS['outerBoundEdgeLoop']:
            if edge.get('pointFeature') is not None:
                if edge['pointFeature'].get('xyz') is not None:
                    [outermostPoints.append(mp) for mp in edge['pointFeature']['xyz']]
            # if edge.get('axisPoint') is not None:
            #     outermostPoints.append(edge['axisPoint'])
                # print('axisPoint')
                # print(edge['axisPoint'])

    return medianPoint(outermostPoints)

lastCentroid = centroid
centroid = medianOfFeaturePoints(AdvancedFaceSurfaces)

# iterate centroid-features relationship
while (np.linalg.norm(lastCentroid - centroid) > eps):
    # recalculate surfaces & edges max/min points according to new centroid
    for AFS in AdvancedFaceSurfaces:
        # update surface after edges
        for edge in AFS['outerBoundEdgeLoop']:
            edgeParse(centroid, edge, STEP_entities)
        # doesn't seem much point in parsing surface minima until centroid is stable, possible exception of bspline maxima, minimaFlag ?
        surfaceParse(AFS, centroid, STEP_entities, minimaExtrema=False)

    lastCentroid = centroid
    centroid = medianOfFeaturePoints(AdvancedFaceSurfaces)
    # for AFS in AdvancedFaceSurfaces:
    #     if AFS['ParsedSurface']['maxPoint'] is not None:
    #         outermostPoints.append(AFS['ParsedSurface']['maxPoint'])
    #         for edge in AFS['outerBoundEdgeLoop']:
    #             outermostPoints.append(edge['maxPoint'])
    #
    # centroid = medianPoint(outermostPoints)
    print('centroid update: ' + str(np.linalg.norm(lastCentroid - centroid)))
    print('centroid: ' + str(centroid))

surfaceParse(AFS, centroid, STEP_entities, minimaExtrema=True)

# todo: search for all edges & surfaces related to point features (maybe symmetrical arcs too) get nearest points
# to originating points
# note that one can expect C2 continuity in STEP format, meaning that two point maxima will not be adjacent except
# where the surface or edge is of a fixed radius from the local centroid.


def FreeCADpointSyntax(A, color=(0., 0., 0.)):
    '''print the instructions to create a FreeCAD model of a list of points'''
    print('import FreeCAD as App')
    print('import Draft')
    for i, xyz in enumerate(A):
        print('P{:} = Draft.make_point( {:1.8f}, {:1.8f}, {:1.8f}, color=({:1.8f}, {:1.8f}, {:1.8f}))'.format(i, xyz[0], xyz[1], xyz[2], color[0], color[1], color[2]))
    print('App.ActiveDocument.recompute()')
    #print('setview()')


def parseAdjacentFeatures(AFS): #getNearestFeatures(sourceRef, parsedFaceSet):
    """ given a STEP reference associated with a feature, find all associated objects (edges & surfaces) and return  """
    # seems there should be sets of associated references with every adjacent feature??
    # only vertex points have associated Ref

    # create structures that only represent adjacent features-
    # max/min @ vertex: return edges w/ second vertex for all edge set, surfaces that touch vertex
    # max/min @ edge: return vertex1 & 2 plus touching surfaces
    # max/min @ surface: return surface & its edges & vertices

    # if bspline surface won't return a maxima at an edge, is this required
    # # create set of edges to every surface
    # for afs in AFS:
    #     afs['ParsedSurface']['surfaceEdgeIndex'] = []
    #     for se in afs['outerBoundEdgeLoop']:
    #         afs['ParsedSurface']['surfaceEdgeIndex'].append(se['edgeRef'])

    # # create set of surface to every edges
    # for afs in AFS:
    #     for se in afs['outerBoundEdgeLoop']:
    #         se['edgeAdjSurfacesIndex'] = {}
    #         for afs2_index, afs2 in enumerate(AFS):
    #             for te in afs2['outerBoundEdgeLoop']:
    #                 if se['edgeRef'] == te['edgeRef']:
    #                     if afs2['SurfaceRef'] not in se['edgeAdjSurfacesIndex'].keys():
    #                         se['edgeAdjSurfacesIndex'][afs2['SurfaceRef']] = afs2_index

    for afs_index, afs in enumerate(AFS):
        for se_index, se in enumerate(afs['outerBoundEdgeLoop']):
            for sRef in ['vertex1ref', 'vertex2ref']:
                if se.get(sRef):
                    ref1 = se[sRef]
                    #indexDictName = sRef + '_' + ref1 + '_featuresIndex'
                    indexDictName = sRef + '_surroundIndex'
                    se[indexDictName] = {}
                    #se[indexDictName][ref1] = (afs_index, se_index)================================================================
                    for afs2_index, afs2 in enumerate(AFS):
                        for te_index, te in enumerate(afs2['outerBoundEdgeLoop']):
                            te_strings = [et for et in te.values() if type(et) == str]
                            if ref1 in te_strings:  #
                                # add adjacent surfaces unnecessary as these are the first field of feature mapping
                                #for tRef in ['vertex1ref', 'vertex2ref']:#, 'edgeRef']:
                                for tRef in ['edgeRef']:
                                    if te.get(tRef):
                                        ref2 = te[tRef]
                                        if (ref2 != ref1):  # and ref2 not in edgeSource[indexDictName].keys():
                                            se[indexDictName][ref2] = (afs2_index, te_index)



    # def mapFeatureRefs(sourceFeatureRefs, targetFeatureRefs, AFS):
    #     for afs in AFS:
    #         for edgeSource in afs['outerBoundEdgeLoop']:
    #             for sourceRef in sourceFeatureRefs:
    #                 if edgeSource.get(sourceRef):
    #                     ref1 = edgeSource[sourceRef]
    #                     indexDictName = sourceRef + '_' + ref1 + '_featuresIndex'
    #                     edgeSource[indexDictName] = {}
    #                     for afs2_index, afs2 in enumerate(AFS):
    #                         for edgeTargetIndex, edgeTarget in enumerate(afs2['outerBoundEdgeLoop']):
    #                             edgeTargetStrings = [et for et in edgeTarget.values() if type(et) == str]
    #                             if ref1 in edgeTargetStrings: #
    #                                 # add adjacent surfaces unnecessary as these are teh first field of feature mapping
    #                                 for targetRef in targetFeatureRefs:
    #                                     if edgeTarget.get(targetRef):
    #                                         ref2 = edgeTarget[targetRef]
    #                                         if (ref2 != ref1):# and ref2 not in edgeSource[indexDictName].keys():
    #                                                 edgeSource[indexDictName][ref2] = (afs2_index, edgeTargetIndex)

    #mapFeatureRefs(['vertex1ref', 'vertex2ref', ], ['vertex1ref', 'vertex2ref', 'edgeRef'], AFS)
    #mapFeatureRefs(['vertex1ref','vertex2ref',],['vertex1ref','vertex2ref','edgeRef'], AFS)
    #mapFeatureRefs(['vertex2ref',], ['edgeRef'], AFS)
    #mapFeatureRefs(['vertex1ref', ], ['vertex2ref'], AFS)
    #pass

    # instead of all purpose function, need to find sets of edges at a common vertex
    # (alongside set of vertices on all edges common to any one vertex)
    # sets of surfaces at a common vertex (do not exclude common ref (s, e) in search)
    # intersection of two surface sets to find common surfaces adjacent to common edge


parseAdjacentFeatures(AdvancedFaceSurfaces)


# rank rotationally symmetric edges as grooves/spherical/ridges based on adjoining surfaces
# namely for the 2 surfaces adjoining an arc/circle, measure the displacement of the surface relative to this axis of rotation
# project each point on adjoining surfaces to rotational axis to determine average displacement
# Limit to adjoining surfaces to determine the extent of relevant points and edges on each side
# (not check edges completely enclose rotational axis)
# average the displacement to this axis of vertices & edges

# test models: Maltese Cruciform
# import FreeCAD as App
# import Part
# v = App.Vector
# doc = App.newDocument()
#
# p0 = v(0,0,-20)
# p1 = v(10,0,-20)
# p2 = v(8,0,-10)
# p3 = v(5,0,0)
# p4 = v(8,0,10)
# p5 = v(10,0,20)
# p6 = v(0,0,20)
#
# L0=Part.makePolygon([p0, p1, p2, p3, p4, p5, p6])
# S0=L0.revolve(App.Vector(0,0,0),App.Vector(0,0,1),360)
# # Part.show(S0, "Surface")
#
# p7 = v(-20,0,0)
# p8 = v(-20,10,0)
# p9 = v(-10,8,0)
# p10 = v(0,5,0)
# p11 = v(10,8,0)
# p12 = v(20,10,0)
# p13 = v(20,0,0)
#
# L1=Part.makePolygon([p7, p8, p9, p10, p11, p12, p13])
# S1=L1.revolve(App.Vector(0,0,0),App.Vector(1,0,0),360)
# # Part.show(S1, "Surface")
#
# fusedX = S0.fuse(S1)
# Part.show(fusedX, "Surface")

# rather than searching for all features around a rotSym feature, restrict to rotSym features?
# recall that distinction between geometric surfaces is determinined by the most simple shapes permissible.
# note that features must be tested to determine they are on the same axis.
# multiple data sets created for multiple axes through centroid (e.g. sea-urchin shape, or throwing jacks)


#import pickle
# p_file = open('AFS_cruciform.pkl', 'wb')
# pickle.dump(AdvancedFaceSurfaces, p_file)
# p_file.close()

# f = open('AFS_cruciform.pkl','rb')
# AdvancedFaceSurfaces = pickle.load(f)


def getRotSymFeatures(AFS):
    '''
    order rotational features to return unique rotSym features
    '''

    allRotSymFeatures = dict()
    allRotSymFeatures['rotSymCentre'] = []
    allRotSymFeatures['rotSymRadius'] = []
    allRotSymFeatures['normDir'] = []

    for AFSindex, AFS in enumerate(AdvancedFaceSurfaces):
        # get surface rotSymFeatures
        for edge in AFS['outerBoundEdgeLoop']:
            if (edge.get('rotSymFeature') is not None):
                if (edge['rotSymFeature'].get('rotSymCentre') is not None) and \
                        (edge['rotSymFeature'].get('rotSymRadius') is not None):
                    if type(edge['rotSymFeature']['rotSymCentre']) == list:
                        for rsf in range(0, len(edge['rotSymFeature']['rotSymCentre'])):
                            allRotSymFeatures['rotSymCentre'].append(edge['rotSymFeature']['rotSymCentre'][rsf])
                            allRotSymFeatures['rotSymRadius'].append(edge['rotSymFeature']['rotSymRadius'][rsf])
                            allRotSymFeatures['normDir'].append(edge['normDir'])
                    else:
                        allRotSymFeatures['rotSymCentre'].append(edge['rotSymFeature']['rotSymCentre'])
                        allRotSymFeatures['rotSymRadius'].append(edge['rotSymFeature']['rotSymRadius'])
                        allRotSymFeatures['normDir'].append(edge['normDir'])

        # get surface rotSymFeatures
        if (AFS['ParsedSurface'].get('rotSymFeature') is not None):
            if (AFS['ParsedSurface']['rotSymFeature'].get('rotSymCentre') is not None) and \
                    (AFS['ParsedSurface']['rotSymFeature'].get('rotSymRadius') is not None):
                if type(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre']) == list:
                    for rsf in range(0, len(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'])):
                        allRotSymFeatures['rotSymCentre'].append(
                            AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'][rsf])
                        allRotSymFeatures['rotSymRadius'].append(
                            AFS['ParsedSurface']['rotSymFeature']['rotSymRadius'][rsf])
                        allRotSymFeatures['normDir'].append(AFS['ParsedSurface']['normDir'])
                else:
                    allRotSymFeatures['rotSymCentre'].append(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'])
                    allRotSymFeatures['rotSymRadius'].append(AFS['ParsedSurface']['rotSymFeature']['rotSymRadius'])
                    allRotSymFeatures['normDir'].append(AFS['ParsedSurface']['normDir'])

    # get unique axis set (ignore reverse directions)
    if len(allRotSymFeatures['normDir']) < 1:
        return []

    normDirSet = [allRotSymFeatures['normDir'][0]]
    for arfsc in range(0, len(allRotSymFeatures['rotSymCentre'])):
        if not any([np.allclose(np.abs(allRotSymFeatures['normDir'][arfsc]), np.abs(nds), eps) for nds in normDirSet]):
            normDirSet.append(allRotSymFeatures['normDir'][arfsc])

    superAxisUniqueSet = []
    for ndc in normDirSet:
        axisUniqueSet = []
        for arfsc in range(0, len(allRotSymFeatures['rotSymCentre'])):
            if np.allclose(np.abs(allRotSymFeatures['normDir'][arfsc]), np.abs(ndc), eps):
                axisUniqueSet.append(arfsc)
        superAxisUniqueSet.append(axisUniqueSet)

    superAxesSet = []
    # need to find adjacent rotSym to establish maxima/minima
    # for sausIndex, saus in enumerate(superAxisUniqueSet):
    for saus in superAxisUniqueSet:
        axisSet = dict()
        # need to determine which rotSymCentre lie on the same side of centroid
        # 1. find point maxCentroidDisp with max disp from centroid
        # 2. sort all other points relative to maxDispCentroid

        # define centroid plane, normDir and centroid point, then find dot product
        # centreDisps = [pointProjectAxis(allRotSymFeatures['rotSymCentre'][s], centroid, normDirSet[sausIndex]) for s in saus]

        centreDisps = [np.linalg.norm(allRotSymFeatures['rotSymCentre'][s] - centroid) for s in saus]
        maxCentreDisps = saus[centreDisps.index(max(centreDisps))]
        axisDisps = [
            np.linalg.norm(allRotSymFeatures['rotSymCentre'][s] - allRotSymFeatures['rotSymCentre'][maxCentreDisps]) for
            s in saus]
        axisDispsSorted = np.argsort(axisDisps)

        saus = [saus[ads] for ads in axisDispsSorted]

        axisSet['centreCentroidDisp'] = [centreDisps[ads] for ads in axisDispsSorted]
        axisSet['rotSymCentre'] = [allRotSymFeatures['rotSymCentre'][s] for s in saus]
        axisSet['rotSymRadius'] = [allRotSymFeatures['rotSymRadius'][s] for s in saus]
        # check neighbours for max/min determination (equality := min)
        # furthest rotSym centre is always a maxima ? ----------------------------------------------------------
        superAxesSet.append(axisSet)

    superAxesSet2 = []
    # remove duplicates
    for sas in superAxesSet:
        axisSet2 = dict()
        axisSet2['rotSymCentre'] = [sas['rotSymCentre'][0], ]
        axisSet2['rotSymRadius'] = [sas['rotSymRadius'][0], ]
        axisSet2['centreCentroidDisp'] = [sas['centreCentroidDisp'][0]]
        for s in range(1, len(sas['rotSymCentre'])):
            if not (np.allclose(sas['rotSymCentre'][s], sas['rotSymCentre'][s - 1], eps) and \
                    np.isclose(sas['rotSymRadius'][s], sas['rotSymRadius'][s - 1], eps)):
                axisSet2['rotSymCentre'].append(sas['rotSymCentre'][s])
                axisSet2['rotSymRadius'].append(sas['rotSymRadius'][s])
                axisSet2['centreCentroidDisp'].append(sas['centreCentroidDisp'][s])
        superAxesSet2.append(axisSet2)

    superAxesSet = superAxesSet2

    # get individual rotSym max/min values from relative radii
    # can assume outlying rotSym features are maxima, as face minima are points - adopt as convention?
    for sas in superAxesSet:
        sas['maxima'] = [False, ] * len(sas['rotSymCentre'])
        sas['minima'] = [False, ] * len(sas['rotSymCentre'])
        sas['maxima'][0] = True
        # sas['minima'][0] = False
        for s in range(1, len(sas['rotSymCentre']) - 1):
            if (sas['rotSymRadius'][s] > sas['rotSymRadius'][s - 1]) and (
                    sas['rotSymRadius'][s] > sas['rotSymRadius'][s + 1]):
                sas['maxima'][s] = True
                sas['minima'][s] = False
            elif (sas['rotSymRadius'][s] <= sas['rotSymRadius'][s - 1]) and (
                    sas['rotSymRadius'][s] <= sas['rotSymRadius'][s + 1]):
                sas['maxima'][s] = False
                sas['minima'][s] = True
        sas['maxima'][-1] = True
        # sas['minima'][-1] = False

    return superAxesSet


# for every local minima or maxima feature point, find the surrounding local minima points by way of geometrical features
# (i.e. opposing minima on a thin plate don't count; adjacency is not calculated by cartesian distance but surface topography)

# create similar dict structure listing local minima/maxima tuple (index1, index2) key and index list of surrounding local maxima/minima
# ignore overall minima within a surface and edge boundaries

# surface maxima/minima may be associated with related edge maxima/minima (note B-spline surface may have several max/min)
# find the minima/maxima associated with every edge

rotSymFeatures = getRotSymFeatures(AdvancedFaceSurfaces)


surfaceMinPoint = []
surfaceMinFeatureCentroidDisp = []
# surfaceMinRotSym = []

surfaceMaxPoint = []
surfaceMaxFeatureCentroidDisp = []
# surfaceMaxRotSym = []

# surfaceSphericalRotSym = []

def getPointFeatures(AdvancedFaceSurface):
    '''
    order point features to return unique
    '''

    def vertexExtremaSearch(AdvancedFaceSurfaces, extremaType='maxima'):

        pointFeatures = dict()
        pointFeatures['xyz'] = []
        pointFeatures['centroidDisp'] = []
        pointFeatures['maxima'] = []
        pointFeatures['minima'] = []

        vertexX = ['vertex1', 'vertex2']
        vertexXref = ['vertex1ref', 'vertex2ref']
        vertexXref_surroundIndex = ['vertex1ref_surroundIndex', 'vertex2ref_surroundIndex']
        # extremaType = 'maxima'
        # extremaType = 'minima'
        extrema = dict()
        for v in range(0, 2):  # vertex1, vertex2
            for AFSindex, AFS in enumerate(AdvancedFaceSurfaces):
                for edge in AFS['outerBoundEdgeLoop']:
                    if (edge.get('superTypeNameFlag') is not None):
                        if edge['superTypeName'] == 'SEAM_CURVE':  # exclude SEAM_CURVE data
                            print("is this code reachable??")
                            break
                            # unnecessary?

                    elif not edge.get('pointFeature'):
                        # commence with searching local minima around edges associated with AFS surface
                        # for each edge, get 'pointFeatures' fields
                        print("missing pointFeature field")  # this should be checked elsewhere
                        break

                    elif ((v == 0) and (edge[vertexXref[v]] not in extrema.keys())) or (
                            (v == 1) and (edge[vertexXref[v]] not in extrema.keys())):
                        # get localMinima for all vertex and attached edges,
                        # retain extrema value in separate set, [surface]['outerBoundEdgeLoop'][edge]['pointFeature'][index]

                        # for each pointFeature element, get max/min
                        # for vertex1/2 find if relative max/min from adjacent pointFeature element - ignore surfaces for now

                        # u == 0, vertex1, get attached pointFeature
                        # surroundIndex should give address of adjacent edges
                        pfSurround = []
                        adjacentEdgeRefs = []
                        reversedOrder = []
                        for vseRef in edge[vertexXref_surroundIndex[v]].keys():
                            adjacentEdgeRefs.append(vseRef)
                            vse = edge[vertexXref_surroundIndex[v]][vseRef]
                            adjPF = AdvancedFaceSurfaces[vse[0]]['outerBoundEdgeLoop'][vse[1]]['pointFeature']
                            if AdvancedFaceSurfaces[vse[0]]['outerBoundEdgeLoop'][vse[1]][vertexXref[v]] == edge[vertexXref[v]]:
                                pfSurround.append(adjPF)
                                reversedOrder.append(False)
                            else:
                                reversePF = dict()
                                reversePF['u'] = adjPF['u'].copy()
                                reversePF['u'].reverse()
                                reversePF['xyz'] = adjPF['xyz'].copy()
                                reversePF['xyz'].reverse()
                                reversePF['centroidDisp'] = adjPF['centroidDisp'].copy()
                                reversePF['centroidDisp'].reverse()
                                pfSurround.append(reversePF)
                                reversedOrder.append(True)

                        # test vertexX with immediate neighbours
                        if extremaType == 'maxima':
                            if all([pfs['centroidDisp'][0] > pfs['centroidDisp'][1] + eps_STEP_AP21 for pfs in pfSurround]):
                                if not array3x1match(edge[vertexX[v]], pointFeatures['xyz']):
                                    pointFeatures['xyz'].append(edge[vertexX[v]])
                                    pointFeatures['centroidDisp'].append(pfSurround[0]['centroidDisp'][0])
                                    pointFeatures['maxima'].append(True)
                                    pointFeatures['minima'].append(False)

                        elif extremaType == 'minima':
                            if all([pfs['centroidDisp'][0] < pfs['centroidDisp'][1] - eps_STEP_AP21 for pfs in pfSurround]):  # local min
                                if not array3x1match(edge[vertexX[v]], pointFeatures['xyz']):
                                    pointFeatures['xyz'].append(edge[vertexX[v]])
                                    pointFeatures['centroidDisp'].append(pfSurround[0]['centroidDisp'][0])
                                    pointFeatures['maxima'].append(False)
                                    pointFeatures['minima'].append(True)

                        maxPFlen = max([len(pfs['u']) for pfs in pfSurround]) - 1
                        if maxPFlen > 1:
                            crawlindex = 1
                            while crawlindex < maxPFlen:
                                # should capture all local max/min before second vertex
                                for pfsIndex, pfs in enumerate(pfSurround):
                                    if crawlindex < len(pfs['centroidDisp']) - 1:  # 'centroidDisp' find local maxima/minima
                                        pfcd0 = pfs['centroidDisp'][crawlindex - 1]
                                        pfcd1 = pfs['centroidDisp'][crawlindex]
                                        pfcd2 = pfs['centroidDisp'][crawlindex + 1]

                                        if reversedOrder[pfsIndex]:
                                            u = len(pfs['u']) - crawlindex - 1  # CHECK
                                        else:
                                            u = crawlindex

                                        if extremaType == 'maxima':
                                            if (pfcd1 > pfcd0 + eps_STEP_AP21) and (pfcd1 > pfcd2 + eps_STEP_AP21):  # local max
                                                if not array3x1match(pfs['xyz'][u], pointFeatures['xyz']):
                                                    pointFeatures['xyz'].append(pfs['xyz'][u])
                                                    pointFeatures['centroidDisp'].append(pfcd1)
                                                    pointFeatures['maxima'].append(True)
                                                    pointFeatures['minima'].append(False)

                                        elif extremaType == 'minima':
                                            if (pfcd1 < pfcd0 - eps_STEP_AP21) and (pfcd1 < pfcd2 - eps_STEP_AP21):  # local min
                                                if not array3x1match(pfs['xyz'][u], pointFeatures['xyz']):
                                                    pointFeatures['xyz'].append(pfs['xyz'][u])
                                                    pointFeatures['centroidDisp'].append(pfcd1)
                                                    pointFeatures['maxima'].append(True)
                                                    pointFeatures['minima'].append(False)

                                crawlindex += 1
        return pointFeatures

    maxPointFeatures = vertexExtremaSearch(AdvancedFaceSurfaces, extremaType='maxima')
    minPointFeatures = vertexExtremaSearch(AdvancedFaceSurfaces, extremaType='minima')


_1=1 # todo: looking for 'superTypeName', e.g TiltedCylinder.step

vertexX = ['vertex1', 'vertex2']
vertexXref = ['vertex1ref', 'vertex2ref']
vertexXref_surroundIndex = ['vertex1ref_surroundIndex', 'vertex2ref_surroundIndex']
extremaType = 'maxima'
#extremaType = 'minima'
#extremaXYZ = []
extrema = dict()
for v in range(0, 2): # vertex1, vertex2
    for AFSindex, AFS in enumerate(AdvancedFaceSurfaces):
        for edge in AFS['outerBoundEdgeLoop']:
            if (edge.get('superTypeNameFlag') is not None):
                if edge['superTypeName'] == 'SEAM_CURVE':  # exclude SEAM_CURVE data
                    print("is this code reachable??")
                    break
                    # unnecessary?

            elif not edge.get('pointFeature'):
                # commence with searching local minima around edges associated with AFS surface
                # for each edge, get 'pointFeatures' fields
                print("missing pointFeature field") # this should be checked elsewhere
                break

            elif ((v==0) and (edge[vertexXref[v]] not in extrema.keys())) or ((v==1) and (edge[vertexXref[v]] not in extrema.keys())):
                # get localMinima for all vertex and attached edges,
                # retain extrema value in separate set, [surface]['outerBoundEdgeLoop'][edge]['pointFeature'][index]
                # conduct championship knockout to find global extrema

                # for each pointFeature element, get max/min
                # for vertex1/2 find if relative max/min from adjacent pointFeature element - ignore surfaces for now

                #pfLocal = edge['pointFeature']
                # u == 0, vertex1, get attached pointFeature
                # surroundIndex should give address of adjacent edges
                pfSurround = []
                adjacentEdgeRefs = []
                reversedOrder = []
                for vseRef in edge[vertexXref_surroundIndex[v]].keys():
                    adjacentEdgeRefs.append(vseRef)
                    vse = edge[vertexXref_surroundIndex[v]][vseRef]
                    adjPF = AdvancedFaceSurfaces[vse[0]]['outerBoundEdgeLoop'][vse[1]]['pointFeature']
                    if AdvancedFaceSurfaces[vse[0]]['outerBoundEdgeLoop'][vse[1]][vertexXref[v]] == edge[vertexXref[v]]:
                        pfSurround.append(adjPF)
                        reversedOrder.append(False)
                    else:
                        reversePF = dict()
                        reversePF['u'] = adjPF['u'].copy()
                        reversePF['u'].reverse()
                        reversePF['xyz'] = adjPF['xyz'].copy()
                        reversePF['xyz'].reverse()
                        reversePF['centroidDisp'] = adjPF['centroidDisp'].copy()
                        reversePF['centroidDisp'].reverse()
                        pfSurround.append(reversePF)
                        reversedOrder.append(True)

                # test vertexX with immediate neighbours
                if extremaType == 'maxima':
                    if all([pfs['centroidDisp'][0] > pfs['centroidDisp'][1] + eps_STEP_AP21 for pfs in pfSurround]):
                        if not array3x1match(edge[vertexX[v]], surfaceMaxPoint):
                            surfaceMaxPoint.append(edge[vertexX[v]])
                            surfaceMaxFeatureCentroidDisp.append(pfSurround[0]['centroidDisp'][0])

                elif extremaType == 'minima':
                    if all([pfs['centroidDisp'][0] < pfs['centroidDisp'][1] - eps_STEP_AP21 for pfs in pfSurround]):  # local min
                        if not array3x1match(edge[vertexX[v]], surfaceMinPoint):
                            surfaceMinPoint.append(edge[vertexX[v]])
                            surfaceMinFeatureCentroidDisp.append(pfSurround[0]['centroidDisp'][0])

                maxPFlen = max([len(pfs['u']) for pfs in pfSurround])-1
                if maxPFlen > 1:
                    crawlindex = 1
                    while crawlindex < maxPFlen:
                        # should capture all local max/min before second vertex
                        for pfsIndex, pfs in enumerate(pfSurround):
                            if crawlindex < len(pfs['centroidDisp']) - 1: # 'centroidDisp' find local maxima/minima
                                pfcd0 = pfs['centroidDisp'][crawlindex-1]
                                pfcd1 = pfs['centroidDisp'][crawlindex]
                                pfcd2 = pfs['centroidDisp'][crawlindex+1]

                                if reversedOrder[pfsIndex]:
                                    u = len(pfs['u']) - crawlindex - 1 #CHECK
                                else:
                                    u = crawlindex

                                if extremaType == 'maxima':
                                    if (pfcd1 > pfcd0 + eps_STEP_AP21) and (pfcd1 > pfcd2 + eps_STEP_AP21): # local max
                                        #vse = edge[vertexXref_surroundIndex[v]][adjacentEdgeRefs[pfsIndex]]
                                        if not array3x1match(pfs['xyz'][u], surfaceMaxPoint):
                                            surfaceMaxPoint.append(pfs['xyz'][u])
                                            surfaceMaxFeatureCentroidDisp.append(pfcd1)

                                elif extremaType == 'minima':
                                    if (pfcd1 < pfcd0 - eps_STEP_AP21) and (pfcd1 < pfcd2 - eps_STEP_AP21):  # local min
                                        #vse = edge[vertexXref_surroundIndex[v]][adjacentEdgeRefs[pfsIndex]]
                                        if not array3x1match(pfs['xyz'][u], surfaceMinPoint):
                                            surfaceMinPoint.append(pfs['xyz'][u])
                                            surfaceMinFeatureCentroidDisp.append(pfcd1)

                        crawlindex +=1


# collate maxima minima extrema within all surfaces, note that local max/min implies that no global hierarchy is required
surfacePointFeature = dict() # todo: this should become an initiated class
surfacePointFeature['xyz'] = []
surfacePointFeature['centroidDisp'] = []
surfacePointFeature['maxima'] = []
surfacePointFeature['minima'] = []

surfaceSphericalFeature = dict()
surfaceSphericalFeature['rotSymCentre'] = []
surfaceSphericalFeature['rotSymRadius'] = []

#for AFSindex, AFS in enumerate(AdvancedFaceSurfaces):
for AFS in AdvancedFaceSurfaces:
    if (AFS.get('ParsedSurface') is not None):
        for index_xyz, xyz in enumerate(AFS['ParsedSurface']['pointFeature']['xyz']):
            if not array3x1match(xyz, surfacePointFeature['xyz']):
                surfacePointFeature['xyz'].append(xyz)
                surfacePointFeature['centroidDisp'].append(AFS['ParsedSurface']['pointFeature']['centroidDisp'][index_xyz])
                surfacePointFeature['maxima'].append(AFS['ParsedSurface']['pointFeature']['maxima'][index_xyz])
                surfacePointFeature['minima'].append(AFS['ParsedSurface']['pointFeature']['minima'][index_xyz])


        if AFS['SurfaceTypeName'] == 'SPHERICAL_SURFACE':
            if AFS['ParsedSurface']['sphereFeatureFlag']:
                surfaceSphericalFeature['rotSymCentre'] = AFS['ParsedSurface']['pointFeature']['rotSymFeature']['rotSymCentre']
                surfaceSphericalFeature['rotSymRadius'] = AFS['ParsedSurface']['pointFeature']['rotSymFeature']['rotSymRadius']

#     def __init__(self):
#         self.objectHandle = (
#             ""  # pointer or string used to reference model in CAD model-space
#         )
#         self.filepath = ""
#         self.name = ""
#         self.generationTime = 0
#         self.surfaceStatus = ""
#         self.insertionPoint = Point(0.0, 0.0, 0.0)
#         self.rotation = 0.0
#         self.rotationAxis = None
#         self.rotationMatrix = None
#         self.scale = 1.0

#         self.featureMaxPoints = []
#         self.featureMinPoints = []  # arrange as a list of lists representing points

#         self.surfacePoints = []

#         self.centroid = None
#         self.rotSymRidgePoints = []
#         self.rotSymGroovePoints = []

#         self.featureMaxCurveDisps = []
#         self.featureMaxCentres = []

#         self.featureMinCurveDisps = []
#         self.featureMinCentres = []

#         self.featureSphereDisps = []
#         self.spherePoints = []

#         self.centrePoints = []


pass

# for AFSindex, AFS in enumerate(AdvancedFaceSurfaces):
#     if AFS['SurfaceTypeName'] == 'SPHERICAL_SURFACE':
#         # edge and surface point only equidistant from origin if centre coincides with origin
#         print("write code for SPHERICAL_SURFACE")
#         break
#     else:
#         # commence with searching local minima around edges associated with AFS surface
#         for edge in AFS['outerBoundEdgeLoop']:
#             if (edge.get('superTypeNameFlag') is not None):
#                 if edge['superTypeName'] == 'SEAM_CURVE':  # exclude SEAM_CURVE data
#                     print("is this code reachable??")
#                     break
#                     # unnecessary?
#
#             else:
#                 edgeMinima = {}
#                 edgeMinimaCentroidDisp = {}
#
#                 # in the instance that a vertex is the edge minima, compare minima associated with adjoining edges & surfaces
#                 # adjEdgeSet, adjSurfaceSet contain the indices of adjacent surface & edges
#
#                 if edge['vertex1extremaMin']:
#                     adjSurfaceSet = edge['vertex1SurfacesIndex'].values()
#                     adjEdgeSet = edge['vertex1edgesIndex'].values()
#
#                 elif edge['vertex2extremaMin']:
#                     adjSurfaceSet = edge['vertex2SurfacesIndex'].values()
#                     adjEdgeSet = edge['vertex2edgesIndex'].values()
#
#                 else:  # minPoint somewhere between vertices on edge todo midpoint=> surfaces adjoining endpoints
#                     adjSurfaceSet = edge['edgeAdjSurfacesIndex'].values()
#                     # adjEdgeSet is union of set of edges at each vertex & remove identity edge
#                     adjEdgeSet = ([edge['vertex1edgesIndex'][i] for i in edge['vertex1edgesIndex'].keys() if i is not edge['edgeRef']] +
#                                   [edge['vertex2edgesIndex'][i] for i in edge['vertex2edgesIndex'].keys() if i is not edge['edgeRef']])
#
#
#                 # vertexX, adjacent surfaces minima
#                 for adjSurfaceIndex in adjSurfaceSet:
#                     adjSurface = AdvancedFaceSurfaces[adjSurfaceIndex]
#                     if adjSurface['ParsedSurface'].get('minimaPoints') is not None:
#                         edgeMinima[adjSurface['SurfaceRef']] = adjSurface['ParsedSurface']['minimaPoints']
#                         edgeMinimaCentroidDisp[adjSurface['SurfaceRef']] = adjSurface['ParsedSurface']['minimaPointsCentroidDisp']
#                     elif adjSurface['ParsedSurface'].get('minPoint') is not None:
#                         edgeMinima[adjSurface['SurfaceRef']] = adjSurface['ParsedSurface']['minPoint']
#                         edgeMinimaCentroidDisp[adjSurface['SurfaceRef']] = adjSurface['ParsedSurface']['minPointCentroidDisp']
#
#                     # 'rotSymCentre' is centre of rotationally symmetrical surface feature (e.g. cylinder minimum)
#                     if adjSurface['ParsedSurface']['rotSymFeature'].get('rotSymCentre') is not None:
#                         if adjSurface['ParsedSurface'].get('rotSymMin') is not None:
#                             if adjSurface['ParsedSurface'].get('minPointCentroidDisp') is not None:
#                                 edgeMinima[adjSurface['SurfaceRef']] = adjSurface['ParsedSurface']['rotSymFeature']['rotSymCentre']
#                                 edgeMinimaCentroidDisp[adjSurface['SurfaceRef']] = adjSurface['ParsedSurface']['minPointCentroidDisp']
#                             else:
#                                 print("unexpected!")
#
#                 # vertexX, adjacent edges minima, get all relevant minima adjacent to origin vertex/edge/surface
#                 for adjSurfaceEdgeIndex in adjEdgeSet:
#                     adjEdge = AdvancedFaceSurfaces[adjSurfaceEdgeIndex[0]]['outerBoundEdgeLoop'][adjSurfaceEdgeIndex[1]]
#                     if adjEdge.get('minimaPoints') is not None:
#                         edgeMinima[adjEdge['edgeRef']] = adjEdge['minimaPoints']
#                         edgeMinimaCentroidDisp[adjEdge['edgeRef']] = adjEdge['minimaPointsCentroidDisp']
#                     elif adjEdge.get('minPoint') is not None:
#                         edgeMinima[adjEdge['edgeRef']] = adjEdge['minPoint']
#                         edgeMinimaCentroidDisp[adjEdge['edgeRef']] = adjEdge['minPointCentroidDisp']
#                     if adjEdge.get('rotSymMin') is not None:
#                         edgeMinima[adjEdge['edgeRef']] = adjEdge['rotSymCentre']
#                         edgeMinimaCentroidDisp[adjEdge['edgeRef']] = adjEdge['rotSymRadius']
#
#                 # test if local minimum relative to surrounding minima
#                 if len(edgeMinimaCentroidDisp) < 1:
#                     deltaMin = 0.0
#                 else:
#                     deltaMin = edge['minPointCentroidDisp'] - min(edgeMinimaCentroidDisp.values())
#                 if (np.abs(deltaMin) * 2 < eps_STEP_AP21) or (deltaMin <= 0):
#                     if edge.get('rotSymCentre') is not None: # rotSym feature
#                         if not array3x1match(edge['rotSymCentre'], surfaceMinRotSym):  # check already exists
#                             surfaceMinRotSym.append(edge['rotSymCentre'])
#                             surfaceMinFeatureCentroidDisp .append(edge['minPointCentroidDisp'])
#                     if edge.get('minimaPoints') is not None:
#                         #
#                         for minp, minpi in enumerate(edge['minimaPoints']):
#                             if not array3x1match(minp, surfaceMinPoint):
#                                 surfaceMinPoint.append(minp)
#                                 surfaceMinFeatureCentroidDisp.append(edge['minimaPointsCentroidDisp'][minpi])
#                     elif edge.get('minPoint') is not None:
#                         if not array3x1match(edge['minPoint'], surfaceMinPoint):
#                             surfaceMinPoint.append(edge['minPoint'])
#                             surfaceMinFeatureCentroidDisp .append(edge['minPointCentroidDisp'])
#                         edge['localMinima'] = True
#                     else:
#                         edge['localMinima'] = False
#
#         # test minima of surface patch (AFS) against those of immediate surface edges
#         # note that multiple local surface minima can have an overall minima value
#         # todo this approach breaks where there are multiple equal minima within a single surface: prepare for list representation
#         edgeMinima = {}
#         edgeMinimaCentroidDisp = {}
#         for edge in AFS['outerBoundEdgeLoop']: # todo 'faceInnerBoundEdgeLoopList'
#             if edge['minPoint'] is not None:
#                 edgeMinima[edge['edgeRef']] = edge['minPoint']
#                 edgeMinimaCentroidDisp[edge['edgeRef']] = edge['minPointCentroidDisp']
#
#         # test minima of surface patch against those of surface edges
#         # groove representation here? - 'minPointCentroidDisp' will work, 'minArcPoint' represents arc centre? minFeatureCentroidDisp
#         if (AFS['ParsedSurface'].get('minPointCentroidDisp') is not None):
#             if len(edgeMinimaCentroidDisp) < 1:
#                 deltaMin = 0.0
#             else:
#                 deltaMin = AFS['ParsedSurface']['minPointCentroidDisp'] - min(edgeMinimaCentroidDisp.values())
#
#             if (np.abs(deltaMin) < eps_STEP_AP21) or (deltaMin < 0):
#                 if AFS['ParsedSurface']['minPoint'] is None: # rotSym feature
#                     if len(surfaceMinPoint) > 0:
#                         if len(surfaceMinRotSym) > 0:
#                             if not array3x1match(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'], surfaceMinRotSym):  # check already exists
#                                 surfaceMinRotSym.append(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'])
#                                 surfaceMinFeatureCentroidDisp.append(AFS['ParsedSurface']['minPointCentroidDisp'])
#                         else: # first value
#                             surfaceMinRotSym.append(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'])
#                             surfaceMinFeatureCentroidDisp.append(AFS['ParsedSurface']['minPointCentroidDisp'])
#                 else:
#                     if surfaceMinPoint is not None:
#                         if not array3x1match(AFS['ParsedSurface']['minPoint'], surfaceMinPoint): # check already exists
#                             surfaceMinPoint.append(AFS['ParsedSurface']['minPoint'])
#                             surfaceMinFeatureCentroidDisp.append(AFS['ParsedSurface']['minPointCentroidDisp'])
#                     else:
#                         surfaceMinPoint.append(AFS['ParsedSurface']['minPoint'])
#                         surfaceMinFeatureCentroidDisp.append(AFS['ParsedSurface']['minPointCentroidDisp'])
#                     AFS['ParsedSurface']['localMinima'] = True
#                     for edge in AFS['outerBoundEdgeLoop']:
#                         edge['localMinima'] = False
#             # else:
#             #     AFS['ParsedSurface']['localMinima'] = False

# print(surfaceMinPoint)
# print(surfaceMinFeatureCentroidDisp )

# # repeat for maxima search
# for AFSindex, AFS in enumerate(AdvancedFaceSurfaces):
#     if AFS['SurfaceTypeName'] == 'SPHERICAL_SURFACE':
#         print("write code for SPHERICAL_SURFACE")
#         # edge and surface point only equidistant from origin if centre coincides with origin
#     else:
#         # commence with searching local minima around edges associated with AFS surface
#         for edge in AFS['outerBoundEdgeLoop']:
#             if edge.get('superTypeName'):
#                 if edge['superTypeName'] == 'SEAM_CURVE':  # exclude SEAM_CURVE data
#                     break
#
#                 else:
#                     edgeMaxima = {}
#                     edgeMaximaCentroidDisp = {}
#
#                 # # in the instance that a vertex is the edge maxima, compare maxima associated with adjoining edges & surfaces
#                 # # adjEdgeSet, adjSurfaceSet contain the indices of adjacent surface & edges
#                 # determine whether maxPoint is vertex or edge; multiple equal maxima, see hill-climbing categorisation
#                 # multiple local maxima suggest saddle points and/or surrounding local minima
#
#                 if edge['vertex1extremaMax']:
#                     adjSurfaceSet = edge['vertex1SurfacesIndex'].values()
#                     adjEdgeSet = edge['vertex1edgesIndex'].values()
#                 elif edge['vertex2extremaMax']:
#                     adjSurfaceSet = edge['vertex2SurfacesIndex'].values()
#                     adjEdgeSet = edge['vertex2edgesIndex'].values()
#                 else:  # maxPoint somewhere between vertices on edge
#                     adjSurfaceSet = edge['edgeAdjSurfacesIndex'].values()
#                     # adjEdgeSet is union of set of edges at each vertex
#                     adjEdgeSet = ([edge['vertex1edgesIndex'][i] for i in edge['vertex1edgesIndex'].keys() if i is not edge['edgeRef']] +
#                                   [edge['vertex2edgesIndex'][i] for i in edge['vertex2edgesIndex'].keys() if i is not edge['edgeRef']])
#
#                 # vertexX, adjacent surfaces maxima
#                 for adjSurfaceIndex in adjSurfaceSet:
#                     adjSurface = AdvancedFaceSurfaces[adjSurfaceIndex]
#                     if adjSurface['ParsedSurface'].get('maximaPoints') is not None:
#                         edgeMaxima[adjSurface['SurfaceRef']] = adjSurface['ParsedSurface']['maximaPoints']
#                         edgeMaximaCentroidDisp[adjSurface['SurfaceRef']] = adjSurface['ParsedSurface']['maximaPointsCentroidDisp']
#                     elif adjSurface['ParsedSurface'].get('maxPoint') is not None:
#                         edgeMaxima[adjSurface['SurfaceRef']] = adjSurface['ParsedSurface']['maxPoint']
#                         edgeMaximaCentroidDisp[adjSurface['SurfaceRef']] = adjSurface['ParsedSurface']['maxPointCentroidDisp'] # todo rename maxFeatureCentroidDisp?
#                     # 'rotSymCentre' is centre to rotationally-symmetric feature, meaning that it's a placeholder for no-point-minima condition
#                     if adjSurface['ParsedSurface'].get('rotSymCentre') is not None:
#                         if adjSurface['ParsedSurface'].get('rotSymMax') is not None:
#                             if adjSurface['ParsedSurface'].get('maxPointCentroidDisp') is not None:
#                                 edgeMinimaCentroidDisp[adjSurface['SurfaceRef']] = adjSurface['ParsedSurface']['maxPointCentroidDisp']
#                             else:
#                                 print("unexpected! - frustum instance")
#
#                 # vertexX, adjacent edges maxima
#                 for adjSurfaceEdgeIndex in adjEdgeSet:
#                     adjEdge = AdvancedFaceSurfaces[adjSurfaceEdgeIndex[0]]['outerBoundEdgeLoop'][adjSurfaceEdgeIndex[1]]
#                     if adjEdge.get('maximaPoints') is not None:
#                         edgeMaxima[adjEdge['edgeRef']] = adjEdge['maximaPoints']
#                         edgeMaximaCentroidDisp[adjEdge['edgeRef']] = adjEdge['maximaPointsCentroidDisp']
#                     elif adjEdge.get('maxPoint') is not None:
#                         edgeMaxima[adjEdge['edgeRef']] = adjEdge['maxPoint']
#                         edgeMaximaCentroidDisp[adjEdge['edgeRef']] = adjEdge['maxPointCentroidDisp']
#                     if adjEdge.get('rotSymMax') is not None:
#                         edgeMinima[adjEdge['edgeRef']] = adjEdge['rotSymCentre']
#                         edgeMinimaCentroidDisp[adjEdge['edgeRef']] = adjEdge['rotSymRadius']
#
#                 # test if local maximum relative to surrounding maxima
#                 if len(edgeMaximaCentroidDisp) < 1:
#                     deltaMax = 0.0
#                 else:
#                     deltaMax = edge['maxPointCentroidDisp'] - max(edgeMaximaCentroidDisp.values())
#                 if (np.abs(deltaMax) < eps_STEP_AP21) or (deltaMax > 0): # EPS greater than or equal to
#                     if edge.get('rotSymCentre') is not None: # rotSym feature
#                         if not array3x1match(edge['rotSymCentre'], surfaceMaxRotSym):  # check already exists
#                             surfaceMaxRotSym.append(edge['rotSymCentre'])
#                             surfaceMaxFeatureCentroidDisp.append(edge['maxPointCentroidDisp'])
#                     if edge['maxPoint'] is not None:
#                         if not array3x1match(edge['maxPoint'], surfaceMaxPoint):
#                             surfaceMaxPoint.append(edge['maxPoint'])
#                             surfaceMaxFeatureCentroidDisp.append(edge['maxPointCentroidDisp'])
#                         edge['localMaxima'] = True
#                     else:
#                         edge['localMaxima'] = False
#
#         # test maxima of surface patch (AFS) against those of immediate surface edges
#         edgeMaxima = {}
#         edgeMaximaCentroidDisp = {}
#         for edge in AFS['outerBoundEdgeLoop']:
#             if edge['maxPoint'] is not None:
#                 edgeMaxima[edge['edgeRef']] = edge['maxPoint']
#                 edgeMaximaCentroidDisp[edge['edgeRef']] = edge['maxPointCentroidDisp']
#
#         # test maxima of surface patch against those of surface edges
#         #if (AFS['ParsedSurface'].get('maxPointCentroidDisp') is not None) and (len(edgeMaximaCentroidDisp) > 0):
#         if (AFS['ParsedSurface'].get('maxPointCentroidDisp') is not None):
#             if len(edgeMaximaCentroidDisp) < 1:
#                 deltaMax = 0.0
#             else:
#                 deltaMax = AFS['ParsedSurface']['maxPointCentroidDisp'] - min(edgeMaximaCentroidDisp.values())
#
#             if (np.abs(deltaMax) < eps_STEP_AP21) or (deltaMax > 0):
#                 if AFS['ParsedSurface']['maxPoint'] is None: # rotSym feature
#                     if len(surfaceMaxPoint) > 0:
#                         if len(surfaceMaxRotSym) > 0:
#                             if not array3x1match(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'], surfaceMaxRotSym):  # check already exists
#                                 surfaceMaxRotSym.append(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'])
#                                 surfaceMaxFeatureCentroidDisp.append(AFS['ParsedSurface']['maxPointCentroidDisp'])
#                         else: # first value
#                             surfaceMaxRotSym.append(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'])
#                             surfaceMaxFeatureCentroidDisp.append(AFS['ParsedSurface']['maxPointCentroidDisp'])
#                     else:
#                         if surfaceMaxPoint is not None:
#                             if not array3x1match(AFS['ParsedSurface']['maxPoint'], surfaceMaxPoint): # check already exists
#                                 surfaceMaxPoint.append(AFS['ParsedSurface']['maxPoint'])
#                                 surfaceMaxFeatureCentroidDisp.append(AFS['ParsedSurface']['maxPointCentroidDisp'])
#                         else:
#                             surfaceMaxPoint.append(AFS['ParsedSurface']['maxPoint'])
#                             surfaceMaxFeatureCentroidDisp.append(AFS['ParsedSurface']['maxPointCentroidDisp'])
#                         AFS['ParsedSurface']['localMaxima'] = True
#                         for edge in AFS['outerBoundEdgeLoop']:
#                             edge['localMaxima'] = False
#
#                 else:
#                     if surfaceMaxPoint is not None:
#                         if not array3x1match(AFS['ParsedSurface']['maxPoint'], surfaceMaxPoint): # check already exists
#                             surfaceMaxPoint.append(AFS['ParsedSurface']['maxPoint'])
#                             surfaceMaxFeatureCentroidDisp.append(AFS['ParsedSurface']['maxPointCentroidDisp'])
#                         else:
#                             surfaceMaxPoint.append(AFS['ParsedSurface']['maxPoint'])
#                             surfaceMaxFeatureCentroidDisp.append(AFS['ParsedSurface']['maxPointCentroidDisp'])
#                         AFS['ParsedSurface']['localMaxima'] = True
#                         for edge in AFS['outerBoundEdgeLoop']:
#                             edge['localMaxima'] = False

# print('p = Draft.make_point( {:1.8f}, {:1.8f}, {:1.8f})'.format(xyz[0], xyz[0], xyz[2]))
pass # stop here for now

# *****SURFACE*****

# Plane - position - Axis_placement_3d
# (detect if plane normal passes through origin)

# Conical_surface - position - Axis_placement_3d
#                 - radius - Positive_length_measure
#                 - semi_angle - plane_angle_measure
# ( is cone axis parallel to ray through origin?,
#  are cone surfaces orthogonal to ray through origin?)

# Cylindrical_surface - position - Axis_placement_3d
#                     - radius - Positive_length_measure
# ( is cylindrical surface perpendicular to ray through origin?
# ( if not, extract point from bounding curve)

# Spherical_surface - position - Axis_placement_3d
#                   - radius - Positive_length_measure
# ( point on surface cotangent to centre and origin point)

# Swept_surface - swept_curve - Curve

# Swept_surface - Extruded_surface - extrusion_axis - Direction

# Swept_surface - Surface_of_revolution - axis_direction - Direction
#                                       - axis_point - Cartesian_point

# Toroidal_surface - position - Axis_placement_3d
#                  - radius - Positive_length_measure
#                  - minor_radius - Positive_length_measure
# ( if toroid is complete at outer/inner radius, increment rut-symm maxima/minima )

# Bounded_surface - B_spline_surface - U_degree - integer
#                                    - V_degree - integer
#                                    - control_points - Cartesian_point
#                                    - U_closed - boolean
#                                    - V_closed - boolean
# (part of complex_entity listing, need to find footpoint for maxima/minima)

# Bounded_surface - B_spline_surface - Rational_B_spline_surface - real

# Bounded_surface - B_spline_surface - Surface_with_explicit_knots - U_knot_multiplicities - integer
#                                                                  - U_knot_values - Parameter_value
#                                                                  - V_knot_multiplicities - integer

# Bounded_surface - B_spline_surface - Surface_with_implicit_knots - Knot_specification

# Bounded_surface - Curve_bounded_surface - basis_surface - Surface
#                                         - boundaries - Boundary_curve
#                                         - implicit_outer - boolean

# Bounded_surface - Rectangular_composite_surface - segments - Surface_patch
#                                                 - using_surfaces - Surface_patch
# Bounded_surface - parent_surface - Surface_patch - u_sense - boolean
#                                                  - v_sense - boolean
#                                                  - u_transition - Surface_transition_code
#                                                  - v_transition - Surface_transition_code

# Bounded_surface - Trimmed_surface - u1 - Parameter_value - real
#                                   - u2 - Parameter_value - real
#                                   - v1 - Parameter_value - real
#                                   - v2 - Parameter_value - real
#                                   - usense - boolean
#                                   - vsense - boolean

# *****CURVE*****

# Conic - position - Axis_placement

# Conic - Circle - Closed_curve (optional) - Closed_composite_curve
#                - radius - Positive_length_measure
# ( does circle axis pass thru origin? )

# Conic - Ellipse - first_semi_axis - Positive_length_measure
#                 - second_semi_axis - Positive_length_measure

# Conic - Hyperbola - semi_axis - Positive_length_measure
#                   - imaginary_semi_axis - Positive_length_measure

# Conic - Parabola - focal_distance - Length_measure

# Line - point - Cartesian_point
#      - line_direction - Direction

# Bounded_curve - segment_curve - Composite_curve_segment

# Bounded_curve - Composite_curve - curve_segment - Composite_curve_segment
# Bounded_curve - Composite_curve - using_curves - Composite_curve_segment

# Bounded_curve - Closed_composite_curve - Boundary_curve

# Bounded_curve - B_spline_curve - degree - integer
#                                - control_points - Cartesian_point
#                                - closed - boolean

# Bounded_curve - B_spline_curve - Rational_b_spline_curve - weight_values - real

# Bounded_curve - B_spline_curve - Curve_with_explicit_knots - knot_values - Parameter_value
#                                                            - knot_multiplicities - integer

# Bounded_curve - B_spline_curve - Curve_with_implicit_knots - knot_type - Knot_specification

# Trimmed_curve - start_point - Cartesian_point
#               - end_point - Cartesian_point


# maxima/minima on edges, but not at vertices => traverse adjoining surfaces and edges until first min/max
# maxima/minima at vertices => traverse adjoining surfaces and edges until first min/max


# if maxPoint/minPoint at an edge are at an edge vertex, this requires that all surfaces adjacent to this vertex are identified & tested

# traverse data and construct a set of minima points, including their nearest minima points
# repeat for maxima points
# philosophy is to find local minima within surrounding ring of nearest


# identify minima/maxima points in hierarchy from local minima maxima points.
# for every point in all points, find nearest neighbours and determine maximal disp from centroid
# calculate rotSym minima maxima from surrounding vertices

# for finding nearest vertices, can use KD-tree, e.g.
# from scipy import spatial # numpy < 1.24
# A = np.random.random((10,3))*100
# pt = [6, 30]  # <-- the point to find
# A[spatial.KDTree(A).query(pt)[1]] # <-- the nearest point
# distance,index = spatial.KDTree(A).query(pt)

# extract all rotationally symmetric features,
# test against adjoining surface for groove or ridge distinction

# strategy for planar polygonal surfaces is to divide into non-overlapping triangles where corners are polygon vertices
# and test for nearest/furthest point on triangle

# import and build pymesh locally, see https://github.com/PyMesh/PyMesh/blob/main/README.md


# class STEPface(object):
#     """
#     container class for STEP AP 203 surface
#     """
#
#     def __init__(self, baseRef=""):
#         self.baseRef = baseRef
#         self.typeName = '' # 'CYLINDRICAL_SURFACE',
#
#     def addShape(
#         self,
#         shapeObject,
#         shapeName
#     ):
#         shapeViewObject = self.modelDoc.addObject("Part::FeaturePython", shapeName)


# STEP Part21 decomposition:
# https://www.mbx-if.org/documents/AP203e2_html/AP203e2.htm

# root node, CLOSED_SHELL (OPEN_SHELL), Connected_Face_Set SUPERTYPE OF (ONEOF (Closed_Shell, Open_Shell))

# rethink
# deconstruct STEP model to find all faces
# categorise circles, cylinders and other forms with axes.
# extract points from Face_outer_bound (holes in face for minima?)

# first pass, find all edge cartesian_points to generate origin based on median
# second pass, extract surface maxima from faces,

# get all VERTEX, part of EDGE_LOOP
# get median barycentre/origin

# for all EDGE_LOOP, get VERTEX, find furthest vertex
# for all surfaces bound by EDGE_LOOP, calculate if they contain a point normal to an origin-ray


# note that for planar regions, maxima/minima is at a polygon point
# unless polygon plane is perfectly normal to ray =>

# random start, find points closest to initial Desernosphere rays (rethink: more relevant to NURBS surfaces)
# get polygon point sets attached to point, find polygon point with max/min disp, repeat
# cylinder surface ?? different process, look for entity type
# efficient graph node search between interconnecting points?
# surface normal - Newell's method for surface normal, cross product == zero, origin on a line parallel to surface normal

# for any planar polygon, if 1 point is further from/closer to origin than the others, it is polygon maxima/minima
# if 2 points share distance, lacal max/min is at edge.

# for any polygon point or edge at min/max find adjacent polygon to point/edge and check for associated points/edges of lesser/greater origin displacement.
# (3 points at equivalent disp on valid for symmetric polygon centered and normal to axis thru origin)

# for any cylindrical surface, get axis position
# find shortest distance from origin cylinder axis, returned point is orthogonal
# determine if all cylinder edges are above or below this point
# if not, max disp is point + radius

# torus, find max/min point on major radius circle, repeat for a minor radius circle at max/min point

# ellipsoid, try closest point to an ellipse parallel to major axis?

# 1. get the median origin from all points
# 2. find maximum displacement of points from this point, construct ray
# 3. for every ray find closest cosine angle points
# 3a. identify closest quadrant through point signs (required to translate model to origin?)
# segmentAndPlane( p0 : Point, p1 : Point, v0 : Point, n : Point )
# 3b. distances from points to ray
# 4. identify surfaces associated with these (edge) points
# 5. for each surface, determine if ray is between bounding edge points


    #   (axis1_placement,
    #        axis2_placement_2d,
    #        axis2_placement_3d,
    #        b_spline_curve,
    #        b_spline_curve_with_knots,
    #        b_spline_surface,
    #        b_spline_surface_with_knots,
    #        bezier_curve,
    #        bezier_surface,
    #        boundary_curve,
    #        cartesian_point,
    #        cartesian_transformation_operator_3d,
    #        circle,
    #        composite_curve,
    #        composite_curve_on_surface,
    #        composite_curve_segment,
    #        conic,
    #        conical_surface,
    #        curve,
    #        curve_bounded_surface,
    #        curve_replica,
    #        cylindrical_surface,
    #        degenerate_pcurve,
    #        degenerate_toroidal_surface,
    #        dimension_count,
    #        dimension_of,
    #        direction,
    #        ellipse,
    #        evaluated_degenerate_pcurve,
    #        geometric_representation_context,
    #        geometric_representation_item,
    #        hyperbola,
    #        intersection_curve,
    #        line,
    #        offset_curve_3d,
    #        offset_surface,
    #        outer_boundary_curve,
    #        parabola,
    #        pcurve,
    #        plane,
    #        point,
    #        point_on_curve,
    #        point_on_surface,
    #        point_replica,
    #        polyline,
    #        quasi_uniform_curve,
    #        quasi_uniform_surface,
    #        rational_b_spline_curve,
    #        rational_b_spline_surface,
    #        rectangular_composite_surface,
    #        rectangular_trimmed_surface,
    #        reparametrised_composite_curve_segment,
    #        seam_curve,
    #        spherical_surface,
    #        surface,
    #        surface_curve,
    #        surface_of_linear_extrusion,
    #        surface_of_revolution,
    #        surface_replica,
    #        swept_surface,
    #        toroidal_surface,
    #        trimmed_curve,
    #        uniform_curve,
    #        uniform_surface,


    # strategy for cylinders & cones (& rotationally-symmetric spheres, surfaces of revolution or ellipsoids), determine curved edges
    # if rot axis is between origin and surface, maxima lies on a line segment between maxima points on curved edges
    # if rot axis is beyond surface (concave), minima lies on a line segment between minima points on curved edges
    # otherwise maxima/minima lie on edges.



# given a vertex, edge or surface, return all adjacent vertices
# if point is on edge, return edge parallel surfaces & vertex1 + vertex2
# point at vertex, return connected edges and all adjoining surfaces
# point on surface, return edgeloop for surface (including inner) and all associated vertex

# todo: adjoining edges & surfaces part of future surface object method
# this process can be abstracted to a search for items in list & subset ['

# # find adjoining surfaces to every surface edge
# for AFS in AdvancedFaceSurfaces:
#     for edgeSource in AFS['outerBoundEdgeLoop']:
#         edgeSource['edgeAdjSurfacesIndex'] = {}
#         for AFS2index, AFS2 in enumerate(AdvancedFaceSurfaces):
#             for edgeTarget in AFS2['outerBoundEdgeLoop']:
#                 if edgeSource['edgeRef'] == edgeTarget['edgeRef']:
#                     if AFS2['SurfaceRef'] not in edgeSource['edgeAdjSurfacesIndex'].keys():
#                         edgeSource['edgeAdjSurfacesIndex'][AFS2['SurfaceRef']] = AFS2index
#
# # find adjoining surfaces to every vertex 1 surface point using STEP reference coincidence
# for AFS in AdvancedFaceSurfaces:
#     for edgeSource in AFS['outerBoundEdgeLoop']:
#         v1ref = edgeSource['vertex1ref']
#         edgeSource['vertex1SurfacesIndex'] = {}
#         for AFS2index, AFS2 in enumerate(AdvancedFaceSurfaces):
#             for edgeTarget in AFS2['outerBoundEdgeLoop']:
#                 if (v1ref == edgeTarget['vertex1ref']) or (v1ref == edgeTarget['vertex2ref']):
#                     if AFS2index not in edgeSource['vertex1SurfacesIndex'].keys():
#                         edgeSource['vertex1SurfacesIndex'][AFS2['SurfaceRef']] = AFS2index
#
# # find adjoining surfaces to every vertex 2 surface point
# for AFS in AdvancedFaceSurfaces:
#     for edgeSource in AFS['outerBoundEdgeLoop']:
#         v2ref = edgeSource['vertex2ref']
#         edgeSource['vertex2SurfacesIndex'] = {}
#         for AFS2index, AFS2 in enumerate(AdvancedFaceSurfaces):
#             for edgeTarget in AFS2['outerBoundEdgeLoop']:
#                 if (v1ref == edgeTarget['vertex1ref']) or (v1ref == edgeTarget['vertex2ref']):
#                     if AFS2index not in edgeSource['vertex2SurfacesIndex'].keys():
#                         edgeSource['vertex2SurfacesIndex'][AFS2['SurfaceRef']] = AFS2index
#
# # find adjoining edges to every vertex 1 surface point
# for AFS in AdvancedFaceSurfaces:
#     for edgeSource in AFS['outerBoundEdgeLoop']:
#         v1ref = edgeSource['vertex1ref']
#         edgeSource['vertex1edgesIndex'] = {}
#         for AFS2index, AFS2 in enumerate(AdvancedFaceSurfaces):
#             for edgeTargetIndex, edgeTarget in enumerate(AFS2['outerBoundEdgeLoop']):
#                 if (v1ref == edgeTarget['vertex1ref']) or (v1ref == edgeTarget['vertex2ref']):
#                     if edgeTarget['edgeRef'] not in edgeSource['vertex1edgesIndex'].keys():
#                         edgeSource['vertex1edgesIndex'][edgeTarget['edgeRef']] = (AFS2index, edgeTargetIndex)
#
# # find adjoining edges to every vertex 2 surface point
# for AFS in AdvancedFaceSurfaces:
#     for edgeSource in AFS['outerBoundEdgeLoop']:
#         v2ref = edgeSource['vertex2ref']
#         edgeSource['vertex2edgesIndex'] = {}
#         for AFS2index, AFS2 in enumerate(AdvancedFaceSurfaces):
#             for edgeTargetIndex, edgeTarget in enumerate(AFS2['outerBoundEdgeLoop']):
#                 if (v2ref == edgeTarget['vertex1ref']) or (v2ref == edgeTarget['vertex2ref']):
#                     if edgeTarget['edgeRef'] not in edgeSource['vertex2edgesIndex'].keys():
#                         edgeSource['vertex2edgesIndex'][edgeTarget['edgeRef']] = (AFS2index, edgeTargetIndex)


#================================================================================
#
# for AFSindex, AFS in enumerate(AdvancedFaceSurfaces):
#     # get surface rotSymFeatures
#     for edge in AFS['outerBoundEdgeLoop']:
#         if (edge.get('rotSymFeature') is not None):
#             if (edge['rotSymFeature'].get('rotSymCentre') is not None) and (edge['rotSymFeature'].get('rotSymRadius') is not None):
#                 if type(edge['rotSymFeature']['rotSymCentre']) == list:
#                     for rsf in range(0, len(edge['rotSymFeature']['rotSymCentre'])):
#                         allRotSymFeatures['rotSymCentre'].append(edge['rotSymFeature']['rotSymCentre'][rsf])
#                         allRotSymFeatures['rotSymRadius'].append(edge['rotSymFeature']['rotSymRadius'][rsf])
#                 else:
#                     allRotSymFeatures['rotSymCentre'].append(edge['rotSymFeature']['rotSymCentre'])
#                     allRotSymFeatures['rotSymRadius'].append(edge['rotSymFeature']['rotSymRadius'])
#
#     # get surface rotSymFeatures
#     if (AFS['ParsedSurface'].get('rotSymFeature') is not None):
#         if (AFS['ParsedSurface']['rotSymFeature'].get('rotSymCentre') is not None) and (AFS['ParsedSurface']['rotSymFeature'].get('rotSymRadius') is not None):
#             if type(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre']) == list:
#                 for rsf in range(0, len(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'])):
#                     allRotSymFeatures['rotSymCentre'].append(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'][rsf])
#                     allRotSymFeatures['rotSymRadius'].append(AFS['ParsedSurface']['rotSymFeature']['rotSymRadius'][rsf])
#             else:
#                 allRotSymFeatures['rotSymCentre'].append(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'])
#                 allRotSymFeatures['rotSymRadius'].append(AFS['ParsedSurface']['rotSymFeature']['rotSymRadius'])
#
# print("need to test w/ multi axis")
# SuperAxisRotSymSet = []
# eliminationIndex = [i for i in range(0, len(allRotSymFeatures['rotSymCentre']))]
# # create groups of adjacent rotSym features along colinear axes
# for ARSFCindex, ARSFC in enumerate(allRotSymFeatures['rotSymCentre'] ):
#     if (len(eliminationIndex) > 0) and (ARSFCindex in eliminationIndex):
#         axisRotSymSet = [ARSFCindex,]
#         SuperAxisRotSymSet.append(axisRotSymSet)
#         eliminationIndex.pop(eliminationIndex.index(ARSFCindex))
#         for ARSFC2index, ARSFC2 in enumerate(allRotSymFeatures['rotSymCentre']):
#             if (len(eliminationIndex) > 0) and (ARSFC2index in eliminationIndex): # check vectors collinear
#                 if np.isclose(np.cross(ARSFC - centroid, ARSFC2 - centroid), eps).all() and (ARSFC2index not in axisRotSymSet):
#                     axisRotSymSet.append(ARSFC2index)
#                     eliminationIndex.pop(eliminationIndex.index(ARSFC2index))
#
# # eliminate duplicates here
#
# superAxesSet = []
# # need to find adjacent rotSym to establish maxima/minima
# for SARSS in SuperAxisRotSymSet:
#     lobeSet = dict()
#     # need to determine which rotSymCentre lie on the same side of centroid
#     # 1. find point maxCentroidDisp with max disp from centroid
#     # 2. sort all other points relative to maxDispCentroid
#
#     centreDisps = [np.linalg.norm(allRotSymFeatures['rotSymCentre'][s] - centroid) for s in SARSS]
#     maxCentreDisps = centreDisps.index(max(centreDisps))
#     axisDisps = [np.linalg.norm(allRotSymFeatures['rotSymCentre'][s] - allRotSymFeatures['rotSymCentre'][maxCentreDisps]) for s in SARSS]
#     axisDispsSorted = np.argsort(axisDisps)
#
#     SARSS = [SARSS[ads] for ads in axisDispsSorted]
#
#     lobeSet['centreCentroidDisp'] = [centreDisps[s] for s in SARSS]
#     lobeSet['rotSymCentre'] = [allRotSymFeatures['rotSymCentre'][s] for s in SARSS]
#     lobeSet['rotSymRadius'] = [allRotSymFeatures['rotSymRadius'][s] for s in SARSS]
#     # check neighbours for max/min determination (equality := min)
#     # furthest rotSym centre is always a maxima ? ----------------------------------------------------------
#     superAxesSet.append(lobeSet)
#
#
# for AFSindex, AFS in enumerate(AdvancedFaceSurfaces):
#     # commence with searching local minima around edges associated with AFS surface
#     # no real reason to separate vertices into points closer to localCentroid than arc/circle, and those on the far side.
#     for edge in AFS['outerBoundEdgeLoop']:
#         if (edge.get('rotSymFeature') is not None): # only single value in ['rotSymFeature'], no min, max for edges ??---------------------
#             if (edge['rotSymFeature'].get('rotSymCentre') is not None) and (edge['rotSymFeature'].get('rotSymRadius') is not None):
#                 # recall that it is not required for adjacent surfaces/edges to be radially symmetric
#                 adjSurfaceSet = edge['edgeAdjSurfacesIndex'].values()
#                 # presuming it is not required to identify edges
#
#                 # normalise axis normal
#                 rotSymCentroidDisp = np.linalg.norm(edge['rotSymFeature']['rotSymCentre'] - centroid)
#                 if rotSymCentroidDisp > eps:
#                     rotAxisDir = (edge['rotSymFeature']['rotSymCentre'] - centroid) / rotSymCentroidDisp
#                 else:
#                     # if centre to rotSym feature coincides with centroid, get rotAxisDir from other parsed data
#                     if AFS['ParsedSurface'].get('normDir') is not None:
#                         rotAxisDir = AFS['ParsedSurface']['normDir']
#                     else:
#                         print("no surface axisDir value, cross()?")
#
#                 # adjacent surfaces minima,  nearPointAxisDisp > rotSymCentroidDisp
#                 # near & far based on disp from centroid
#                 nearPointAxisDisp = []
#                 farPointAxisDisp = []
#                 #ddump=[]
#
#                 for adjSurfaceIndex in adjSurfaceSet:
#                     adjSurface = AdvancedFaceSurfaces[adjSurfaceIndex]
#                     for adjSurfaceEdge in adjSurface['outerBoundEdgeLoop']:
#                         if edge['edgeRef'] != adjSurfaceEdge['edgeRef']:
#
#                             # project all points, vertex1, vertex2, maxPoint, minPoint to rotational axis
#                             adjSurfaceVertices = adjSurfaceEdge['pointFeature']['xyz']
#
#                             for asv in adjSurfaceVertices:
#                                 #pAxisProj = pointProjectAxis(asv, centroid, rotAxisDir)
#                                 #pAxisProj = rotAxisDir * np.dot((asv - centroid), rotAxisDir) / np.dot(rotAxisDir, rotAxisDir)
#
#                                 if rotSymCentroidDisp > eps:
#                                     pAxisProj = rotAxisDir * np.dot((asv - centroid), rotAxisDir) / np.dot(rotAxisDir, rotAxisDir)
#                                 else:
#                                     pAxisProj = rotAxisDir * np.dot(asv, rotAxisDir) / np.dot(rotAxisDir, rotAxisDir)
#
#                                 pAxisDisp = np.linalg.norm(asv - pAxisProj)
#                                 #ddump.append(np.dot((asv - edge['rotSymFeature']['rotSymCentre']), rotAxis))
#
#                                 rotSymEdgeDisp = np.dot((asv - edge['rotSymFeature']['rotSymCentre']), rotAxisDir)
#                                 #print(rotSymEdgeDisp)
#                                 if rotSymEdgeDisp > eps: # eps_
#                                     #if np.linalg.norm(pAxisProj - centroid) > rotSymCentroidDisp:
#                                     farPointAxisDisp.append(pAxisDisp)
#                                 elif rotSymEdgeDisp < -eps:
#                                     nearPointAxisDisp.append(pAxisDisp)
#
#                 if len(nearPointAxisDisp)==0 or len(farPointAxisDisp)==0:
#                     # disc at end of object, defined as rotSymMax
#                     edge['rotSymMax'] = True
#                 else:
#                     NPADA = np.average(nearPointAxisDisp)
#                     FPADA = np.average(farPointAxisDisp)
#
#                     # conical midpoints
#                     avgRadius = np.abs(NPADA - FPADA)/2 + min([NPADA, FPADA])
#                     if avgRadius > edge['radius']:
#                         # ridge
#                         edge['rotSymMax'] = True
#                     else:
#                         edge['rotSymMin'] = True
#             # else:
#             #     edge['rotSymCentre'] = None
#             #     edge['rotSymRadius'] = None
#
#     # the other class of minima integral to surfaces, e.g. ring minima on cylinders
#     if (AFS['ParsedSurface'].get('rotSymFeature') is not None):
#         if (AFS['ParsedSurface']['rotSymFeature'].get('rotSymCentre') is not None) and (AFS['ParsedSurface'].get('rotSymRadius') is not None):
#
#             # normalise axis normal
#             rotSymCentroidDisp = np.linalg.norm(AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'] - centroid)
#             if rotSymCentroidDisp > eps:
#                 rotAxisDir = (AFS['ParsedSurface']['rotSymFeature']['rotSymCentre'] - centroid) / rotSymCentroidDisp
#             else:
#                 # if centre to rotSym feature coincides with centroid, get rotAxisDir from other parsed data
#                 if AFS['ParsedSurface'].get('normDir') is not None:
#                     rotAxisDir = AFS['ParsedSurface']['normDir']
#                 else:
#                     print("no surface axisDir value, cross()?")
#
#             # todo what happens with several rotationally symmetric features within a surface?
#             # todo list check?
#
#             # adjacent surfaces minima,  nearPointAxisDisp > rotSymCentroidDisp
#             nearPointAxisDisp = []
#             farPointAxisDisp = []
#
#             # use edges in surface FaceOuterBoundEdgeLoopList
#             for adjSurfaceEdge in AFS['outerBoundEdgeLoop']:
#                 # project all points, vertex1, vertex2, maxPoint, minPoint to rotational axis
#                 adjSurfaceVertices = adjSurfaceEdge['pointFeature']['xyz']
#                 for asv in adjSurfaceVertices:
#                     #pAxisProj = pointProjectAxis(asv, centroid, rotAxisDir)
#                     #pAxisProj = rotAxisDir * np.dot((asv - centroid), rotAxisDir) / np.dot(rotAxisDir, rotAxisDir)
#                     #pAxisProj = rotAxisDir * np.dot(asv, rotAxisDir) / np.dot(rotAxisDir, rotAxisDir)
#
#                     if rotSymCentroidDisp > eps:
#                         pAxisProj = rotAxisDir * np.dot((asv - centroid), rotAxisDir) / np.dot(rotAxisDir, rotAxisDir)
#                     else:
#                         pAxisProj = rotAxisDir * np.dot(asv, rotAxisDir) / np.dot(rotAxisDir, rotAxisDir)
#
#                     pAxisDisp = np.linalg.norm(asv - pAxisProj)
#                     rotSymEdgeDisp = np.dot((asv - AFS['ParsedSurface']['rotSymFeature']['rotSymCentre']), rotAxisDir)
#                     if rotSymEdgeDisp > eps:  # eps_
#                         # if np.linalg.norm(pAxisProj - centroid) > rotSymCentroidDisp:
#                         farPointAxisDisp.append(pAxisDisp)
#                     elif rotSymEdgeDisp < -eps:
#                         nearPointAxisDisp.append(pAxisDisp)
#
#                 if len(nearPointAxisDisp) == 0 or len(farPointAxisDisp) == 0:
#                     # disc at end of object, defined as rotSymMax
#                     AFS['ParsedSurface']['rotSymMax'] = True
#                 else:
#                     NPADA = np.average(nearPointAxisDisp)
#                     FPADA = np.average(farPointAxisDisp)
#
#                     # conical midpoints
#                     avgRadius = np.abs(NPADA - FPADA) / 2 + min([NPADA, FPADA])
#                     if avgRadius > AFS['ParsedSurface']['rotSymRadius']:
#                         # groove
#                         AFS['ParsedSurface']['rotSymMax'] = True
#                     else:
#                         AFS['ParsedSurface']['rotSymMin'] = True