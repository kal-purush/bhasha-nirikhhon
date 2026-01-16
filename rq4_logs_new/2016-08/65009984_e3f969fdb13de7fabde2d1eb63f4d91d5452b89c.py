#!/usr/bin/python
#! coding: utf-8

import sys
from os import path
if path.exists(path.normpath(u'x:\СИД\~INFO\Python\COMLibs')):
    sys.path.append(path.normpath(u'x:\СИД\~INFO\Python\COMLibs'))
if path.exists(path.normpath('d:\PY\LIB')):
    sys.path.append(path.normpath('d:\PY\LIB'))
try:
    from comtypes.client import *
    from comtypes.automation import *
except ImportError:
    showerror(title = 'Error', message = 'Comtypes Lib import error!')
from array import array

class AutoCadObj:
    
    objTypes = {
                 'polyline':[23,24]
               }
    
    def __init__(self, fname=None):
        self.errcode = 0
        try:
            self.acad = GetActiveObject("AutoCAD.Application")
        except:
            self.errcode = 1
        if self.errcode == 1:
            try:
                self.acad = CreateObject("AutoCAD.Application")
                self.errcode = 0
            except:
                self.errcode = 1
        print self.errcode
        if self.errcode == 0:
            try:
                if (fname is not None) and (path.exists(path.normpath(fname))):
                    self.dwg = self.acad.Documents.Open(path.normpath(fname))
                else:
                    self.dwg = self.acad.ActiveDocument
            except:
                self.errcode = 2
        print self.errcode
        if self.errcode == 0:
            try:
                self.mspace = self.dwg.ModelSpace
            except:
                self.errcode = 3
        print self.errcode

    #######################################
    #
    # Static methods
    #
    #######################################
    
    '''
    input: 2 or 3 floats
    returns variant array (x,y,z) or (x,y,0)
    static method !
    '''
    def __point(*args):
        lst = [0.]*3
        if len(args) < 3:
            lst[0:2] = [float(x) for x in args[0:2]]
        else:
            lst = [float(x) for x in args[0:3]]
        return VARIANT(array("d",lst))
        
    __point = staticmethod(__point)
    
    '''
    input: list of coords: [(x,y), (x,y), (x,y)]
    returns variant array (x,y,0,x,y,0 ...)
    static method !
    '''
    def __points(coords):
        tmp = []
        for xy in coords:
            tmp.append(xy[0])
            tmp.append(xy[1])
            tmp.append(0)
        lst = [float(x) for x in tmp]
        return VARIANT(array('d',lst))

    __points = staticmethod(__points)
    
    #######################################
    #
    # Instance methods
    #
    #######################################

    '''
    input: string - name of entity type
    returns list of entitys
    '''
    def getAllByType(self, entityType):
        return filter(lambda x: x.EntityType in AutoCadObj.objTypes[entityType],
                      self.mspace)

    '''
    input: string - name of entity type
    returns list of lists: [[(x,y), (x,y), (x,y)],[(x,y), (x,y), (x,y)]]
    '''
    def getCoordsByType(self, entityType):
        
        def getX(src, base=2):
            mx = len(src) - base + 1
            return src[0:mx:base]

        def getY(src, base=2):
            mx = len(src) - base + 2
            return src[1:mx:base]

        def getZ(src, base=3):
            return src[2:len(src):base]
            
        def getDivisor(src):
            res = False
            if (len(src) % 3 == 0):
                # if all Z equal - divisor is 3
                if len(set(getZ(src))) == 1:
                    res = 3
            if (len(src) % 2 == 0 and not res):
                res = 2
            return res
            
        res = []
        
        for entity in self.getAllByType(entityType):
            tmplst = []
            coords = entity.Coordinates
            divisor = getDivisor(coords)
                        
            if (divisor):
                crdx = getX(coords, divisor)
                crdy = getY(coords, divisor)
                
                for j in xrange(0,len(crdx)):
                    txy = (round(crdx[j],2), round(crdy[j],2))
                    tmplst.append(txy)
            res.append(tmplst)
            
        return res

    '''
    input: list of coords: [(x,y), (x,y), (x,y)]
    returns entity of created polyline
    '''
    def drawPolyLine(self, coords):
        ent = None
        v_ar = AutoCadObj.__points(coords)
        ent = self.mspace.AddPolyline(v_ar)
        ent.Closed = 1
        return ent

    '''
    input: point dictionary
    {
        "x": float,
        "y": float,
        "z": float, #(not required)
        "label": string, #(if not None - printed near point)
        "txtHeight": float, #(ignored if label is None)
        "showCoords": boolean #(if True, coord values should be printed)
    }
    returns entity of created point
    '''
    def drawPoint(self, point):
        ent = None
        z = 0
        if "z" in point:
            z = point["z"]
        v_xy = AutoCadObj.__point(point["x"], point["y"], z)
        ent = self.mspace.AddPoint(v_xy)
        if "label" in point and point["label"] is not None:
            labels = []
            labels.append(point["label"])
            txtHeight = 2
            if "txtHeight" in point:
                txtHeight = point["txtHeight"]
            if "showCoords" in point and point["showCoords"]:
                labels.append(str(point["x"]))
                labels.append(str(point["y"]))
                labels.append(str(z))
            i = 0
            delta = 0
            if len(labels) == 3:
                delta = txtHeight + len(labels) / 2
                i = -delta
            for lbl in labels:
                txtDict = {
                    "x": point["x"] + txtHeight,
                    "y": point["y"] - i,
                    "label": lbl,
                    "txtHeight": txtHeight
                }
                self.drawSlText(txtDict)
                i += delta
        return ent
                
    '''
    input: text dictionary
    {
        "x": float,
        "y": float,
        "label": string,
        "txtHeight": float
    }
    returns entity of created text
    '''
    def drawSlText(self, text):
        ent = None
        v_xy = AutoCadObj.__point(text["x"], text["y"], 0)
        ent = self.mspace.AddText(text["label"], v_xy, text["txtHeight"])
        return ent