import gvsig
import sys

from gvsig import commonsdialog
from gvsig import geom
from org.gvsig.tools.dispose import DisposeUtils

def main(*args):
    currentProject = gvsig.currentProject()
    if currentProject.getView("GSoC 2019 - Test") == None:
        currentProject.createView("GSoC 2019 - Test")
    view = currentProject.getView("GSoC 2019 - Test")
    view.showWindow()
    message = ""
    if view.getLayer("POINTS") == None:
        try:
            gvsig.loadShapeFile(gvsig.getResource(__file__, "data", "POINTS.shp"))
        except:
            message += "\n'POINTS.shp'"
    if view.getLayer("POLYGONS") == None:
        try:
            gvsig.loadShapeFile(gvsig.getResource(__file__, "data", "POLYGONS.shp"))
        except:
            message += "\n'POLYGONS.shp'"
    if message == "":
        pointsLayer = view.getLayer("POINTS")
        polygonsLayer = view.getLayer("POLYGONS")
        if pointsLayer != None and polygonsLayer != None:
            schema = pointsLayer.getSchema()
            newSchema = gvsig.createSchema(schema)
            newSchema.append("FLAG", "INTEGER", 1)
            newPointsLayer = gvsig.createShape(newSchema)
            newPointsLayer.edit()
            try:
                polygonFeatures = polygonsLayer.features()
                for polygonFeature in polygonFeatures:
                    pointFeatures = pointsLayer.features()
                    point = dict()
                    flag = False
                    for pointFeature in pointFeatures:
                        if polygonFeature.geometry().intersects(pointFeature.geometry()):
                            for field in schema:
                                if str(field.getDataTypeName()) != "Long":
                                    point[field.getName()] = pointFeature.get(field.getName())
                                else:
                                    point[field.getName()] = long(pointFeature.get(field.getName()))
                            point["FLAG"] = 1
                            flag = True
                    if not flag:
                        point["NAME"] = polygonFeature.get("NAME")
                        point["FLAG"] = 0
                        point["GEOMETRY"] = polygonFeature.geometry().centroid()
                        flag = True
                    newPointsLayer.append(point)
                message = "Points inside polygons:"
                features = newPointsLayer.features("FLAG=1")
                for feature in features:
                    message += "\n" + feature.get("NAME")
                title = "GSoC 2019 - Test"
                messageType = commonsdialog.IDEA
                root = None
                commonsdialog.msgbox(message, title, messageType, root)
                print message
            except:
                ex = sys.exc_info()[1]
                print ex.__class__.__name__ + " - " + str(ex)
            finally:
                DisposeUtils.disposeQuietly(pointFeatures)
                DisposeUtils.disposeQuietly(polygonFeatures)
                DisposeUtils.disposeQuietly(features)
            
            newPointsLayer.commit()
            newPointsLayer.setName("NEW POINTS")
            view.addLayer(newPointsLayer)
            view.getMapContext().getViewPort().setEnvelope(newPointsLayer.getFullEnvelope())
            view.showWindow()
    else:
        message = "It wasn't possible to load:" + message
        title = "GSoC 2019 - Test"
        messageType = commonsdialog.FORBIDEN
        root = None
        commonsdialog.msgbox(message, title, messageType, root)