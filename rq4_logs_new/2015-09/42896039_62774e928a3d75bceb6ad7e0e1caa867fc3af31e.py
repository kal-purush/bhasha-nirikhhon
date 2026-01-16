import arcpy
from arcpy import env
env.workspace = "P/PythonHomeWork/Data/Excercise05"
outputclass = "P/Python-Homework"
arcpy.Dissolve_Management("parks.shp", outputclass, "PARK_TYPE", "","SINGLE_PART","")