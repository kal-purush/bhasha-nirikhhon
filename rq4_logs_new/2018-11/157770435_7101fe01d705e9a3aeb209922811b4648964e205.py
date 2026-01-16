################## URBAN / SUBURBAN / RURAL INDEX ###############

## Import Modules:

import arcpy

#################################################################

## VARIABLES (USER DEFINED)

#################################################################

## SET WORKSPACE:

env.workspace = "c:/temp/GEOB370_final"

#################################################################

## INPUT LAYERS: (see README for appropriate inputs)

## Input Accessibility Layer
in_access = "accessibility_to_cities_2015_v1.0"

## Input Population Layer
in_pop = "GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0"

## Input Built-Up Layer
in_built = "GHS_BUILT_LDS2014_GLOBE_R2016A54009_250_v1"

## Input Regional Extent (Clip)/ Projection  Layer
clip_region = "EBC_REG_DT_polygon"

#################################################################

## CLIP DATASETS TO REGION EXTENT

## Clip Accessibility Layer
arcpy.Clip_analysis(in_access, clip_region, "clip_access.tif")

## Clip Population Layer
arcpy.Clip_analysis(in_pop, clip_region, "clip_pop.tif")

## Clip Built-Up Layer
arcpy.Clip_analysis(in_built, clip_region, "clip_built.tif")

#################################################################

## REPROJECT DATA LAYERS

## Reproject Accessibility Layer
arcpy.ProjectRaster_management("clip_access", "region_access.tif",\
                               "clip_region", "BILINEAR")

## Reproject Population Layer
arcpy.ProjectRaster_management ("clip_pop", "region_pop.tif",\
			       "clip_region", "BILINEAR")

## Reproject Built-Up Layer
arcpy.ProjectRaster_management("clip_built", "region_built.tif",\
                               "clip_region", "BILINEAR")

#################################################################







#################################################################

## REMOVE TEMPORARY FILES

arcpy.Delete_management("clip_access")
arcpy.Delete_management("clip_pop")
arcpy.Delete_management("clip_built")
arcpy.Delete_management("region_access")
arcpy.Delete_management("region_pop")
arcpy.Delete_management("region_built")
