import xml.etree.ElementTree as ElemTree
import geopandas as gpd
from shapely.geometry import Polygon, LineString

tree = ElemTree.parse("FM3_2.xml")
root = tree.getroot()
model = root[0]

polygon_list = []
polygon_name_list = []
trace_list = []
trace_name_list = []
for fault in model:
    fname = fault.attrib["sectionName"]
    for child in fault:
        if child.tag == "FaultTrace":
            trace_geom = []
            for loc in child:
                loc_float = [float(loc.attrib[x]) for x in ["Longitude", "Latitude"]]
                trace_geom.append(loc_float)
            trace_list.append(LineString(trace_geom))
            trace_name_list.append(fname)

        elif child.tag == "ZonePolygon":
            poly_geom = []
            loc_list = child[0]
            for loc in loc_list:
                loc_float = [float(loc.attrib[x]) for x in ["Longitude", "Latitude"]]
                poly_geom.append(loc_float)
            polygon_list.append(Polygon(poly_geom))
            polygon_name_list.append(fname)

poly_gdf = gpd.GeoDataFrame(polygon_name_list, geometry=polygon_list, columns=["Fault"], crs=4326)
trace_gdf = gpd.GeoDataFrame(trace_name_list, geometry=trace_list, columns=["Fault"], crs=4326)

poly_gdf.to_file("cali_poly.shp")
trace_gdf.to_file("cali_trace.shp")