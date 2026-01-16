import geopandas as gpd

# シェープファイルを読み込む
shapefile_path = "MESH05436.shp"
gdf = gpd.read_file(shapefile_path)
gdf = gdf.to_crs("EPSG:4326")
gdf

# GeoJSON に変換して保存
geojson_path = "MESH05436.geojson"
gdf.to_file(geojson_path, driver="GeoJSON")