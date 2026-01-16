import numpy as np
import pandas as pd
import netCDF4 as nc
import geopandas as gpd
import xarray as xr
import rasterio.mask
import rioxarray as rxr

from sklearn.metrics import confusion_matrix
from scipy.stats import mode
from shapely.geometry import Polygon


# Define a function to download data from a URL
def download_CW(url, filename):
    from urllib.request import urlretrieve
    urlretrieve(url, filename)


def get_geom(xmin, ymin, xmax, ymax):
    # Create the polygon
    polygon = Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])

    # Create a GeoDataFrame with the polygon as the geometry
    gdf = gpd.GeoDataFrame(geometry=[polygon])

    return gdf


def download_seascapes():
    # Download the data
    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2017-01-01T12%3A00%3A00Z&time_end=2017-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/MODISSS_2017.nc")
    print("Saving: data/seascapes/MODISSS_2017.nc")

    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2018-01-01T12%3A00%3A00Z&time_end=2018-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/MODISSS_2018.nc")
    print("Saving: data/seascapes/MODISSS_2018.nc")
    
    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2019-01-01T12%3A00%3A00Z&time_end=2019-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/MODISSS_2019.nc")
    print(f"Saving: data/seascapes/MODISSS_2019.nc")
    
    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2020-01-01T12%3A00%3A00Z&time_end=2020-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/MODISSS_2020.nc")
    print(f"Saving: data/seascapes/MODISSS_2020.nc")
    
    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2021-01-01T12%3A00%3A00Z&time_end=2021-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/MODISSS_2021.nc")
    print(f"Saving: data/seascapes/MODISSS_2021.nc")

    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2022-01-01T12%3A00%3A00Z&time_end=2022-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/MODISSS_2022.nc")
    print(f"Saving: data/seascapes/MODISSS_2022.nc")

    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH_VIIRS_OLCI/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2017-01-01T12%3A00%3A00Z&time_end=2017-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/VIIRSSS_2017.nc")
    print(f"Saving: data/seascapes/VIIRSSS_2017.nc")
    
    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH_VIIRS_OLCI/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2018-01-01T12%3A00%3A00Z&time_end=2018-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/VIIRSSS_2018.nc")
    print(f"Saving: data/seascapes/VIIRSSS_2018.nc")

    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH_VIIRS_OLCI/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2019-01-01T12%3A00%3A00Z&time_end=2019-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/VIIRSSS_2019.nc")
    print(f"Saving: data/seascapes/VIIRSSS_2019.nc")

    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH_VIIRS_OLCI/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2020-01-01T12%3A00%3A00Z&time_end=2020-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/VIIRSSS_2020.nc")
    print(f"Saving: data/seascapes/VIIRSSS_2020.nc")

    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH_VIIRS_OLCI/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2021-01-01T12%3A00%3A00Z&time_end=2021-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/VIIRSSS_2021.nc")
    print(f"Saving: data/seascapes/VIIRSSS_2021.nc")

    download_CW("https://cwcgom.aoml.noaa.gov/thredds/ncss/SEASCAPE_MONTH_VIIRS_OLCI/SEASCAPES.nc?var=CLASS&var=P&north=90&west=-180&east=180&south=-90&disableProjSubset=on&horizStride=1&time_start=2022-01-01T12%3A00%3A00Z&time_end=2022-12-15T12%3A00%3A00Z&timeStride=1&addLatLon=true&accept=netcdf", "data/seascapes/VIIRSSS_2022.nc")
    print(f"Saving: data/seascapes/VIIRSSS_2022.nc")
    
    






# -----------------------------------------------------------------------------




year = "2017"

def proc_seascape_data(year):

    # Get shapefiles for mask
    # -----------------------------------------------------------------------------
    # Load main ocean boundary data
    oceans = gpd.read_file('data/shapefiles/world_oceans/World_Seas_IHO_v3.shp')

    # ca_coast = [-130, 32.5, -114.5, 42]
    ca_coast = get_geom(-130, 32.5, -114.5, 42)

    # east_coast = [-82, 25, -65, 45]
    east_coast = get_geom(-82, 25, -65, 45)

    # Additional oceans
    gulf_of_mexico = oceans[oceans['NAME'] == 'Gulf of Mexico']
    north_pacific = oceans[oceans['NAME'] == 'North Pacific Ocean']
    south_pacific =oceans[oceans['NAME'] == 'South Pacific Ocean']
    north_atlantic = oceans[oceans['NAME'] == 'North Atlantic Ocean']
    south_atlantic = oceans[oceans['NAME'] == 'South Atlantic Ocean']
    indian_ocean = oceans[oceans['NAME'] == 'Indian Ocean']
    
    # Load data
    mdat = rxr.open_rasterio(f"data/seascapes/MODISSS_{year}.nc", crs='EPSG:4326')
    vdat = rxr.open_rasterio(f"data/seascapes/VIIRSSS_{year}.nc", crs='EPSG:4326')

    # Rename columns
    mdat = mdat.rename({'CLASS': 'MCLASS', 'P': 'MP'})
    vdat = vdat.rename({'CLASS': 'VCLASS', 'P': 'VP'})

    # Set crs
    mdat = mdat.rio.write_crs("EPSG:4326")
    vdat = vdat.rio.write_crs("EPSG:4326")

    # Check crs
    # print(mdat.rio.crs)
    # print(vdat.rio.crs)

    # Merge two netcdf files and then get difference
    gdf = xr.merge([mdat, vdat])
    
    # Mask CA Coast
    geom = ca_coast
    outdat = gdf.rio.clip(geom.geometry, all_touched=False, drop=True, invert=False, from_disk=True) 
    outdat.to_netcdf(f"data/processed/ca_coast_{year}.nc")
    print(f"Saving: data/processed/ca_coast_{year}.nc")

    # Mask East Coast
    geom = east_coast
    outdat = gdf.rio.clip(geom.geometry, all_touched=False, drop=True, invert=False, from_disk=True) 
    outdat.to_netcdf(f"data/processed/east_coast_{year}.nc")
    print(f"Saving: data/processed/east_coast_{year}.nc")

    # Mask gulf of mexico
    geom = gulf_of_mexico
    outdat = gdf.rio.clip(geom.geometry, all_touched=False, drop=True, invert=False, from_disk=True) 
    outdat.to_netcdf(f"data/processed/gulf_of_mexico_{year}.nc")
    print(f"Saving: data/processed/gulf_of_mexico_{year}.nc")

    # Mask North Pacific
    geom = north_pacific
    outdat = gdf.rio.clip(geom.geometry, all_touched=False, drop=True, invert=False, from_disk=True) 
    outdat.to_netcdf(f"data/processed/north_pacific_{year}.nc")
    print(f"Saving: data/processed/north_pacific_{year}.nc")

    # Mask South Pacific
    geom = south_pacific
    outdat = gdf.rio.clip(geom.geometry, all_touched=False, drop=True, invert=False, from_disk=True) 
    outdat.to_netcdf(f"data/processed/south_pacific_{year}.nc")
    print(f"Saving: data/processed/south_pacific_{year}.nc")

    # Mask North Atlantic
    geom = north_atlantic
    outdat = gdf.rio.clip(geom.geometry, all_touched=False, drop=True, invert=False, from_disk=True) 
    outdat.to_netcdf(f"data/processed/north_atlantic_{year}.nc")
    print(f"Saving: data/processed/north_atlantic_{year}.nc")

    # Mask South Atlantic
    geom = south_atlantic
    outdat = gdf.rio.clip(geom.geometry, all_touched=False, drop=True, invert=False, from_disk=True) 
    outdat.to_netcdf(f"data/processed/south_atlantic_{year}.nc")
    print(f"Saving: data/processed/south_atlantic_{year}.nc")

    # Mask Indian Ocean
    geom = indian_ocean.geometry
    outdat = gdf.rio.clip(geom.geometry, all_touched=False, drop=True, invert=False, from_disk=True) 
    outdat.to_netcdf("data/processed/indian_ocean_{year}.nc")
    print("Saving: data/processed/indian_ocean_{year}.nc")





# Mask global
indat = indat.to_dataframe().reset_index()
indat.to_csv('data/test_gom.csv', index=False)
cmat = confusion_matrix(indat['MCLASS'], indat['VCLASS'])

outdat = dat.to_dataframe().reset_index()
outdat = outdat.dropna(subset=['diff'])

test = outdat.dropna(subset=['diff'])

mdat.columns = ['time', 'lat', 'lon', 'MCLASS', 'MP']
vdat.columns = ['time', 'lat', 'lon', 'VCLASS', 'VP']

dat = mdat.merge(vdat, on=['time', 'lat', 'lon'], how='left')
dat = dat.assign(diff = dat['MCLASS'] - dat['VCLASS'])

# Get the data for the CLASS variable
mclass = mdat['MCLASS'].values
vclass = vdat['VCLASS'].values

# Flatten the arrays along the lat and lon dimensions while preserving the time dimension
mclass = mclass.reshape((mclass.shape[0], -1))
vclass = vclass.reshape((vclass.shape[0], -1))

# Compute the confusion matrix
conf_mat = confusion_matrix(mclass, vclass)

# Print the confusion matrix
print(conf_mat)



if __name__ == "__main__":

    # download_seascapes()

    years_ = ["2017", "2018", "2019", "2020", "2021", "2022"]

    # proc_seascape_data("2017")

    results = [proc_seascape_data(years) for years in years_]















