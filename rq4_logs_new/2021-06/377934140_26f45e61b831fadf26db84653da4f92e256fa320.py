import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import us

# lookup for mggg-states shapefile raw links
mggg_states = {
    'Ohio': 'https://github.com/mggg-states/OH-shapefiles/blob/master/OH_precincts.zip?raw=true',
    'Alaska': 'https://github.com/mggg-states/AK-shapefiles/blob/master/AK_precincts.zip?raw=true',
    'Michigan': 'https://github.com/mggg-states/MI-shapefiles/blob/main/MI.zip?raw=true',
    'Wisconsin20': 'https://github.com/mggg-states/WI-shapefiles/blob/master/WI_2020_wards.zip?raw=true',
    'Wisconsin': 'https://github.com/mggg-states/WI-shapefiles/blob/master/WI_2011_wards.zip?raw=true',
    'Maryland': 'https://github.com/mggg-states/MD-shapefiles/blob/master/MD_precincts.zip?raw=true',
    'North Carolina': 'https://github.com/mggg-states/NC-shapefiles/blob/master/NC_VTD.zip?raw=true',
    'New Hampshire': 'https://github.com/mggg-states/NH-shapefiles/blob/main/NH.zip?raw=true',
    'Virginia': 'https://github.com/mggg-states/VA-shapefiles/blob/master/VA_precincts.zip?raw=true',
    'Massachusetts': 'https://github.com/mggg-states/MA-shapefiles/blob/master/MA_precincts_12_16.zip?raw=true',
    'Indiana': 'https://github.com/mggg-states/IN-shapefiles/blob/main/Indiana.zip?raw=true',
    'Puerto Rico': 'https://github.com/mggg-states/PR-shapefiles/blob/main/PR.zip?raw=true',
    'Nebraska': 'https://github.com/mggg-states/NE-shapefiles/blob/main/NE.zip?raw=true',
    'Maine': 'https://github.com/mggg-states/ME-shapefiles/blob/master/Maine.zip?raw=true',
    'Pennsylvania': 'https://github.com/mggg-states/PA-shapefiles/blob/master/PA.zip?raw=true',
    'Louisiana': 'https://github.com/mggg-states/LA-shapefiles/blob/main/LA_1519.zip?raw=true',
    'Minnesota': 'https://github.com/mggg-states/MN-shapefiles/blob/master/MN12_18.zip?raw=true',
    'Delaware': 'https://github.com/mggg-states/DE-shapefiles/blob/master/DE_precincts.zip?raw=true',
    'Arizona': 'https://github.com/mggg-states/AZ-shapefiles/blob/master/az_precincts.zip',
    'Connecticut': 'https://github.com/mggg-states/CT-shapefiles/blob/master/CT_precincts.zip?raw=true',
    'Georgia': 'https://github.com/mggg-states/GA-shapefiles/blob/master/GA_precincts.zip?raw=true',
    'Hawaii': 'https://github.com/mggg-states/HI-shapefiles/blob/master/HI_precincts.zip?raw=true',
    'Colorado': 'https://github.com/mggg-states/CO-shapefiles/blob/master/CO_precincts.zip?raw=true',
    'Oklahoma': 'https://github.com/mggg-states/OK-shapefiles/blob/master/OK_precincts.zip?raw=true',
    'Utah': 'https://github.com/mggg-states/UT-shapefiles/blob/master/UT_precincts.zip?raw=true',
    'Oregon': 'https://github.com/mggg-states/OR-shapefiles/blob/master/OR_precincts.zip?raw=true',
    'New Mexico': 'https://github.com/mggg-states/NM-shapefiles/blob/master/new_mexico_precincts.zip?raw=true',
    'Missouri': 'https://github.com/mggg-states/MO-shapefiles/blob/master/MO_vtds.zip?raw=true',
    'Vermont': 'https://github.com/mggg-states/VT-shapefiles/blob/master/VT_towns.zip?raw=true',
    'Texas': 'https://people.csail.mit.edu/ddeford/TX_vtds.zip',
    'Rhode Island': 'https://github.com/mggg-states/RI-shapefiles/blob/master/RI_precincts.zip?raw=true',
    'Iowa': 'https://github.com/mggg-states/IA-shapefiles/blob/master/IA_counties.zip?raw=true'
}

# takes in an assignment df (coi_df from fetch) and spits it out with geometries
def assignment_to_shape(df):
    # add a units row to the df
    crs = None
    df['units'] = df['districtr_data'].apply(lambda x: x['plan']['units']['id'])
    state = df.iloc[0]['districtr_data']['plan']['place']['state']
    fips = us.states.lookup(state).fips
    
    acc = pd.DataFrame(columns = ['id', 'plan_id', 'coi_id', 'tile_id', 'geometry'])
    # iterate over units
    for unit in set(df['units']):
        print(f'Downloading shapefile for {unit}')
        # download appropriate shape
        if unit == "blockgroups":
            link = f'https://www2.census.gov/geo/pvs/tiger2010st/{fips}_{state}/{fips}/tl_2010_{fips}_bg10.zip'
        elif unit == "blocks":
            link = f'https://www2.census.gov/geo/pvs/tiger2010st/{fips}_{state}/{fips}/tl_2010_{fips}_tabblock10.zip'
        else:
            if state == 'Wisconsin' and unit == '2020 Wards':
                link = mggg_states['Wisconsin20']
            else:
                link = mggg_states[state]
        shp = gpd.read_file(link)
        print("Have shapefile.")

        # get everything into the same crs
        if not crs:
            crs = shp.crs
            print(f'Projecting into crs {crs.to_epsg()}')
        else:
            shp = shp.to_crs(crs)

        subset = df[df['units'] == unit]
        print(f'{len(subset)} submissions using {unit}\n')
        # each COI is a row
        for idx, row in subset.iterrows():
            # get all info
            plan_id = row['plan_id']
            key = row['districtr_data']['plan']['idColumn']['key']
            try:
                asn = row['districtr_data']['plan']['assignment']
            except KeyError: # empty plan
                print("Empty plan...")
                continue
            # make lists
            ids = []
            plan_ids = []
            coi_ids = []
            tile_ids = []
            geoms = []
            for k, v in asn.items():
                if isinstance(v, list):
                    for v_prime in v:
                        ids.append(f'{plan_id}-{v_prime}')
                        plan_ids.append(plan_id)
                        coi_ids.append(v_prime)
                        tile_ids.append(k)
                        geoms.append(shp[shp[key] == k]['geometry'].iloc[0])
                else:
                    ids.append(f'{plan_id}-{v}')
                    plan_ids.append(plan_id)
                    coi_ids.append(v)
                    tile_ids.append(k)
                    geoms.append(shp[shp[key] == k]['geometry'].iloc[0])
            tmp = pd.DataFrame(zip(ids, plan_ids, coi_ids, tile_ids, geoms), 
                               columns = ['id', 'plan_id', 'coi_id', 'tile_id', 'geometry'])
            acc = acc.append(tmp, ignore_index = True)
    return gpd.GeoDataFrame(acc, crs = crs)
        
# in these, clip_bounds can either be a capitalized state name or a geometry to clip to
def plot_coi_boundaries(coi_df, clip_bounds):
    if isinstance(clip_bounds, str):
        state_gdf = gpd.read_file('https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_5m.zip')
        clip_bounds = state_gdf[state_gdf['NAME'] == clip_bounds].to_crs(coi_df.crs)
    clipped = gpd.clip(coi_df, clip_bounds)
    fig, ax = plt.subplots(figsize = (20,10))
    dissolved = gpd.clip(coi_df.dissolve(by = 'id'), clip_bounds)
    ax.set_axis_off()
    dissolved.boundary.plot(ax = ax, cmap = 'tab20')
    clipped.plot(ax = ax, column = 'id', cmap = 'tab20', alpha = 0.5)
    clip_bounds.boundary.plot(ax = ax, color = 'black', linewidth = 2)
    plt.show()

def plot_coi_heatmap(coi_df, clip_bounds, color = 'purple'):
    if isinstance(clip_bounds, str):
        state_gdf = gpd.read_file('https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_5m.zip')
        clip_bounds = state_gdf[state_gdf['NAME'] == clip_bounds].to_crs(coi_df.crs)
    clipped = gpd.clip(coi_df, clip_bounds)
    fig, ax = plt.subplots(figsize = (20,10))
    ax.set_axis_off()
    clip_bounds.boundary.plot(ax = ax, color = 'black', linewidth = 2)
    clipped.plot(ax = ax, color = color, alpha = 0.2)
    plt.show()