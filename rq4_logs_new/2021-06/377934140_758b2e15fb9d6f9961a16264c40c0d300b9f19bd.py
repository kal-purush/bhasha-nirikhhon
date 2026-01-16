import fetch
import coi_maps
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import us
import fetch
import contextily as ctx

pref_units = {
    "Michigan": 'blockgroups',
    "Missouri": 'blockgroups',
    "New Mexico": 'precincts',
    "Ohio": 'blockgroups',
    "Texas": 'precincts',
    "Utah": 'blocks',
    "Virginia": 'precincts',
    "Wisconsin": 'wards'
}

def assignment_to_pivot(df, outfile = None):
    # add a units col to the df
    df['units'] = df['districtr_data'].apply(lambda x: x['plan']['units']['id'])
    try:
        state = df.iloc[0]['districtr_data']['plan']['place']['state']
    except:
        print(f"ERROR: {len(df)} COI SUBMISSIONS")
        return None
    unit = pref_units[state]
    
    fips = us.states.lookup(state).fips
    
    acc = pd.DataFrame(columns = ['id', 'plan_id', 'coi_id', 'tile_id', 'geometry'])
        
    # download appropriate shape
    if unit == "blockgroups":
        link = f'https://www2.census.gov/geo/pvs/tiger2010st/{fips}_{state}/{fips}/tl_2010_{fips}_bg10.zip'
    elif unit == "blocks":
        link = f'https://www2.census.gov/geo/pvs/tiger2010st/{fips}_{state}/{fips}/tl_2010_{fips}_tabblock10.zip'
    else:
        link = coi_maps.mggg_states[state]
    shp = gpd.read_file(link)
    print("Have shapefile.")
    
    subset = df[df['units'] == unit]
    key = subset.iloc[0]['districtr_data']['plan']['idColumn']['key']
    if state == 'Wisconsin':
        key = "Code-2"


    # cast everything to int (and do some error checking)
    try:
        shp[key] = shp[key].apply(int)
    except KeyError:
        if key == "GEOID":
            try:
                key = "GEOID10"
                shp[key] = shp[key].apply(int)
            except KeyError:
                print("ERROR: GEOID and GEOID10 not in shapefile.")
                return None
        else:
            print(f"ERROR: {key} not in shapefile.")
            return None
    except ValueError:
        shp[key] = shp[key] # can't be turned to an int (not a GEOID)
            

    # get everything into the same crs
    crs = shp.crs
    
    tiles = list(shp[key].apply(str))
    pivot = pd.DataFrame(columns = tiles)

    # each COI is a row
    for idx, row in subset.iterrows():
        # get all info
        plan_id = row['plan_id']
        row_key = row['districtr_data']['plan']['idColumn']['key']
        if state == "Wisconsin" and row_key == "GEOID10" and unit == "wards":
            #print("Skipping a plan because it is on old WI wards")
            continue

        try:
            asn = row['districtr_data']['plan']['assignment']
        except KeyError: # empty plan
            #print("Empty plan...")
            continue

        # make lists
        assigned = asn.keys()
        distinct_cois = {}
        for tile in assigned:
            tmp = asn[tile]
            if not isinstance(tmp, list):
                tmp = [tmp]
            for coi in tmp:
                if coi not in distinct_cois.keys():
                    distinct_cois[coi] = []
                distinct_cois[coi].append(tile)
        
        plan_ids = [f'{plan_id}-{d+1}' for d in distinct_cois.keys()]
        acc = pd.DataFrame(index = plan_ids, columns = tiles)
        for (d, p) in zip(distinct_cois.keys(), plan_ids):
            for t in distinct_cois[d]:
                acc.at[p, t] = 1
        pivot = pivot.append(acc)
        
    pivot = pivot.fillna(0)
    pivot = pivot.T
    if outfile:
        pivot.to_csv(outfile)
    return pivot