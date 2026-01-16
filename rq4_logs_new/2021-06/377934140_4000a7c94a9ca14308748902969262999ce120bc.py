'''
Jack's script for writing all of the coi images at once
- Jack Deschler
'''

import fetch
import coi_maps
import geopandas as gpd
import pandas as pd
import datetime

to_draw = {
    # "Michigan": ('statewide', 'test/michigan_test'), ## NOT WORKING
    # "Missouri": [('statewide', 'test/missouri_test'), ('../shp_test/stlouis/St_Louis_square.shp', 'test/stlouis_test')],
    # "Ohio": ('statewide', 'test/ohio_test2'),
    # "New Mexico": ('statewide', 'test/newmexico_test'), 
    # "Texas": ('statewide', 'test/texas_test'),
    # "Utah": ('statewide', 'test/utah_test'),
    # "Virginia": ('statewide', 'test/virginia_test'), ## NOT WORKING
    "Wisconsin": ('statewide', 'test/wisconsin_test')
}

def parse_date(d):
    lst = d.split(' ')[1:4]
    months = {
        'Jan': 1,
        'Feb': 2,
        'Mar': 3,
        'Apr': 4,    
        'May': 5,
        'Jun': 6,    
        'Jul': 7,
        'Aug': 8,    
        'Sep': 9,
        'Oct': 10,
        'Nov': 11,
        'Dec': 12
    }
    try:
        return datetime.date(int(lst[2]), months[lst[0]], int(lst[1]))
    except:
        print(d)

# data is list of (geom, outfile) tuples
def create_coi_maps(state, data):
    if not isinstance(data, list):
        data = [data]
    link = state.lower().replace(" ", "")
    print(f'{len(data)} set(s) to print in {state.upper()}')

    # read the COI dataframe
    ids = f"https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/districtr-ids/{link}"
    plan = f"https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/{link}?type=plan&length=10000"
    cois = f"https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/{link}?type=coi&length=10000"
    written = f"https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/{link}?type=written&length=10000"

    if state == 'Michigan':
        ids = "https://qp2072772f.execute-api.us-east-2.amazonaws.com/dev/submissions/districtr-ids"
        plans = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/prod/submissions/csv?type=plan&length=10000"
        cois = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/prod/submissions/csv?type=coi&length=10000"
        written = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/prod/submissions/csv?type=written&length=10000"


    
    _, coi_df, _ = fetch.submissions(ids, plan, cois, written)

    coi_df['date'] = coi_df['datetime'].apply(parse_date)
    cumulative = coi_maps.assignment_to_shape(coi_df)
    if not isinstance(cumulative, pd.DataFrame):
        print(f"Done with {state.upper()}\n\n")
        return
        
    weekly = coi_df[coi_df['date'] > (datetime.date.today() - datetime.timedelta(days = 7))]

    
    for (geom, outfile) in data:
        print(f"Mapping {outfile}")
        # figure out if geom is a state name or a shapefile
        osm = False
        clip = state
        if geom != "statewide":
            clip = gpd.read_file(geom)
            osm = True
    
        coi_maps.plot_coi_boundaries(cumulative, clip, osm = osm, outfile = f'{outfile}_boundaries.png', show = False)
        coi_maps.plot_coi_heatmap(cumulative, clip, osm = osm, outfile = f'{outfile}_heatmap.png', show = False)
        
        today = str(datetime.date.today()).replace('-','_')
        coi_maps.plot_coi_boundaries(cumulative, clip, osm = osm, outfile = f'{outfile}__weekly{today}_boundaries.png', show = False)
        coi_maps.plot_coi_heatmap(cumulative, clip, osm = osm, outfile = f'{outfile}_weekly{today}_heatmap.png', show = False)
        
    print(f"Done with {state.upper()}\n\n")

def main():
    for s in to_draw.keys():
        create_coi_maps(s, to_draw[s])

if __name__ == "__main__":
    main()