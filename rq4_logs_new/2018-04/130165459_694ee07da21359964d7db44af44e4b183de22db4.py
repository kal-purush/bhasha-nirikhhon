import re
import json
import operator
Grid_data = json.load(open('melbGrid.json'))
grid_counts = {}
rank_by_row = {}
rank_by_col = {}
regex = r'"coordinates"\:\[(.+?)\]'
for j in range(0,16):
    key = Grid_data["features"][j]["properties"]["id"]
    grid_counts[key] = 0
with open("tinyInstagram.json",'r',encoding='UTF-8') as f:
    for line in f:
        if re.search(regex, line):
            found = re.search(regex,line).group(1)
            ycoordinate = float(found.split(',')[0])
            xcoordinate = float(found.split(',')[1])
            for j in range(0,16):
                if xcoordinate>=Grid_data["features"][j]["properties"]["xmin"] and xcoordinate<=Grid_data["features"][j]["properties"]["xmax"] and ycoordinate>=Grid_data["features"][j]["properties"]["ymin"] and ycoordinate<=Grid_data["features"][j]["properties"]["ymax"]:
                    grid_counts[Grid_data["features"][j]["properties"]["id"]] = grid_counts[Grid_data["features"][j]["properties"]["id"]]+1
rank_by_grid = sorted(grid_counts.items(), key=operator.itemgetter(1),reverse = True)
row_val_A = grid_counts["A1"] + grid_counts["A2"] + grid_counts["A3"] + grid_counts["A4"]
row_val_B = grid_counts["B1"] + grid_counts["B2"] + grid_counts["B3"] + grid_counts["B4"]
row_val_C = grid_counts["C1"] + grid_counts["C2"] + grid_counts["C3"] + grid_counts["C4"] + grid_counts["C5"]
row_val_D = grid_counts["D3"] + grid_counts["D4"] + grid_counts["D5"]
rank_by_row['A-Row'] = row_val_A
rank_by_row['B-Row'] = row_val_B
rank_by_row['C-Row'] = row_val_C
rank_by_row['D-Row'] = row_val_D
rank_by_row = sorted(rank_by_row.items(), key=operator.itemgetter(1),reverse = True)
col_val_1 = grid_counts["A1"] + grid_counts["B1"] + grid_counts["C1"]
col_val_2 = grid_counts["A2"] + grid_counts["B2"] + grid_counts["C2"]
col_val_3 = grid_counts["A3"] + grid_counts["B3"] + grid_counts["C3"] + grid_counts["D3"]
col_val_4 = grid_counts["A4"] + grid_counts["B4"] + grid_counts["C4"] + grid_counts["D4"]
col_val_5 = grid_counts["C5"] + grid_counts["D5"]
rank_by_col['Col-1'] = col_val_1
rank_by_col['Col-2'] = col_val_2
rank_by_col['Col-3'] = col_val_3
rank_by_col['Col-4'] = col_val_4
rank_by_col['Col-5'] = col_val_5
rank_by_col = sorted(rank_by_col.items(), key=operator.itemgetter(1),reverse = True)
print(rank_by_grid,rank_by_row,rank_by_col, sep="\n")