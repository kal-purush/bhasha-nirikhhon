
import os
import csv
import re

def get_ventilation(data_path):
    """Find average ventilation factor for given run"""
    
    # differentiate between T1 and T5 hulls
    static_draft = 0
    if data_path.find("T1") > 0:
        static_draft = 0.029
    else:
        static_draft = 0.052
    
    with open(data_path, 'r') as data:
        data_reader = csv.reader(data, delimiter=',')
        steady_data = list(data_reader)[400:1100]
    
    sum_elevations = [0 for i in range(7)]
    for row in steady_data:
        num_row = [float(i) for i in row]
        sum_elevations = map(sum, zip(sum_elevations, num_row))
    elevations = [elevation / len(steady_data) for elevation in sum_elevations]
        
    drafts = [static_draft + elevation for elevation in elevations]
    ventilations = [(static_draft - draft) / static_draft for draft in drafts]
    return ventilations



# scan all T5 runs
for filename in os.listdir("../data/2016-06-29_T5"):
    if filename != "Thumbs.db":
        data_path = "../data/2016-06-29_T5/" + filename
        fn_match = re.search('-R(.*)A1V', filename)
        fn = float(fn_match.group(1))
        ventilations = get_ventilation(data_path)
        print(data_path)
        with open("../data/ventilation/2016-06-29_T5.csv", 'a', newline='') as data:
            write = csv.writer(data)
            row = [fn] + ventilations
            write.writerows([row])
        print("Data %s analysis complete." % filename)
        
# scan all T1 runs
for filename in os.listdir("../data/2016-06-27_T1"):
    if filename != "Thumbs.db":
        data_path = "../data/2016-06-27_T1/" + filename
        fn_match = re.search('-R(.*)A1V', filename)
        fn = float(fn_match.group(1))
        ventilations = get_ventilation(data_path)
        print(data_path)
        with open("../data/ventilation/2016-06-27_T1.csv", 'a', newline='') as data:
            write = csv.writer(data)
            row = [fn] + ventilations
            write.writerows([row])
        print("Data %s analysis complete." % filename)
