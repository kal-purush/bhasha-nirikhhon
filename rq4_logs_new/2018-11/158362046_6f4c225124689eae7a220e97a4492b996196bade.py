import csv
from matplotlib import pyplot as plt
from ast import literal_eval as make_tuple

lat_lon = []

with open("contours-simplifies-des-departements-francais-2015.csv", "r") as f:
    reader = csv.DictReader(f, delimiter = ";")
    for row in reader:
        lat_lon.append(row["Geo Point"])

lat_lon_tuples = []
for couple in lat_lon:
    x, y = make_tuple(couple)
    if x > 40:
        lat_lon_tuples.append(make_tuple(couple))
    else: 
        pass

# keep only data for x between 40 and 60

for (x, y) in lat_lon_tuples:
    plt.scatter(x, y, marker = "o")

plt.title("Un couple (latitude, longitude) par département français")
plt.show()
# print(lat_lon_tuples)