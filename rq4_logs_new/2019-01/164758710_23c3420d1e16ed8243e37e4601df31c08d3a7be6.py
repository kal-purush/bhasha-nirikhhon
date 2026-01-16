import csv
import json

"""
who: Chelsey Humphreys
when: 1.15.2019
what: a1 assignment
"""

plane_data = []      # this is to collect data for printing at the very end.
plane_total_count = 0    # for counting the rows of data
with open('data1/planecrashinfo_20181121001952.csv', 'r') as csvfile:

    # parse csv file pointer into plane_stream
    plane_stream = csv.DictReader(csvfile, delimiter=',', quotechar='"')

    # interate over plane_stream to process.
    for plane_data_row in plane_stream:
        plane_total_count += 1

        plane_location = plane_data_row['location']
        plane_operator = plane_data_row['operator']
        plane_flight_no = plane_data_row['flight_no']

        # only show the first 10 rows.
        if plane_total_count <= 10:
            print(plane_total_count, plane_location,plane_operator, plane_flight_no)
            plane_data.append({plane_total_count: [plane_location,plane_operator, plane_flight_no]
                })


print("I found " + str(plane_total_count) + " planes.")
print("I am all done processing data!!!")

print(json.dumps(plane_data))