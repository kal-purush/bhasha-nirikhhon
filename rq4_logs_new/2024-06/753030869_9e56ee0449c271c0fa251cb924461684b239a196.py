import os
import csv

directory = 'D:\workspace\Pathfinder-Analysis\sample'

section_percent = 0.6
industry_set = set()
category_set = set()
industry_fund_map = dict()
for subdir in os.listdir(directory):
    sub = os.path.join(directory, subdir)
    # checking if it is a file
    print(subdir)
    for filename in os.listdir(sub):
        f = os.path.join(sub, filename)
        ticker = filename
        if (filename=="Background.csv"):
            with open(f, encoding="utf8") as file:
                csvreader = csv.reader(file)
                header =next(csvreader)
                value = next(csvreader)
                category = value[4]
                category_set.add(category)

        if (filename=="Industry.csv"):
            with open(f, encoding="utf8") as file:
                csvreader = csv.reader(file)
                header = next(csvreader)
                top = next(csvreader)
                industry = top[1]
                percentage_str = top[3]
                percentage_float = float(percentage_str.strip('%'))/100
                industry_set.add(industry)
                if percentage_float > section_percent:
                    industry_fund_map[ticker] = industry

                for row in csvreader:
                    industry = row[1]
                    industry_set.add(industry)

print(industry_fund_map)
print(industry_set)
print(category_set)



