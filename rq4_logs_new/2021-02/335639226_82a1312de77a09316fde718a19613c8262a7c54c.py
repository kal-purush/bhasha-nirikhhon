
import csv
import matplotlib.pyplot as plt

filename = '/Users/carolynwhelpley/covid/data/CasesByDate.csv'
with open(filename) as f:
    reader = csv.reader(f)
    header_row = next(reader)

    # for index, column_header in enumerate(header_row):
    #print(index, column_header)

    cases = []
    for row in reader:
        print(row[3])
        case = (row[2])
        cases.append(case)

    

    plt.style.use('default')
    fig, ax = plt.subplots()
    ax.plot(cases, c='red')

    plt.title("Daily cases for MA, covid 19", fontsize=24)
    plt.xlabel(" ", fontsize=16)
    plt.ylabel("Case number", fontsize=16)
    plt.tick_params(axis='both', which='major', labelsize=16)

    plt.show()