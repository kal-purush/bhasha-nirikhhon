import csv

#headers = ['paramter_id', 'strategy_id', 't', 's', 'R1', 'R2', 'R12'] used for population table
headers = ['paramter_id', 'strategy_id', 't', 'drug1', 'drug2']
data = []
NEW_LINE_VALUE = 2 # Used to increment t while keeping parameter and strategy ids

# Ensure file doesn't contain header values -- just data
fileName = "/Users/cutlersimpson/Personal/School/DPM-SRI/ignored_files/dosage_ALLDRUG_0 - original data.csv"

with open(fileName, "r") as f:
    reader = csv.reader(f, delimiter="\t")
    for line in reader:
        row = line[0]
        cells = row.split(',')

        rowData = []

        tValue = 45
        parameterId = int(cells[0]) + 1
        strategyId = int(cells[1]) + 1
        counter = 0

        rowData.append(parameterId)
        rowData.append(strategyId)
        rowData.append(tValue)

        for index, cell in enumerate(cells[2:]):
            rowData.append(cell)
            counter += 1

            if (counter == NEW_LINE_VALUE):
                rowCopy = rowData.copy()
                data.append(rowCopy)
                rowData.clear()
                tValue += 45
                if (tValue > 1800):
                    break
                rowData.append(parameterId)
                rowData.append(strategyId)
                rowData.append(tValue)
                counter = 0

        data.append(rowData)

writeFileName = "/Users/cutlersimpson/Personal/School/DPM-SRI/ignored_files/dosage-file.csv"

with open(writeFileName, "w") as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(data)
