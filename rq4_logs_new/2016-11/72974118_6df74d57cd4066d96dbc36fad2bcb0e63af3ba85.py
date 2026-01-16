import tablib
import csv
import sys


def csv2xls(csv_name, file_name):
    with open(csv_name, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        header = reader.__next__()
        data = tablib.Dataset(headers=header)
        for row in reader:
            data.append(row)

        with open(file_name, 'wb') as xf:
            xf.write(data.xls)


if __name__ == "__main__":
    params = sys.argv[1:]
    try:
        csv2xls(params[0], params[1])
    except:
        print("Run the program by")
        print("python csv2xls.py csv_name.csv xls_name.xls")
        raise