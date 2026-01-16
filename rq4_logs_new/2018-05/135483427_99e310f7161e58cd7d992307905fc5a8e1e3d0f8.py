"""Read and clean data for customer pricing to write to new csv."""
import csv


def clean_and_write_prices(in_csv, out_csv):
    """Read dirty pricing csv, clean data, then write to output csv."""
    with open(in_csv, encoding='ISO-8859-1') as in_file, open(out_csv, 'w') as out_file:
        reader = csv.reader(in_file)
        writer = csv.writer(out_file)
        writer.writerow(['customer', 'pricing_rule', 'price'])

        for row in reader:
            if 'Customers Description' in row[0]:
                curr_cust = row[0].strip()
            writer.writerow([curr_cust, row[0], row[1]])
        print('Output complete')


if __name__ == '__main__':
    # change file names when calling function below and make sure paths are correct
    clean_and_write_prices('Clean_Pricing_Tab_2.csv', 'out.csv')