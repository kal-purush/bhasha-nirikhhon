#!/bin/env python3

import argparse
import csv

def formatted(x):
    return "{:.2f}".format(float(x))

def sort_by_p(x):
    return float(x['p'])

def main(args):
    DEBUG = args.debug

    with open(args.csv) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')

        values_for = {}

        print(",".join(['cluster','feature','p','fold.change','original_row_num']))
        for row in csv_reader:
            row_num = int(row.pop(''))
            cluster = row.pop('cluster')

            if cluster in values_for:
                pass
            else:
                values_for[cluster] = {}

            output_string = f"{cluster},{row['feature']},{formatted(row['p'])},{formatted(row['fold.change'])},{row_num}"

            if DEBUG:
                p_value_threshold_and_value = f"{formatted(args.p_value_cutoff)} ({formatted(row['p'])})"

            if float(row['p']) < args.p_value_cutoff:
                row['p'] = float(row['p'])
                row['fold.change'] = float(row['fold.change'])
                values_for[cluster][row_num] = row
                if DEBUG:
                    print(f"p_value less than {p_value_threshold_and_value}")
            else:
                if DEBUG:
                    print(f"p_value equal or greater than {p_value_threshold_and_value}")
                pass

            if DEBUG:
                print(f'values for "row_name, p_value, fold_change, cluster, feature" are "{output_string}"')

        for cluster in sorted(values_for,key=int):

            for row_num in sorted(values_for[cluster],key=lambda x: values_for[cluster][x]['p']):
                z = values_for[cluster][row_num]
                print(f"{cluster},{z['feature']},{formatted(z['p'])},{formatted(z['fold.change'])},{row_num}")

# command line interface (making this a modulino)
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                description='Filter list by p_value'
             )
    parser.add_argument(
        'csv',
        type=str,
        help='Name of csv flie',
    )
    parser.add_argument(
        '--debug',
        dest='debug',
        action='store_true',
        default=False,
    )
    parser.add_argument(
        '--p_value_column',
        type=str,
        default='3',
        help='Column containing p_values to filter by',
    )
    parser.add_argument(
        '--p_value_cutoff',
        type=float,
        default=0.05,
        help='Default p_value threshold',
    )

    args = parser.parse_args()

    main(args)