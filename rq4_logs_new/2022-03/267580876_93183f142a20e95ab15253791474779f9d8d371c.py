from . import GLPI
import argparse
import sys
import csv

parser = argparse.ArgumentParser(description='Fetch list of items from GLPI')
parser.add_argument('item_type')
parser.add_argument(
    '--format',
    help='Output format',
    choices=['csv', 'tsv', 'json'],
    default='tsv',
)
parser.add_argument(
    '--columns',
    help='Columns to show (comma-separated)',
    default='otherserial,name',
)

args = parser.parse_args()
glpi_api = GLPI()

out = []
for result in glpi_api(args.item_type).GET().ranges:
    out.extend([item[col] for col in args.columns.split(',')] for item in result.json())

if args.format == 'json':
    import json
    json.dump(out, sys.stdout)
else:
    writer = csv.writer(
        sys.stdout,
        delimiter=',' if args.format == 'csv' else '\t',
        quoting=csv.QUOTE_MINIMAL,
    )
    writer.writerows(out)