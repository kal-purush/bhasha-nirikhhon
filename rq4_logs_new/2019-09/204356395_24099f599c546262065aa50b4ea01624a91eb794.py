import os
import Levenshtein
import pandas as pd

CSV_DIR = './csv_tables'
PREDICTIONS = 'Predictions1_1.csv'
METRICS = 'Metrics.csv'


def main():
    prediction_file = os.path.join(CSV_DIR, PREDICTIONS)
    with open(prediction_file, 'r') as prf:
        pre = pd.read_csv(prf).set_index('ID')

    pre['Length'] = pre['LABEL'].map(len)
    pre['Levin'] = pre.apply(lambda row: Levenshtein.distance(row['LABEL'], row['PREDICTION']), axis=1)
    pre['Subm'] = pre.apply(lambda row: row['Levin'] / row['Length'], axis=1).round(decimals=2)
    pre['Readable'] = pre.apply(lambda row: row['Subm'] < 1/3, axis=1)

    metrics_file = os.path.join(CSV_DIR, METRICS)
    pre[['Levin', 'Subm', 'Readable']].to_csv(metrics_file)

    readable, total = pre["Readable"].sum(), pre["Readable"].count()
    print(f'Consider readable {readable} of {total} files, {readable / total * 100:3.2f}%')


if __name__ == '__main__':
    main()