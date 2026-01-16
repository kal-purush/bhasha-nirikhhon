import os

import pandas as pd
import pickle

if __name__ == '__main__':
    dtype = {'rq': str, 'lb': int, 'cmzh': str, 'csfzl': str}
    left_files = [
        ('input.xlsx', 'Sheet2'),
        ('input.xlsx', 'Sheet3'),
        ('input.xlsx', 'Sheet4'),
        ('input.xlsx', 'Sheet5'),
        ('input.xlsx', 'Sheet6'),
    ]
    right_files = [
        ('input.xlsx', 'Sheet1'),
    ]
    output_filename = 'output.csv'

    PICKLE_FILE = 'cache.pkl'
    if os.path.exists(PICKLE_FILE):
        print(f'File {PICKLE_FILE} exists. Skip reading excel files. Delete this file to update data.')

        with open(PICKLE_FILE, 'rb') as f:
            df_inputs = pickle.load(f)
    else:
        print('Read left files ...')
        df_left = [pd.read_excel(filename, sheet_name=sheet_name, dtype=dtype) for filename, sheet_name in left_files]
        print('Read right files ...')
        df_right = [pd.read_excel(filename, sheet_name=sheet_name, dtype=dtype) for filename, sheet_name in right_files]
        df_inputs = {'left': df_left, 'right': df_right}
        with open(PICKLE_FILE, 'wb') as f:
            pickle.dump(df_inputs, f)
    print('Merging ...')
    df_right = pd.concat(df_inputs['right'])
    for i, left_file in enumerate(left_files):
        excel_name, sheet_name = left_file
        print(f'Processing {excel_name} {sheet_name} ...')
        df = df_inputs['left'][i].merge(df_right, how="left", on="cmzh")
        df.to_csv(f'{sheet_name}.csv', encoding='utf-8')