import pandas as pd
import argparse
import os
import glob

def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('paths', nargs='+', default=os.getcwd())
    parser.add_argument('merge_column')
    parser.add_argument('-e', '--extension', default="csv", required=False) 
    parser.add_argument('-f', '--result_file_name', default="joined", required=False)#remember to append the ext argument to this for output
    parser.add_argument('-n', '--na_value', default="NA", required=False)
    parser.add_argument('-s', '--column_separator', default=None, required=False)
    args=parser.parse_args()
    return args

def get_all_files_in_directory_with_extension(path: str, ext: str) -> list:
    return glob.glob(path+"/*."+ext)

def get_input_files(paths: list[str], extension:str)->list[str]:
    filtered_file_paths = []
    for path in paths:
        globbed = glob.glob(path)
        for globbed_path in globbed:
            if os.path.isdir(globbed_path):
                filtered_file_paths.extend(get_all_files_in_directory_with_extension(globbed_path, extension))
            elif os.path.isfile(globbed_path):
                if globbed_path.endswith(extension):
                    filtered_file_paths.append(globbed_path)
    return filtered_file_paths

def remove_df_columns_where_header_starts_with_unnamed(df: pd.DataFrame) -> pd.DataFrame:
    #filter out unnamed columns, weirdness with pandas
    return df.loc[:, df.columns.str.contains('^Unnamed') == False]

def read_file(path: str, col_separator = None) -> pd.DataFrame:
    df = pd.read_csv(path, sep=col_separator, header=0, index_col=0, engine='python')
    df = remove_df_columns_where_header_starts_with_unnamed(df)
    return df

def merge_dfs_on_column(df1: pd.DataFrame, df2: pd.DataFrame, merge_column: str) -> pd.DataFrame:
    if df1.empty:
        return df2
    return pd.merge(df1, df2, how='outer', left_on = merge_column, right_on = merge_column)

def move_merge_column_to_front(df: pd.DataFrame, merge_column: str) -> pd.DataFrame:
    merge_column_values = df[merge_column]
    df.drop(columns=[merge_column], inplace=True)
    df.insert(loc=0, column=merge_column, value=merge_column_values)
    return df

def main() -> None:
    args = read_args()
    extension = args.extension.lstrip('.')
    merge_column = args.merge_column
    col_separator = args.column_separator
    result_path = args.result_file_name+"."+extension
    input_files = get_input_files(args.paths, extension)
    na_value = args.na_value

    result = pd.DataFrame()

    for input_file in input_files:
        df = read_file(input_file, col_separator)
        result = merge_dfs_on_column(result, df, merge_column)

    
    merge_column_values = result[merge_column]
    result.drop(columns=[merge_column], inplace=True)
    result.insert(loc=0, column=merge_column, value=merge_column_values)

    result.to_csv(f"{result_path}", sep='\t',na_rep=na_value,float_format="%.0f", index=False)

if __name__ == "__main__":
    main()