import pandas as pd
import json
import os
import datasets
from datasets import Dataset, DatasetDict
import random
from tqdm import tqdm
import warnings
warnings.filterwarnings(action='ignore', category=pd.errors.PerformanceWarning)
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
'''
https://huggingface.co/docs/datasets/upload_dataset
'''

def save_rows_as_json(input_csv, output_csv):
    # Read the dataframe from the csv file
    df = pd.read_csv(input_csv)

    # Fill NaN values with an empty string
    df = df.fillna('')

    # Initialize a list to store the JSON strings
    json_strings = []

    # Iterate over the rows in the dataframe
    for index, row in df.iterrows():
        # Convert the row (excluding the first column) to a dictionary
        row_dict = row.iloc[1:].to_dict()

        # Convert the dictionary to a JSON string
        json_string = json.dumps(row_dict)

        # Only add the JSON string to the list if it ends with a '}'
        if json_string.endswith('}'):
            json_strings.append(json_string)
        else:
            json_strings.append(None)

    # Add the JSON strings as a new column in the original dataframe
    df['transcription'] = json_strings

    # Remove rows where the JSON string did not end with a '}'
    df = df.dropna(subset=['transcription'])

    # Create a new dataframe with the specimen column and the JSON strings
    df_new = pd.DataFrame({
        'specimen': df.iloc[:, 0],
        'transcription': df['transcription']
    })

    # Save the new dataframe to a CSV file
    df_new.to_csv(output_csv, index=False)


def save_rows_as_json_shuffle_words(input_csv, output_csv):
    # Read the dataframe from the csv file
    df = pd.read_csv(input_csv)

    # Fill NaN values with an empty string
    df = df.fillna('')

    # Initialize lists to store the JSON strings and shuffled texts
    json_strings = []
    shuffled_texts = []

    # Iterate over the rows in the dataframe
    for index, row in df.iterrows():
        # Convert the row (excluding the first column) to a dictionary
        row_dict = row.iloc[1:].to_dict()

        # Convert the dictionary to a JSON string
        json_string = json.dumps(row_dict)

        # Only add the JSON string to the list if it ends with a '}'
        if json_string.endswith('}'):
            json_strings.append(json_string)
        else:
            json_strings.append(None)

        # Create a single string from all cells in the row, excluding the first cell
        row_text = ' '.join(str(v) for v in row_dict.values())

        # Split the row text by spaces, shuffle the parts, and then re-join with a single space
        parts = row_text.split(' ')
        random.shuffle(parts)
        shuffled_text = ' '.join(parts)

        shuffled_texts.append(shuffled_text)

    # Add the JSON strings as a new column in the original dataframe
    df['transcription'] = json_strings

    # Remove rows where the JSON string did not end with a '}'
    df = df.dropna(subset=['transcription'])

    # Create a new dataframe with the shuffled text and the JSON strings
    df_new = pd.DataFrame({
        'text': shuffled_texts,
        'transcription': df['transcription']
    })

    # Save the new dataframe to a CSV file
    df_new.to_csv(output_csv, index=False)

def save_rows_as_json_shuffle_cells(input_csv, output_csv):
    # Read the dataframe from the csv file
    df = pd.read_csv(input_csv)

    # Fill NaN values with an empty string
    df = df.fillna('')

    # Initialize lists to store the JSON strings and shuffled texts
    json_strings = []
    shuffled_texts = []

    # Iterate over the rows in the dataframe
    for index, row in df.iterrows():
        # Convert the row (excluding the first column) to a dictionary
        row_dict = row.iloc[1:].to_dict()

        # Convert the dictionary to a JSON string
        json_string = json.dumps(row_dict)

        # Only add the JSON string to the list if it ends with a '}'
        if json_string.endswith('}'):
            json_strings.append(json_string)
        else:
            json_strings.append(None)

        # Create a single string from all cells in the row, excluding the first cell
        row_text = '||'.join(str(v) for v in row_dict.values())

        # Split the row text by spaces, shuffle the parts, and then re-join with a single space
        parts = row_text.split('||')
        random.shuffle(parts)
        shuffled_text = ' '.join(parts)

        shuffled_texts.append(shuffled_text)

    # Add the JSON strings as a new column in the original dataframe
    df['transcription'] = json_strings

    # Remove rows where the JSON string did not end with a '}'
    df = df.dropna(subset=['transcription'])

    # Create a new dataframe with the shuffled text and the JSON strings
    df_new = pd.DataFrame({
        'text': shuffled_texts,
        'transcription': df['transcription']
    })

    # Save the new dataframe to a CSV file
    df_new.to_csv(output_csv, index=False)


def save_txt_rows_as_json_shuffle_cells2(input_csv, output_csv, chunksize=10000):
    # Initialize a DataFrame to store the results
    df_new = pd.DataFrame(columns=['text', 'transcription'])

    # Initialize the csv reader
    chunk_reader = pd.read_csv(input_csv, sep='\t', chunksize=chunksize, dtype=str, low_memory=False,on_bad_lines='skip')
    
    # Get the total number of chunks
    total_size = sum(1 for row in open(input_csv, 'r'))
    total_chunks = total_size // chunksize + 1

    # Iterate over the chunks
    for df in tqdm(chunk_reader, total=total_chunks):
        # Fill NaN values with an empty string
        df = df.fillna('')

        # Initialize lists to store the JSON strings and shuffled texts
        json_strings = []
        shuffled_texts = []

        # Iterate over the rows in the dataframe
        for index, row in df.iterrows():
            # Convert the row (excluding the first column) to a dictionary
            row_dict = row.iloc[1:].to_dict()

            # Convert the dictionary to a JSON string
            json_string = json.dumps(row_dict)

            # Only add the JSON string to the list if it ends with a '}'
            if json_string.endswith('}'):
                json_strings.append(json_string)
            else:
                json_strings.append(None)

            # Create a single string from all cells in the row, excluding the first cell
            row_text = '||'.join(str(v) for v in row_dict.values())

            # Split the row text by spaces, shuffle the parts, and then re-join with a single space
            parts = row_text.split('||')
            random.shuffle(parts)
            shuffled_text = ' '.join(parts)

            shuffled_texts.append(shuffled_text)

        # Add the JSON strings as a new column in the original dataframe
        df['transcription'] = json_strings

        # Remove rows where the JSON string did not end with a '}'
        df = df.dropna(subset=['transcription'])

        # Create a new dataframe with the shuffled text and the JSON strings
        df_chunk = pd.DataFrame({
            'text': shuffled_texts,
            'transcription': df['transcription']
        })

        # print(df_chunk.head(20))
        # Append the new dataframe to the results dataframe
        df_new = pd.concat([df_new, df_chunk], ignore_index=True)

    # Save the new dataframe to a CSV file
    df_new.to_csv(output_csv, index=False)

def process_chunk(df):
    # Fill NaN values with an empty string
    df = df.fillna('')

    # Initialize lists to store the JSON strings and shuffled texts
    json_strings = []
    shuffled_texts = []

    # Iterate over the rows in the dataframe
    for index, row in df.iterrows():
        # Convert the row (excluding the first column) to a dictionary
        row_dict = row.iloc[1:].to_dict()

        # Convert the dictionary to a JSON string
        json_string = json.dumps(row_dict)

        # Only add the JSON string to the list if it ends with a '}'
        if json_string.endswith('}'):
            json_strings.append(json_string)
        else:
            json_strings.append(None)

        # Create a single string from all cells in the row, excluding the first cell
        row_text = '||'.join(str(v) for v in row_dict.values())

        # Split the row text by spaces, shuffle the parts, and then re-join with a single space
        parts = row_text.split('||')
        random.shuffle(parts)
        shuffled_text = ' '.join(parts)

        shuffled_texts.append(shuffled_text)

    # Add the JSON strings as a new column in the original dataframe
    df['transcription'] = json_strings

    # Remove rows where the JSON string did not end with a '}'
    df = df.dropna(subset=['transcription'])

    # Create a new dataframe with the shuffled text and the JSON strings
    df_chunk = pd.DataFrame({
        'text': shuffled_texts,
        'transcription': df['transcription']
    })

    return df_chunk

def save_txt_rows_as_json_shuffle_cells(input_csv, output_csv):
    # Read the dataframe using Dask
    ddf = dd.read_csv(input_csv, sep='\t', dtype=str, on_bad_lines='skip')

    # Apply your function to each partition
    ddf_new = ddf.map_partitions(process_chunk)

    # Compute the results and write to CSV
    with ProgressBar():
        ddf_new = ddf_new.compute(scheduler='processes', num_workers=8)

    ddf_new.to_csv(output_csv, index=False)


def create_and_push_dataset(path_csv_in, path_csv_out, hug_name, version):

    if version == 'catalog':
        save_rows_as_json(path_csv_in, path_csv_out)

    elif version == 'shuffle_cells':
        save_rows_as_json_shuffle_cells(path_csv_in, path_csv_out)

    elif version == 'shuffle_words':
        save_rows_as_json_shuffle_words(path_csv_in, path_csv_out)

    elif version == 'txt_shuffle_cells':
        save_txt_rows_as_json_shuffle_cells(path_csv_in, path_csv_out)

    df = pd.read_csv(path_csv_out)
    dataset = Dataset.from_pandas(df)

    # To upload it to the hub
    dataset.push_to_hub(hug_name, private=True)

def push_dataset_from_scripted_csv(path_csv_out, hug_name):
    df = pd.read_csv(path_csv_out)
    dataset = Dataset.from_pandas(df)

    # To upload it to the hub
    dataset.push_to_hub(hug_name, private=True)


if __name__ == '__main__':
    # os.system("huggingface-cli login")


    # herbarium_version = 'D:/Dropbox/LeafMachine2/leafmachine2/transcription/domain_knowledge/AllAsiaMinimalasof25May2023_2__TRIMMED.csv'
    

    # scripted_version = 'D:/Dropbox/LeafMachine2/leafmachine2/transcription/domain_knowledge/scripted__AllAsiaMinimalasof25May2023_2__TRIMMED.csv' # (new file)
    # scripted_version_ba = 'D:/Dropbox/LeafMachine2/leafmachine2/transcription/domain_knowledge/scripted-ba__AllAsiaMinimalasof25May2023_2__TRIMMEDtiny.csv' # (new file)
    # scripted_version_ba_shuffle = 'D:/Dropbox/LeafMachine2/leafmachine2/transcription/domain_knowledge/scripted-ba-shuffle__AllAsiaMinimalasof25May2023_2__TRIMMEDtiny.csv' # (new file)
    
    # create_and_push_dataset(herbarium_version, scripted_version, "HLT-test-trimmed", 'shuffle_cells')
   
    # create_and_push_dataset(herbarium_version, scripted_version_ba, "HLT-test-shuffle-cells", 'shuffle_cells')

    # create_and_push_dataset(herbarium_version, scripted_version_ba_shuffle, "HLT-test-shuffle-words", 'shuffle_words')

    # '''All Asia, 21 columns'''
    # herbarium_version = '/home/brlab/Dropbox/LeafMachine2/leafmachine2/transcription/domain_knowledge/AllAsiaMinimalasof25May2023_2__TRIMMED.csv'
    # scripted_version = '/home/brlab/Dropbox/LeafMachine2/leafmachine2/transcription/domain_knowledge/scripted__HLT-AA-C21-v1.csv' # (new file)
    # create_and_push_dataset(herbarium_version, scripted_version, "HLT-AA-C21-v1", 'shuffle_cells')

    '''GBIF from 5 million records'''
    # herbarium_version = '/home/brlab/Dropbox/LM2_Env/Image_Datasets/GBIF_Ingest/GBIF/occurrence.txt'
    # scripted_version_txt_shuffle = '/home/brlab/data/HLT_Datasets/GBIF_DwC-random_order/scripted_HLT-GBIF-DwC-random-order.csv' # (new file)
    # create_and_push_dataset(herbarium_version, scripted_version_txt_shuffle, "HLT-GBIF-DwC-random-order", 'txt_shuffle_cells')
    push_dataset_from_scripted_csv(scripted_version_txt_shuffle, "HLT-GBIF-DwC-random-order")
# 
    '''BENCHMARK MICH 50'''
    bench_mich_50 = '' # (new file)
    bench_mich_50_out = '' # (new file)
    create_and_push_dataset(bench_mich_50, bench_mich_50_out, "SLTP-B50-MICH-MIpost2000", 'txt_shuffle_cells')