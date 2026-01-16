import os
import pandas as pd
import chardet
import json
import re
import time
from airtable import Airtable
from dotenv import load_dotenv
from urllib.parse import urlparse

__version__ = 1.0

load_dotenv()

input_folder = 'input_files'
csv_columns = [
    'Referring page URL', 'Language', 'Platform', 'Referring page HTTP code',
    'Target URL', 'Type', 'Content', 'Nofollow', 'UGC', 'Sponsored',
    'Rendered', 'Lost status', 'Lost'
]
boolean_columns = ['Type', 'Content', 'Nofollow', 'UGC', 'Sponsored', 'Rendered']

api_key = os.getenv('API_KEY')
base_id = os.getenv('BASE_ID')
table_name = os.getenv('TABLE_ID')
data = None


def process_files():
    global data
    files = 0
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)

            # Detect the file encoding
            with open(file_path, 'rb') as f:
                result = chardet.detect(f.read())
            print(f"Detected encoding for {filename}: {result['encoding']}")
            df = pd.read_csv(file_path, encoding=result['encoding'], delimiter='\t', usecols=csv_columns)

            # Convert boolean columns to boolean
            # for col in boolean_columns:
            #     df[col] = df[col].astype(bool)

            if set(df.columns) == set(csv_columns):
                # df['Domain Name'] = df['Referring page URL'].apply(lambda x: urlparse(x).netloc.replace('www.', ''))
                #df['Domain Name'] = df['Referring page URL'].apply(remove_html_extension)
                df['Domain Name'] = df['Referring page URL'].str.lower()
                df['Filename'] = filename
                data = pd.concat([data, df], ignore_index=True)
                print(f"Data from {filename}:\n")
                print(df.head())
                print(f"Total rows: {len(df)}")
                # df.apply(process_row, axis=1)
                files += 1
            else:
                print(f"File '{filename}' does not have the required columns. Skipping...")

    print(f"[!!] Total files processed: {files}\n\n")


def check_match_with_airtable(airtable_df):
    airtable_df['Exists in Files'] = None

    file_df = data.copy()
    print(f"\n\n[**] Checking match with Airtable...")
    print(file_df.head())
    temp_df = airtable_df.copy()
    # temp_df['Exists in Airtable'] = temp_df['Website Domain'].isin(file_df['Domain Name'])
    # matches = temp_df['Exists in Airtable'].sum()
    merged_df = pd.merge(temp_df, file_df[['Domain Name', 'Lost status', 'Lost', 'Filename']],
                         left_on='URL', right_on='Domain Name', how='left')
    print(merged_df.head())
    merged_df['Exists in Files'] = merged_df['Domain Name'].notnull()
    matches = merged_df['Exists in Files'].sum()
    print(f"Number of matches found: {matches}")
    filtered_df = merged_df[merged_df['Exists in Files']]
    print(f"Number of matches found: {len(filtered_df)}")
    print(filtered_df.head(100))
    # Update the original DataFrame
    airtable_df = filtered_df.copy()
    airtable_df.to_csv('matched_output.csv', index=True)
    return airtable_df


def get_airtable_records():
    should_store_records = False
    airtable = Airtable(base_id, table_name, api_key)
    records = airtable.get_all()
    print(f"[+] Total records from Airtable: {len(records)}")
    if should_store_records:
        store_records(records)
    return records


def get_records_to_update(airtable_df):
    records_to_update = []
    for _, row in airtable_df.iterrows():
        # Get the ID, Lost Status, and Lost from the DataFrame row
        record_id = row['id']
        status = 'Link Removed By Author' if row['Lost status'] == 'removedfromhtml' else 'Lost'
        filename = row['Filename']
        lost_status = row['Lost status']
        lost = row['Lost']
        record = {'id': record_id, 'fields': {'fldzFUL78jCoDgteB': lost_status, 'fldqSybJpbY4uhlaP': lost, 'fldyYjQjZqux3ZQ2a': status, 'fldzIhjEHUEg8pZ97': filename}}
        if record not in records_to_update:
            records_to_update.append(record)
        else:
            print(f"Duplicate record found: {record}")
        # print(f"Updated Airtable record with ID {record_id}: Lost Status={lost_status}, Lost={lost}")
    return records_to_update


def update_airtable_batch(records, batch_size=10):
    # Initialize the Airtable client
    airtable = Airtable(base_id, table_name, api_key=api_key)

    total_batches = len(records) // batch_size  # Calculate the total number of complete batches

    # Update records in batches
    for i in range(total_batches):
        start_idx = i * batch_size
        end_idx = (i + 1) * batch_size
        batch_records = records[start_idx:end_idx]

        # Perform Airtable batch update
        try:
            print(f"[+] Updating batch {i + 1} / {total_batches} Total records: {len(batch_records)}...")
            airtable.batch_update(batch_records)
        except Exception as e:
            print(f"[!!!] Error while updating batch: {e}\n")

        # Add a small delay between batches to avoid rate limiting
        time.sleep(1)

    print("Batch update completed.")


def convert_records_to_df(records):
    print(records[0])
    # df = pd.DataFrame.from_records((r['fields'] for r in records))
    print("[!] Converting records to DataFrame...")
    df_airtable = pd.DataFrame([dict(id=record['id'], **record['fields']) for record in records])
    df_airtable['URL'] = df_airtable['URL'].astype(str)
    df_airtable['URL'] = df_airtable['URL'].str.lower()
    # Remove rows where 'Website' is an empty string
    df_airtable = df_airtable[df_airtable['URL'] != '']
    # df_airtable['Website Domain'] = df_airtable['Website'].apply(extract_domain)
    print(df_airtable.head())
    print(f"[+] Total records converted to the Dataframe from Airtable: {len(df_airtable)}")
    return df_airtable




def store_records(records):
    filename = os.getenv('OUTPUT_FILE')
    with open('airtable_records.json', 'w') as f:
        json.dump(records, f)


def read_json_file(json_filepath):
    with open(json_filepath, 'r') as f:
        json_data = json.load(f)

    return json_data


def extract_domain(url):
    domain = urlparse(url).netloc
    if domain.startswith('www.'):
        domain = domain[4:]
    return domain if domain else url.lower()


def remove_html_extension(url):
    pattern = r"(.*\/)[^\/]*\.(html|htm)?$"
    match = re.match(pattern, url)
    if match:
        return match.group(1)
    else:
        return url


def main():
    print(f"Running {__file__} version {__version__}")
    print(f"Input folder: {input_folder}")
    print("[!] Processing files... \n");
    process_files()
    print("-" * 50)
    print("\n\n[!] Fetching Airtable records from the API...")
    records = get_airtable_records()
    df_airtable = convert_records_to_df(records)
    print("-" * 50)
    records_to_update = check_match_with_airtable(df_airtable)
    print(f"Total Records from Airtable: {len(records)}")
    matches = records_to_update['Exists in Files'].sum()  # Count the number of True values in the column
    print(f"[!] Number of matches found: {matches}")
    print("-" * 50)
    print(f"\n\n[!] Updating Airtable records...")
    at_records_to_update = get_records_to_update(records_to_update)
    print(f"Total records to update: {len(at_records_to_update)}\n\n")
    update_airtable_batch(at_records_to_update)
    print("-" * 50)
    print("\n\n[!] Done.")




if __name__ == '__main__':
    main()
    exit(0)