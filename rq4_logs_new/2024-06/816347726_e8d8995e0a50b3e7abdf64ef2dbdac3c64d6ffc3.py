import json
import csv
import os
import re
import shutil
import re
import pandas as pd
from collections import Counter, defaultdict

#### Support Functions ####

def flatten_json(json_obj):
    result = {}

    def find_closest_key(obj, parent_key=''):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, (dict, list)):
                    find_closest_key(value, key)
                else:
                    result[key] = value
        elif isinstance(obj, list):
            for item in obj:
                find_closest_key(item, parent_key)

    find_closest_key(json_obj)
    return result

def is_postman_variable(body):
    return bool(re.match(r'^\s*\{\{\w+\}\}\s*$', body))

def traverse_items(items, parent_path=""):
    request_details = []
    for item in items:
        if 'item' in item:
            request_details.extend(traverse_items(item['item'], parent_path + '[' + item['name'] + ']'))
        else:
            keypath = parent_path + '[' + item['name'] + ']'
            method = item['request']['method']

            url = ""
            if 'url' in item['request']:
                if isinstance(item['request']['url'], dict) and 'raw' in item['request']['url']:
                    url = item['request']['url']['raw']
                elif isinstance(item['request']['url'], str):
                    url = item['request']['url']

            parameters = "x" if '?' in url and '=' in url.split('?', 1)[1] else ""
            url_present = "x" if url else ""
            headers = "x" if item['request'].get('header') else ""
            body = "x" if 'body' in item['request'] and 'raw' in item['request']['body'] and item['request']['body']['raw'].strip() else ""
            request_details.append({
                "keypath": keypath,
                "method": method,
                "url": url,
                "parameters": parameters,
                "url_present": url_present,
                "headers": headers,
                "body": body,
                "request": item['request']
            })
    return request_details

def load_csv(csv_file):
    data = {}
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data[row['request']] = row
    return data

def is_variable(value):
    return bool(re.match(r'^\s*\{\{.*\}\}\s*$', value))

def process_csv(file_path):
    with open(file_path, newline='') as csvfile:
        return list(csv.DictReader(csvfile))

def count_values(data):
    total, hard_coded, variables = 0, 0, 0
    key_counts = defaultdict(lambda: {'total': 0, 'variables': 0, 'unique': set(), 'unique_variables': set()})

    for row in data:
        for key, value in row.items():
            if key == 'request' or not value:
                continue
            total += 1
            key_counts[key]['total'] += 1
            key_counts[key]['unique'].add(value)
            if is_variable(value):
                variables += 1
                key_counts[key]['variables'] += 1
                key_counts[key]['unique_variables'].add(value)
            else:
                hard_coded += 1
    
    unique_values = sum(len(v['unique']) for v in key_counts.values())
    unique_variables = sum(len(v['unique_variables']) for v in key_counts.values())
    return total, hard_coded, variables, unique_values, unique_variables, key_counts

def parse_url(url):
    # Initialize variables
    scheme, url_host, subdirs, param_dict = '', '', [], {}

    # Split URL to components
    if '://' in url:
        scheme, rest = url.split('://', 1)
        scheme += '://'
    else:
        rest = url

    # Determine the end of the host part before the first '/' or '?'
    if '/' in rest:
        end_of_host = rest.find('/')
    elif '?' in rest:
        end_of_host = rest.find('?')
    else:
        end_of_host = len(rest)

    url_host = scheme + rest[:end_of_host]

    # Extract subdirectories and handle '?' in subdirectories
    subdirectory_part = rest[end_of_host:]
    if '/' in subdirectory_part:
        subdirs = subdirectory_part.split('/')
        # Remove parameter part if it exists
        if '?' in subdirs[-1]:
            subdirs[-1], _, _ = subdirs[-1].partition('?')
    else:
        subdirs = []

    # Extract parameters
    param_start = subdirectory_part.find('?')
    if param_start != -1:
        param_string = subdirectory_part[param_start+1:].split('#', 1)[0]  # Ignore fragments
        param_list = [p.split('=') for p in param_string.split('&') if '=' in p]
        param_dict = {param[0]: param[1] for param in param_list}

    return scheme, url_host, subdirs, param_dict

def create_table(data, headers, total_unique, total_unique_variables):
    col_widths = [max(len(str(item)) for item in col) for col in zip(*data, headers)]
    row_format = "| " + " | ".join([f"{{:<{width}}}" for width in col_widths]) + " |"
    separator = "-" * (sum(col_widths) + 3 * len(headers) + 1)

    table = separator + "\n" + row_format.format(*headers) + "\n" + separator
    for row in data:
        row[2] = f"{row[2]}/{row[1]}"  # Modify variables column to the "variable/unique" format
        table += "\n" + row_format.format(*row)
    table += "\n" + separator
    table += "\n" + row_format.format("TOTAL", total_unique, f"{total_unique_variables}/{total_unique}")
    table += "\n" + separator
    return table

#### Primary Functions ####

def parse_postman_collection(json_file, output_dir):
    body_counter = Counter()
    all_bodies = []
    header_counter = Counter()
    all_headers = []
    max_subdirs = [0]
    all_hosts = []
    all_paths = []
    all_params = []
    param_counter = Counter()
    all_requests = []

    with open(json_file, 'r') as f:
        data = json.load(f)

    request_details = traverse_items(data['item'])

    for detail in request_details:
        path = detail["keypath"]
        request = detail["request"]
        all_requests.append([detail["keypath"], detail["method"], detail["url_present"], detail["parameters"], detail["headers"], detail["body"]])

        # Parse body
        body = request.get('body', {}).get('raw', '')
        if body:
            try:
                if is_postman_variable(body):
                    flat_body = {'body': body}
                else:
                    body_json = json.loads(body)
                    flat_body = flatten_json(body_json)
                all_bodies.append((path, flat_body))
                body_counter.update(flat_body.keys())
            except json.JSONDecodeError:
                flat_body = {'body': body}
                all_bodies.append((path, flat_body))
                body_counter.update(flat_body.keys())

        # Parse headers
        headers = request.get('header', [])
        if headers:
            flat_headers = {header['key']: header['value'] for header in headers}
            all_headers.append((path, flat_headers))
            header_counter.update(flat_headers.keys())

        # Parse URLs and parameters
        url = request['url']
        if isinstance(url, dict):
            url = url['raw']
        scheme, url_host, subdirs, params = parse_url(url)
        max_subdirs[0] = max(max_subdirs[0], len(subdirs))  # Update the max subdirs count

        # Collect all host and path data
        all_hosts.append((path, url_host))
        all_paths.append((path, subdirs))
        all_params.append((path, params))
        param_counter.update(params.keys())
        
    # Save bodies to CSV
    sorted_body_keys = [key for key, _ in body_counter.most_common()]
    body_fieldnames = ['request'] + sorted_body_keys
    body_csv_file = os.path.join(output_dir, "bodies.csv")
    with open(body_csv_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=body_fieldnames)
        writer.writeheader()
        for path, flat_body in all_bodies:
            row = {'request': path}
            row.update(flat_body)
            valid = [x for x in row if x]
            if len(valid) < 2:
                continue
            writer.writerow(row)

    # Save headers to CSV
    sorted_headers = [header for header, _ in header_counter.most_common()]
    header_fieldnames = ['request'] + sorted_headers
    header_csv_file = os.path.join(output_dir, "headers.csv")
    with open(header_csv_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header_fieldnames)
        writer.writeheader()
        for path, flat_headers in all_headers:
            row = {'request': path}
            row.update(flat_headers)
            valid = [x for x in row if x]
            if len(valid) < 2:
                continue
            writer.writerow(row)

    # Save hosts to CSV
    host_csv_file = os.path.join(output_dir, "hosts.csv")
    with open(host_csv_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['request', 'host'])
        for path, url_host in all_hosts:
            writer.writerow([path, url_host])

    # Save paths to CSV
    path_fieldnames = ['request'] + [f'subdir{i}' for i in range(max_subdirs[0])]
    path_csv_file = os.path.join(output_dir, "paths.csv")
    with open(path_csv_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=path_fieldnames)
        writer.writeheader()
        for path, url_subdirs in all_paths:
            row = {'request': path}
            for i, subdir in enumerate(url_subdirs):
                row[f'subdir{i}'] = subdir
            writer.writerow(row)

    # Save parameters to CSV
    param_fieldnames = ['request'] + [param for param, _ in param_counter.most_common()]
    param_csv_file = os.path.join(output_dir, "queries.csv")
    with open(param_csv_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=param_fieldnames)
        writer.writeheader()
        for path, param_dict in all_params:
            row = {'request': path}
            row.update(param_dict)
            valid = [x for x in row if x]
            if len(valid) < 2:
                continue
            writer.writerow(row)

    # Save requests to CSV
    requests_csv_file = os.path.join(output_dir, "requests.csv")
    with open(requests_csv_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['request', 'method', 'url', 'parameters', 'headers', 'body'])
        csv_writer.writerows(all_requests)

def extract_variables_from_directory(directory):
    variable_counts = defaultdict(int)
    pattern = re.compile(r'{{(.*?)}}')

    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            with open(filepath, newline='') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    for cell in row:
                        matches = pattern.findall(cell)
                        for match in matches:
                            variable_counts[match.strip()] += 1

    output_filepath = os.path.join(directory, 'variables.csv')
    with open(output_filepath, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Variable', 'Count', 'Given Value'])
        for variable, count in variable_counts.items():
            variable = "{{" + variable + "}}"
            writer.writerow([variable, count, ""])

def extract_values_from_directory(directory):
    value_counts = defaultdict(int)
    value_sets = defaultdict(set)

    for filename in os.listdir(directory):
        if filename in ['bodies.csv', 'headers.csv', 'hosts.csv', 'paths.csv', 'queries.csv']:
            filepath = os.path.join(directory, filename)
            with open(filepath, newline='') as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader, None)
                if not headers:
                    continue

                for row in reader:
                    for col_idx, cell in enumerate(row[1:], start=1):
                        if col_idx < len(headers) and cell.strip():
                            header = headers[col_idx]
                            value_counts[(filename, header)] += 1
                            value_sets[(filename, header)].add(cell.strip())

    max_unique_values = max(len(values) for values in value_sets.values())

    output_filepath = os.path.join(directory, 'values.csv')
    with open(output_filepath, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_ALL, escapechar='\\')

        unique_headers = [f'unique{i+1}' for i in range(max_unique_values)]
        header = ['filename', 'key', 'total_count', 'unique_count'] + unique_headers
        writer.writerow(header)

        for (filename, key), count in value_counts.items():
            unique_values = sorted(value_sets[(filename, key)])
            unique_count = len(unique_values)
            unique_values += [''] * (max_unique_values - len(unique_values))
            row = [filename.replace('.csv', ''), key, count, unique_count] + unique_values
            writer.writerow(row)

def restructure(output_dir):
    # Define the new structure
    matrix_folder = os.path.join(output_dir, "Matrix")
    meta_folder = os.path.join(output_dir, "Meta")
    
    # Create folders if they don't already exist
    os.makedirs(matrix_folder, exist_ok=True)
    os.makedirs(meta_folder, exist_ok=True)
    

    # Mapping of files to their new locations
    files_to_move = {
        "Matrix": ["bodies.csv", "headers.csv", "hosts.csv", "paths.csv", "queries.csv", "variables.csv"],
        "Meta": ["requests.csv", "values.csv"]
    }

    # Move files to the Matrix folder, processing paths.csv to remove subdir0 column
    for file_name in files_to_move["Matrix"]:
        src_path = os.path.join(output_dir, file_name)
        if os.path.exists(src_path):
            if file_name == "paths.csv":
                # Process paths.csv to remove subdir0 column
                df = pd.read_csv(src_path)
                if 'subdir0' in df.columns:
                    df.drop(columns=['subdir0'], inplace=True)
                df.to_csv(src_path, index=False)
            shutil.move(src_path, matrix_folder)

    # Move files to the Meta folder
    for file_name in files_to_move["Meta"]:
        src_path = os.path.join(output_dir, file_name)
        if os.path.exists(src_path):
            shutil.move(src_path, meta_folder)

def summarize_collection(directory):
    files = {
        'requests': os.path.join(directory, 'Meta/requests.csv'),
        'hosts': os.path.join(directory, 'Matrix/hosts.csv'),
        'paths': os.path.join(directory, 'Matrix/paths.csv'),
        'queries': os.path.join(directory, 'Matrix/queries.csv'),
        'headers': os.path.join(directory, 'Matrix/headers.csv'),
        'bodies': os.path.join(directory, 'Matrix/bodies.csv')
    }

    data = {key: process_csv(file) for key, file in files.items() if os.path.exists(file)}
    num_requests = len(data['requests']) if 'requests' in data else 0

    counts = {key: count_values(data[key]) for key in data if key != 'requests'}

    summary = ""

    total_values = 0
    total_unique_values = 0
    total_unique_variables = 0

    # Define the desired order of keys
    ordered_keys = ['hosts', 'paths', 'queries', 'headers', 'bodies']

    for key in ordered_keys:
        if key in counts:
            total, hard_coded, variables, unique_values, unique_variables, key_counts = counts[key]
            # Sort table_data by the first column alphabetically
            table_data = sorted([[k, len(v['unique']), len(v['unique_variables'])] for k, v in key_counts.items()], key=lambda x: x[0])
            table = create_table(table_data, [key.capitalize(), "Unique Values", "Variables"], unique_values, unique_variables)
            summary += table + "\n\n"
            total_values += total
            total_unique_values += unique_values
            total_unique_variables += unique_variables

    summary = f"""\nCollection {directory}:
    {num_requests} requests
    {total_values} fields (configuration/authorization/authentication/identification)
    {total_unique_values} unique values
    {total_unique_variables}/{total_unique_values} unique values as variables\n\n""" + summary

    with open(f'{directory}/summary.txt', 'w') as f:
        f.write(summary)

    return summary

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: python parse.py <postman_collection.json>")
        sys.exit(1)

    postman_collection_file = sys.argv[1]
    base_name = os.path.basename(postman_collection_file).split('.')[0]
    output_dir = os.path.join(os.path.dirname(postman_collection_file), base_name)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    parse_postman_collection(postman_collection_file, output_dir)
    extract_variables_from_directory(output_dir)
    extract_values_from_directory(output_dir)
    restructure(output_dir)
    summarize_collection(output_dir)