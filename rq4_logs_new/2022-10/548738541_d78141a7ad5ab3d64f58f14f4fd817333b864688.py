def change_body(my_body, row):
    my_body["terminal"]["id"] = row["id_meio_captura"]

def print_line_to_process(row):
    print(f'Line to process: {row}')

def print_result(line_count, response_status, response):
    print(f'Response {line_count}: {response}')
    print(f'Status code: {response_status}')

def print_send_request(line_count):
    if line_count == 0:
        print(f'Sending request {line_count + 1}:')
    else:
        print(f'Sending request {line_count}:')

def process_result(line_count):
    if line_count == 1:
        print(f'Processed {line_count} line of data.')
    else:
        print(f'Processed {line_count} lines of data.')

def process_responses(post_response):
    response_status = post_response.status_code
    responsej = post_response.json()
    return response_status,responsej