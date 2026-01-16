"""Retrieve Prisma Access addresses of tenant and output in dynamic list format"""
import json
import sys
from datetime import datetime
import argparse
import logging
import requests

API_ENDPOINT = 'https://api.prod.datapath.prismaaccess.com/getPrismaAccessIP/v2'
# Some tenants use "prod6" instead of "prod"
API_ENDPOINT_2 = 'https://api.prod6.datapath.prismaaccess.com/getPrismaAccessIP/v2'
CONN_TIMEOUT = 10

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Print Prisma Access endpoints of tenant in IP list format')
    parser.add_argument('output', type=str, help='Output file name')
    parser.add_argument('-s', '--service-type', action='store', type=str, help='Service type can be one of (all | '
                                                                               'remote_network | gp_gateway | '
                                                                               'gp_portal | clean_pipe | swg_proxy). '
                                                                               'Default: all',
                        default='all')
    parser.add_argument('-a', '--address-type', action='store', type=str, help='Address type can be one of (all | '
                                                                               'active | service_ip | '
                                                                               'auth_cache_service | '
                                                                               'network_load_balancer). Default: all',
                        default='all')
    parser.add_argument('-l', '--location', action='store', type=str, help='Location can be one of (all | deployed) '
                                                                           'Default: all',
                        default='all')
    parser.add_argument('-k', '--key', action='store', type=str, help='The API key if not reading it from file')
    parser.add_argument('-c', '--key-file', action='store', type=str, help='Optional. Location of the file with the '
                                                                           'API key')
    parser.add_argument('-n', '--no-comments', action='store_true', help='Omit address details in output file')
    parser.add_argument('-2', '--endpoint2', action='store_true', help='Use the alternative endpoint prod6')
    parser.add_argument('-x', '--debug-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='WARNING', help='Logging message verbosity. Default: WARNING')
    args = parser.parse_args()
    logging.basicConfig(level=args.debug_level, format='%(asctime)s [%(levelname)s] %(message)s')
    logging.info('Starting with args %s', args)
    iteration = 0
    if not args.key:
        if not args.key_file:
            logging.critical('API key not passed as argument or key file. Exiting...')
            sys.exit(1)
        logging.info('Attempting to read key from %s', args.key_file)
        with open(args.key_file, 'r', encoding='utf-8') as f:
            API_KEY = f.read().strip()
    else:
        logging.info('Key passed as argument')
        API_KEY = args.key
    logging.info('Looking for old file %s', args.output)
    old_address_list = []
    try:
        with open(args.output, 'r', encoding='utf-8') as f:
            old_file = f.readlines()
        if old_file[0].strip() == '# prisma_ip_to_edl.py':
            logging.info('Found old file')
            if old_file[1].startswith('#'):
                iteration = int(old_file[1].replace('# Iteration ', '').strip())
            logging.info('Old iteration number is %d', iteration)
            iteration = iteration + 1
            for line in old_file:
                if not line.startswith('#'):
                    old_address_list.append(line.strip())
    except FileNotFoundError:
        logging.warning('Previous file not found. Creating a new one.')
    if args.endpoint2:
        API_ENDPOINT = API_ENDPOINT_2
    fw_req = requests.Session()
    fw_req.verify = True
    fw_req.timeout = CONN_TIMEOUT
    fw_req.headers.update({'header-api-key': API_KEY})
    body_as_dict = {'serviceType': args.service_type, 'addrType': args.address_type, 'location': args.location}
    logging.debug('Contents: %s', str(body_as_dict))
    retrieval_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    try:
        response = fw_req.post(API_ENDPOINT, json=body_as_dict)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logging.critical('HTTP 401 received. Check API key and API endpoints are correct. '
                             'As of May 2023, some Cloud Managed deployments use '
                             'https://api.prod6.datapath.prismaaccess.com/getPrismaAccessIP/v2 '
                             '(note the prod6 in the URL instead of prod). Run the script with the --endpoint2 option '
                             'to use this instead.')
            sys.exit(1)
        e.response.raise_for_status()
    logging.debug('Received response: %s', str(response.text))
    body_dict = json.loads(response.text)  # json to dict
    logging.info('Opening output file')
    output_list = []
    address_list = []
    output_list.append(f'# prisma_ip_to_edl.py')
    output_list.append(f'# Iteration {str(iteration)}')
    output_list.append(f'# Retrieved at: {retrieval_time}')
    output_list.append(f'# Last change detected was between previous iteration and {retrieval_time}')
    output_list.append(f'# {str(body_as_dict)}')
    output_list.append(f'#')
    ip_timestamps = []
    for result in body_dict['result']:
        zone_addresses = []
        output_list.append(f"#\n#################### {result['zone']} ####################\n#")
        for item in result['address_details']:
            try:
                output_list.append(f"#    type: {item['serviceType']}")
            except KeyError:
                output_list.append(f"#    type: N/A")
            try:
                output_list.append(f"#    active: {item['addressType']}")
            except KeyError:
                output_list.append(f"#    active: N/A")
            try:
                output_list.append(f"#    allowlisted: {item['allow_listed']}")
            except KeyError:
                output_list.append(f"#    allowlisted: N/A")
            try:
                creation_timestamp = datetime.utcfromtimestamp(item['create_time']).strftime("%Y-%m-%d %H:%M:%S")
                ip_timestamps.append(int(item['create_time']))
            except KeyError:
                creation_timestamp = 'N/A'
            output_list.append(f"#    creation_timestamp: {creation_timestamp}")
            output_list.append('#')
            address_list.append(item['address'])
            zone_addresses.append(item['address'])
            output_list.append(item['address'])
            output_list.append('#')
        # Make sure there's no extra data in result['addresses']
        if result['addresses_v6'] or set(result['addresses']) != set(zone_addresses):
            logging.info('Found extra addresses or v6')
            output_list.append('# No info available:')
        for address in result['addresses']:
            if address not in address_list:
                logging.info("Found extra address in result['addresses']: %s", address)
                address_list.append(address)
                output_list.append(address)
        for address6 in result['addresses_v6']:
            address_list.append(address6)
            output_list.append(address6)
    if set(old_address_list) == set(address_list) and ip_timestamps:
        # catch changed addresses that don't have creation_timestamp in the json
        output_list[3] = f'# Last change detected via creation_timestamp at: ' \
                         f'{datetime.utcfromtimestamp(max(ip_timestamps)).strftime("%Y-%m-%d %H:%M:%S.%f")}'
    if old_address_list and not address_list:
        logging.critical('EDL has emptied')
    if not address_list:
        logging.warning('EDL contains no entries')
    with open(args.output, 'w', encoding='utf-8') as f:
        for line in output_list:
            if line.startswith('#') and line != '# prisma_ip_to_edl.py' and args.no_comments:
                logging.debug('skipping line due to -n flag: %s', line)
                continue
            logging.debug('%s', line)
            f.write(f'{line}\n')