"""Create Salesforce leads of users listed in a supplied text file."""
import argparse
import csv
from datetime import date
import os
import sys
import time

import requests
from requests import ConnectionError
from zerorpc import LostRemote
from zerorpc import RemoteError
from zerorpc import TimeoutExpired

import jw.xml
import jw.xml.jw_log as jw_log


def _create_initial_csv():
    """Create initial file that contains only column headers."""

    csv_file = _generate_filename()
    try:
        handler = open(csv_file, 'a')
    except:
        return 0

    columns = [
        "00N6F00000E28vO",
        "00N6F00000ECoPl",
        "00N90000006GLvx",
        "00N90000006wTGU",
        "00N9000000AMEsb",
        "00N9000000ES3ir",
        "00N9000000ES3kJ",
        "00N9000000ES3kT",
        "00N9000000ES3kY",
        "company",
        "email",
        "lead_source",
        "oid",
        "phone",
        "recordType"
    ]
    writer = csv.writer(handler, quoting=csv.QUOTE_ALL)
    writer.writerow(columns)
    return {'filename': csv_file, 'handler': handler}


def _get_arg_parser():
    """Setup script parameters."""
    parser = argparse.ArgumentParser(description=__doc__)
    help_str = ('Text file that is the source of a comma-delimited list of '
                'email addresses from the user table.')
    parser.add_argument('source', help=help_str)
    help_str = ('Flag to only generate a CSV file of the report instead of '
                'creating Salesforce leads.')
    parser.add_argument('-c', '--csv', action='store_true', help=help_str)
    return parser


def _generate_filename():
    """Generate the filename of the resulting report."""
    logger = jw_log.get_logger(__package__)
    today = date.today()
    filename = 'salesforce-leads-{}.csv'.format(today)

    #check if csv file exists, remove it if it does
    if os.path.exists(filename):
        os.remove(filename)

    logger.info('Generating file %s ', filename)
    return filename


def create_salesforce_lead(customer_data):
    """Create a Salesforce lead."""
    url = 'https://webto.salesforce.com/servlet/servlet.WebToLead?' \
          'encoding=UTF-8'

    payload = {'00N6F00000E28vO': customer_data['name'],
               '00N6F00000ECoPl': '1',
               '00N90000006GLvx': customer_data['id'],
               '00N90000006wTGU': customer_data['unique_name'],
               '00N9000000AMEsb': customer_data['parent_account_id'],
               '00N9000000ES3ir': '',
               '00N9000000ES3kJ': '',
               '00N9000000ES3kT': '',
               '00N9000000ES3kY': '',
               'company': customer_data['company_name'],
               'email': customer_data['email'],
               'lead_source': 'Claim-Pay-Per-Listing Lead',
               'oid': '00D90000000lh7Q',
               'phone': customer_data['mobile'],
               'recordType': '012900000009EYz'}

    try:
        requests.post(url, data=payload)
    except:
        logger.error('Could not connect to Salesforce API.')
        return False

    return True


def create_salesforce_lead_csv(customer_data, handler, csv_file):
    """Create a CSV containing Salesforce leads from provided data."""
    writer = csv.writer(handler, quoting=csv.QUOTE_ALL)
    content = [
        customer_data['name'],
        '1',
        customer_data['id'],
        customer_data['unique_name'],
        customer_data['parent_account_id'],
        '',
        '',
        '',
        '',
        customer_data['company_name'],
        customer_data['email'],
        'Claim-Pay-Per-Listing Lead',
        '00D90000000lh7Q',
        customer_data['mobile'],
        '012900000009EYz'
    ]
    writer.writerow(content)


def load_data_write_csv(options):
    """Open text file for reading, load its contents, write them to CSV."""
    logger = jw_log.get_logger(__package__)

    current = 0
    total = os.path.getsize(options.source)

    today = date.today()
    log_file = 'salesforce_failed_{}.log'.format(today)

    #check if log file exists, remove it if it does
    if os.path.exists(log_file):
        os.remove(log_file)

    if options.csv:
        initial = _create_initial_csv()
        if initial > 0:
            csv_file = initial['filename']
            handler = initial['handler']
        else:
            logger.error('Could not create CSV file.')
            return

    progress_bar(current, total)
    with open(options.source) as emails, open(log_file, 'a') as failed:
        for email in emails:
            current += len(email)
            progress_bar(current, total)
            time.sleep(0.5)
            client = jw.xml.rpc.Client('customer', heartbeat=None, timeout=15)
            try:
                result = client.get_customer_by_email(email.strip('\n'))
                if result['status'] == 'ERROR':
                    raise LookupError
            except (ConnectionError, LookupError, LostRemote, RemoteError,
                    TimeoutExpired):
                failed.write(email)
                continue
            finally:
                client.close()

            content = result['content']

            if options.csv:
                create_salesforce_lead_csv(content, handler, csv_file)
            else:
                salesforce_connect = create_salesforce_lead(content)
                if salesforce_connect is False:
                    break

        if failed.tell() == 0:
            os.remove(log_file)
            return

        sys.stdout.write('\n')
        logger.warn('At least one email address was not processed. '
                    'The list was saved in %s. You may want to re-run '
                    'this script and use the log file as your new source.',
                    log_file)


def progress_bar(current, total):
    """A custom progress bar."""
    percent = float(current) / total
    hashes = '#' * int(round(percent * 50))
    spaces = ' ' * (50 - len(hashes))
    sys.stdout.write("\rProgress: [{0}] {1}%".format(hashes + spaces,
                     int(round(percent * 100))))
    sys.stdout.flush()


def main():
    """Main function that initially executes the necessary functions."""
    arg_parser = _get_arg_parser()
    options = arg_parser.parse_args()

    load_data_write_csv(options)


if __name__ == '__main__':
    main()