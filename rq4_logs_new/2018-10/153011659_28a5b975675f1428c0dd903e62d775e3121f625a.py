import argparse
import os
import traceback
import urllib

from bs4 import BeautifulSoup
import requests
import textract


# Dumb script to grab PDFs from the posts on https://whitepaperdatabase.com
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        '--download',
                        help='Download whitepapers',
                        action='store_true',
                        default=False)
    parser.add_argument('-e',
                        '--extract',
                        help='Extract text from whitepapers',
                        action='store_true',
                        default=False)
    args = parser.parse_args()
    if args.download:
        download_pdfs()
    if args.extract:
        extract_text()


def download_pdfs():
    pdf_dir = os.path.join(os.path.dirname(__file__), 'raw_whitepapers')
    already_downloaded = os.listdir(pdf_dir)
    with open(os.path.join(os.path.dirname(__file__), 'whitepapers.txt'), 'r') as whitepaper_urls:
        for line in whitepaper_urls:
            print('Processing {}'.format(line))
            whitepaper_page_response = requests.get(line)
            if whitepaper_page_response.status_code == 200:
                print('Parsing HTML for {}'.format(line))
                soup = BeautifulSoup(whitepaper_page_response.text)
                potential_source_links = soup.find_all('a')
                for link in potential_source_links:
                    pdf_link = link.get('href')
                    if pdf_link and pdf_link.endswith('pdf'):
                        print('Downloading {}'.format(pdf_link))
                        # Download the PDF
                        local_filename = pdf_link.split('/')[-1]
                        if local_filename in already_downloaded:
                            print('Skipping {}'.format(local_filename))
                            continue
                        try:
                            urllib.request.urlretrieve(pdf_link, '{}/{}'.format(pdf_dir, local_filename))
                        except (urllib.error.HTTPError, UnicodeEncodeError) as exc:
                            print('Couldn\'t get file {}'.format(pdf_link))
                            traceback.print_exc()
                        print('Saved {}'.format(local_filename))

def extract_text():
    raw_pdf_dir = os.path.join(os.path.dirname(__file__), 'raw_whitepapers')
    processed_pdf_dir = os.path.join(os.path.dirname(__file__), 'processed_whitepapers')
    pdf_filenames = os.listdir(raw_pdf_dir)
    for item in pdf_filenames:
        print('OUTPUT {}'.format(item))
        try:
            text = textract.process('{}/{}'.format(raw_pdf_dir, item))
        except textract.exceptions.ShellError as exc:
            traceback.print_exc()
            print('Skipping {}'.format(item))
            continue
        with open('{}/{}'.format(processed_pdf_dir, item), 'w') as output_file:
            output_file.write(str(text, 'utf-8'))


if __name__ == '__main__':
    main()