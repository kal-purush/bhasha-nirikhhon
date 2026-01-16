# Nested loops

import csv
import json


def get_article_subjects(article):
    """Returns a list of "subject" string values retrieved from the passed in
    < article >'s "keywords" list.

    Parameters:
        article (dict): representation of an article

    Returns:
        list: list of "subject" strings associated with the < article >
    """

    pass # TODO Implement


def read_json(filepath, encoding='utf-8'):
    """Reads a JSON document, decodes the file content, and returns a list or dictionary if
    provided with a valid filepath.

    Parameters:
        filepath (str): path to file
        encoding (str): name of encoding used to decode the file

    Returns:
        dict/list: dict or list representations of the decoded JSON document
    """

    with open(filepath, 'r', encoding=encoding) as file_obj:
        return json.load(file_obj)


def write_csv(filepath, data, headers=None, encoding='utf-8', newline=''):
    """
    Writes data to a target CSV file. Column headers are written as the first
    row of the CSV file if optional headers are specified.

    WARN: If newline='' is not specified, newlines '\n' or '\r\n' embedded inside quoted
    fields may not be interpreted correctly by the csv.reader. On platforms that utilize
    `\r\n` an extra `\r` will be added.

    Parameters:
        filepath (str): path to target file (if file does not exist it will be created)
        data (list | tuple): sequence to be written to the target file
        headers (seq): optional header row list or tuple
        encoding (str): name of encoding used to encode the file
        newline (str): specifies replacement value for newline '\n'
                       or '\r\n' (Windows) character sequences

    Returns:
        None
    """

    with open(filepath, 'w', encoding=encoding, newline=newline) as file_obj:
        writer = csv.writer(file_obj)
        if headers:
            writer.writerow(headers)
            for row in data:
                writer.writerow(row)
        else:
            writer.writerows(data)


def write_json(filepath, data, encoding='utf-8', ensure_ascii=False, indent=2):
    """Serializes object as JSON. Writes content to the provided filepath.

    Parameters:
        filepath (str): the path to the file
        data (dict)/(list): the data to be encoded as JSON and written to the file
        encoding (str): name of encoding used to encode the file
        ensure_ascii (str): if False non-ASCII characters are printed as is; otherwise
                            non-ASCII characters are escaped.
        indent (int): number of "pretty printed" indention spaces applied to encoded JSON

    Returns:
        None
    """

    with open(filepath, 'w', encoding=encoding) as file_obj:
        json.dump(data, file_obj, ensure_ascii=ensure_ascii, indent=indent)


def main():
    """Program entry point.  Orchestrates program's flow of execution.

    Parameters:
        None

    Returns:
        None
    """

    # CHALLENGE 01

    # Get articles
    articles = None # TODO Call function

    # print(f"\nCh 01 Articles (n={len(articles)})")

    subject_counts = {}

    # TODO Implement loop

    # Sort by value (reversed = -x[1]), then by key (x[0])
    subject_counts = None # dict comp

    # Write to file
    # TODO write to file


    # CHALLENGE 02

    authors = []

    # TODO Implement loop

    # print(f"\nauthors (n={len(authors)})")

    # Sort Authors
    # WARN: convert None to empty string to avoid runtime exception (middlename value)
    # str(x or '') returns '' if x is falsy
    # TypeError: '<' not supported between instances of 'NoneType' and 'str'
    authors = None # TODO list comp

    # print(f"\nAuthors = {authors}")

    # TODO write to file


    # CHALLENGE 03

    citations = {}

    # TODO Implment loop

    # Sort Authors
    citations = None # TODO dict comp

    # TODO write to file



    # CHALLENGE 04

    # TODO Refactor Challenge 03 loop

    # print(f"\nCh 03 Benedict Carey authored articles = {len(citations['Carey_Benedict'])}")


if __name__ == '__main__':
    main()