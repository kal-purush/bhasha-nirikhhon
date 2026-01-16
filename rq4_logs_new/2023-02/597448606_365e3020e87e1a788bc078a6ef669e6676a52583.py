#!/usr/bin/env python3

"""
Allows to interface with a tabular database in an SQL-like manner.

This module is intended as a lightweight alternative to SQL interfaces such
as SQLAlchemy. It is not intended to be a full-featured database interface,
but rather a simple way to access tabular data in a SQL-like manner,
performing basic operations such as filtering, sorting, and aggregating.
It is also supposed to rely as much as possible only on standard Python
libraries, so that it can be used in a wide range of contexts.

The entire project consists, by design, of a single file, which is
intended to be copied and pasted into the project that needs to use it. There
are no dependencies, even though some libraries are used if available. The
project can work on both single tabular files and directories containing
multiple tabular files, which are then treated as separate tables. No
additional annotation or metadata is required, but these are used if they
are present.
"""

# Define metadata for the project
__author__ = "Tiago Tresoldi"
__email__ = "tiago.tresoldi@lingfil.uu.se"
__version__ = "0.1.0"
__citation__ = f"Tresoldi, Tiago (2022). {__name__}.py. Version {__version__}."

# Import Python standard libraries
from pathlib import Path
from typing import *
import csv
import logging
import sqlite3
import unicodedata

# Define an implementation for the `unicode2ascii` function: first try the `unidecode`
# library, then a local copy of the `unicode2ascii` simplepy function, and finally
# a simple function that only removes non-alphanumeric characters.

# Import the `unidecode` library if available; if not, define a much
# simpler function to replace it
try:
    from unidecode import unidecode as unicode2ascii
except:
    logging.debug(
        "The `unidecode` library is not available, attempting to load `unicode2ascii`."
    )
    try:
        from .unicode2ascii import unicode2ascii
    except:
        logging.debug(
            "The `unicode2ascii` function is not available, using a simple fallback."
        )

        def unicode2ascii(string: str) -> str:
            """
            Build a string with only ASCII characters.

            This function is a much simpler version of the `unidecode` library,
            which is used to remove accents from characters. It is used only
            when neither the `unidecode` library or the `unicode2ascii` module
            are available. It is particularly *not* recommended to use this
            function for non-Latin alphabets.
            """

            # Decompose the string into its base and combining characters,
            # so that there are higher changes of obtaining ASCII ones
            string = unicodedata.normalize("NFKD", string)

            return "".join(
                [c for c in string if c.isalpha() or c.isdigit() or c.isspace()]
            )


class Database:
    def __init__(self, source_path: Optional[Union[Path, str]] = None):
        """
        Initialize a database object.

        A database can be initialized as an empty object, or by reading
        either a single tabular file or a directory containing multiple
        tabular files.

        Parameters
        ----------
        source_path : Path or str, optional
            The path to the file or directory containing the data to be
            added to the database. If not provided, the database is
            initialized as an empty object.
        """

        # Initialize the internal variables
        self.tables = {}  # map table names to filenames

        # Initialize the in-memory sqlite3 database
        self.connection = sqlite3.connect(":memory:")

        # Make sure we have a `Path` object
        if isinstance(source_path, str):
            source_path = Path(source_path)

        # If the input is a directory, read all files in it, each one
        # being a table; otherwise, read the file as a single table.
        # The table name comes from the filename, without the extension.
        if source_path.is_file():
            self._add_table(source_path)

    def _add_table(self, filepath: Path):
        """
        Add a table to the database.

        Parameters
        ----------
        filepath : Path
            The path to the file containing the data to be added.
        """

        # Obtain data
        tabledata, datatypes = self._read_tabular(filepath)

        # Obtain an sql table name
        # TODO: check for duplicates; add slug
        tablename = unicode2ascii(filepath.stem)

        # Create the table
        command = "CREATE TABLE {table} ({columns})".format(
            table=tablename,
            columns=", ".join(
                [
                    "{key} {datatype}".format(key=key, datatype=datatype)
                    for key, datatype in datatypes.items()
                ]
            ),
        )
        logging.debug(command)
        self.connection.execute(command)
        self.tables[tablename] = filepath

        # Add the data
        command = "INSERT INTO %s VALUES (%s)" % (
            tablename,
            ", ".join([f":{key}" for key in datatypes.keys()]),
        )
        logging.debug(command)
        self.connection.executemany(
            command,
            tabledata,
        )

    # TODO: move to inside the class?
    # TODO: accept datatype specifications
    # TODO: work with sigils, perhaps not by default?
    # TODO: consider "standard" column names such latitute, longitude, etc.?
    # TODO: support for booleans, dates, and times?
    # TODO: sigils, type characters: $ for string, % for integer, # for float, ! for boolean, @ for date, & for time
    def _read_tabular(self, filename: Path) -> Tuple[List[dict], Dict[str, str]]:
        """
        Read the contents of a tabular file.

        Contents of the file are returned as a list of dictionaries.
        The encoding of the file is assumed to be UTF-8, and the
        dialect is automatically detected via the standard `csv`
        library.

        Parameters
        ----------
        filename : Path
            The path to the file to be read.

        Returns
        -------
        data : List[dict]
            A list of dictionaries, where each dictionary represents
            a row in the file, and the keys are the column names.
        datatypes : Dict[str, str]
            A dictionary mapping the column names to their SQL datatypes.
        """

        # Open the file and use the sniffer to detect the dialect
        with open(filename, encoding="utf-8") as f:
            dialect = csv.Sniffer().sniff("\n".join(f.readlines(5)))
            f.seek(0)

            # Read the contents as a list of dictionaries
            data = [row for row in csv.DictReader(f, dialect=dialect)]

        # Obtain a list of all keys and iterate over them to check which
        # ones can be converted to `int` or `float`; the check is conducted
        # by simply testing the conversion for the values expressed in all
        # rows
        # TODO: consider and treat empty values and NAs
        # TODO: add support for used-defined, sigils, etc.
        keys = data[0].keys()
        values = {key: [] for key in keys}
        key_map = {}
        for row in data:
            for key, value in row.items():
                values[key].append(value)

        for key in keys:
            # Check if all values can be converted to `int`
            try:
                values = [int(value) for value in values[key]]
                key_map[key] = "INTEGER"
            except:
                pass

            # Check if all values can be converted to `float`
            try:
                values = [float(value) for value in values[key]]
                key_map[key] = "REAL"
            except:
                pass

        # Iterate over all rows and convert the values of the columns
        # that were detected as numeric (the key is either in `int_keys`
        # or `float_keys`)
        for row in data:
            for key, datatype in key_map.items():
                if datatype == "INTEGER":
                    row[key] = int(row[key])
                elif datatype == "REAL":
                    row[key] = float(row[key])

        # Build a dictionary of keys and datatypes, defaulting to TEXT
        datatypes = {key: key_map.get(key, "TEXT") for key in keys}

        return data, datatypes


# TODO: get as a string, or allow to redirect to a file/log/etc
def print_table(data: Union[List[dict], List[List[str]]]):
    """
    Print a table of data.

    This function prints a table of data, composed of a list of
    dictionaries, or a list of lists. The table is printed to the standard
    output. It is similar to behavior of libraries such as `pandas`,
    `tabulate`, and `prettytable`, but it is implemented from scratch
    and does not require any external dependencies.

    Parameters
    ----------
    data : Union[List[dict], List[List[str]]]
        The data to be printed. If the data is a list of dictionaries,
        the keys of the dictionaries are used as column names.
    """

    if all(isinstance(row, dict) for row in data):
        # Find the longest string in each column across all rows
        header = data[0].keys()
        column_widths = [
            max(len(str(row.get(col, ""))) for row in data) for col in header
        ]

        # Print header
        header_row = " | ".join(
            str(col).ljust(width) for col, width in zip(header, column_widths)
        )
        separator_row = "-+-".join("-" * width for width in column_widths)
        print(header_row)
        print(separator_row)

        # Print rows
        for row in data:
            values = row.values()
            row = " | ".join(
                str(val).ljust(width) for val, width in zip(values, column_widths)
            )
            print(row)
    else:
        # Find the longest string in each column across all rows
        column_widths = [
            max(len(str(row[col])) for row in data) for col in range(len(data[0]))
        ]

        # Print rows
        for row in data:
            row = " | ".join(
                str(val).ljust(width) for val, width in zip(row, column_widths)
            )
            print(row)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python3 database.py <input>")
    else:
        db = Database(sys.argv[1])
        while True:
            command = input("Enter a query: ")
            if command == "quit":
                break
            else:
                print_table(db.connection.execute(command).fetchall())