import sqlite3
import os


class ParsedField(object):
    def __init__(self, column_name, value):
        self.column = column_name
        self.value = value

    def __repr__(self):
        return "{}: {}".format(self.column, self.value)


class FieldParser(object):
    def __init__(self, column_name, **kwargs):
        self.column = column_name
        self.type = kwargs.get('type', 'text')
        self.indexed = kwargs.get('indexed', False)
        self.uppercase = kwargs.get('uppercase', True)

    def parse(self, value):
        if self.type == 'text':
            if not isinstance(value, unicode):
                if any(ord(c) < 128 for c in value):
                    # TODO: ugly, but there are some latin-1 characters
                    # in there so this tries to detect that and convert
                    # to unicode
                    value = value.decode('latin-1')

                value = unicode(value)

        return ParsedField(self.column, value)

    def __repr__(self):
        return "{}".format(self.column)


fields = (
    FieldParser("last_name", indexed=True),
    FieldParser("first_name", indexed=True),
    FieldParser("middle_name"),
    None, # Name suffix
    None, # Residence House Number
    None, # Residence Fractional Address
    None, # Residence Apartment
    None, # Residence Pre Street Direction
    None, # Residence Street Name
    None, # Residence Post Street Direction

    # 11
    None, # Residence City
    FieldParser("zip_code", indexed=True), # Residence Zip Code 5
    None, # Residence Zip Code + 4
    None, # Mailing Address 1
    None, # Mailing Address 2
    None, # Mailing Address 3
    None, # Mailing Address 4
    FieldParser("DOB"), # DOB YYYYMMDD
    None, # Gender (M/F)
    FieldParser("political_party"), # Political Party (DEM, REP, CON, GRE, WOR, IND, WEP, SCC, OTH, BLK)

    # 21
    FieldParser("other_party"), # Other party (LBT, SAP)
    None, # Country Code
    None, # Election District
    None, # Legislative District
    None, # Town / City
    None, # Ward
    None, # Congressional District
    None, # Senate District
    None, # Assembly District
    None, # Last date voted (YYYYMMDD)

    # 31
    None, # Last year voted
    None, # Last county voted in
    None, # Last registered address
    None, # Last registered name (if different)
    None, # County voter registration number
    FieldParser("application_date"), # Application date (YYYYMMDD)
    FieldParser("application_source"), # Application Source (AGCY, CBOE, DMV, LOCALREG, MAIL, SCHOOL)
    None, # ID required (Y/N)
    None, # ID verification requirement met (Y/N)
    FieldParser("voter_status"), # Voter Status Codes (A, AM, AF, AP, AU, I, P, 17)

    # 41
    None, # Status Reason Codes
    None, # Date Voter Made Inactive (YYYYMMDD)
    None, # Date voter was purged (YYYYMMDD)
    None, # Unique NYS Voter ID
    None, # Voter History (semicolon separated)
)


TABLE_NAME = "voters"


def create_sqlite_database(file_name):
    if not file_name.endswith('.db'):
        raise Exception("Invalid SQLite name: {}".format(file_name))

    if os.path.exists(file_name):
        os.remove(file_name)

    conn = sqlite3.connect(file_name)

    c = conn.cursor()
    c.execute("CREATE TABLE {} (id INTEGER PRIMARY KEY, {})".format(TABLE_NAME, ', '.join("{} {}".format(fp.column, fp.type) for fp in fields if fp is not None)))

    conn.commit()
    return conn


def insert_into_table(parsed_fields, db_conn):
    c = db_conn.cursor()
    sql = "INSERT INTO {} ({}) VALUES ({})".format(TABLE_NAME,
           ', '.join(pf.column for pf in parsed_fields),
           ', '.join('?' for pf in parsed_fields))

    try:
        c.execute(sql, tuple(pf.value for pf in parsed_fields))
    except:
        print (sql)
        raise


def create_index(field, db_conn):
    print("Creating index on {}.{}".format(TABLE_NAME, field.column))
    c = db_conn.cursor()
    sql = "CREATE INDEX {} ON {} ({})".format("{}_{}_idx".format(TABLE_NAME, field.column), TABLE_NAME, field.column)
    c.execute(sql)
    db_conn.commit()


def analyze_database(db_conn):
    c = db_conn.cursor();
    c.execute("ANALYZE")
    db_conn.commit()


def import_file(fname, db_conn, max_allowed=-1):
    f = open(fname)

    row_count = 0
    # Loads everything into RAM at once
    lines = f.readlines()
    f.close()

    lcount = 0
    for line in lines:
        lcount += 1

        if not line:
            continue

        parsed_fields = []

        line = line.strip()
        s = line
        fields_to_read = len(fields)
        while fields_to_read > 0:
            field_index = len(fields) - fields_to_read
            fields_to_read -= 1

            first_quote_pos = s.find('"')
            if first_quote_pos != 0:
                print("Bad line {}? {}".format(lcount - 1, s))

            # Search for <","> instead of just <">, to allow for double quotes inside the
            # field (yeah some of those exist)
            last_quote_pos = max(s.find('"', first_quote_pos + 1), s.find('","', first_quote_pos + 1))
            if last_quote_pos < 0:
                print("Bad line {}? {}".format(lcount - 1, s))
                continue

            next_comma_pos = s.find(',', last_quote_pos + 1)
            if next_comma_pos != last_quote_pos + 1 and fields_to_read > 0:
                print("Bad line {}? {}".format(lcount - 1, s))

            value = s[first_quote_pos + 1:last_quote_pos].strip()
            if fields[field_index]:
                parsed_fields.append(fields[field_index].parse(value))

            if next_comma_pos < 0:
                s = ''
            else:
                s = s[next_comma_pos + 1:]

        if s.strip():
            print("Have extra junk: {}".format(s))

        insert_into_table(parsed_fields, db_conn)
        row_count += 1

        if max_allowed >= 0 and row_count >= max_allowed:
            break

    db_conn.commit()

    for fp in fields:
        if fp and fp.indexed:
            create_index(fp, db_conn)

    analyze_database(db_conn)

    return row_count


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description='Voter data DB importer.')
    p.add_argument('--db', '-d', required=True, dest="db", help="The database file to be created")
    p.add_argument('--input', '-i', required=True, dest="input", help="The input text file containing voter data")
    p.add_argument('--max', '-m', default=-1, type=int, dest="max", help="The maximum number of voters to import (for debugging and stuff)")
    args = p.parse_args()

    import_count = import_file(args.input, create_sqlite_database(args.db), args.max)
    print("Imported: {}".format(import_count))