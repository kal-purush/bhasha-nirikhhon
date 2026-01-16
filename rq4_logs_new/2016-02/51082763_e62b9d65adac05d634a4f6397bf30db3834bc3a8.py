import csv

"""
Programm welches zwei csv dateien mit unterschiedlichen dialekt einliesst und
als csv mit einem ; dilekt speichert.
"""
__author__ = "Schwarz Stephan"
__version__ = "1.0"


class csv_uebung():

    def __init__(self):

        self.f1 = 'a.csv'
        self.f2 = 'input2.csv'
        self.f3 = 'output.csv'

        f1 = self.open_file("input1.csv", "rt")
        f2 = self.open_file("input2.csv", "rt")
        f3 = self.open_file("output.csv", "wt")

        d1 = self.get_dialect(f1)
        d2 = self.get_dialect(f2)

        self.reg_dialect("semicolon", ";")

        r1 = self.read_file(f1, d1)
        r2 = self.read_file(f2, d2)

        out = self.append_files(r1, r2)

        self.write_file(f3, "semicolon", out)

        self.close_file(f1)
        self.close_file(f2)
        self.close_file(f3)

        pass

    def open_file(self, name, type):
        return open(name, type)

    def get_dialect(self, file):
        d = csv.Sniffer().sniff(file.read(1024))
        file.seek(0)
        return d

    def reg_dialect(self, name, delimiter):
        csv.register_dialect(name, delimiter=delimiter)

    def read_file(self, file, dialect):
        return csv.reader(file, dialect)

    def append_files(self, reader1, reader2):
        out = []

        for row in reader1:
            out.append(row)

        for row in reader2:
            out.append(row)

        return out

    def write_file(self, file, dialect, output):
        writer = csv.writer(file, dialect=dialect)
        for i in output:
            writer.writerow(i)

    def close_file(self, file):
        file.close()

if __name__ == '__main__':
    csv_uebung()