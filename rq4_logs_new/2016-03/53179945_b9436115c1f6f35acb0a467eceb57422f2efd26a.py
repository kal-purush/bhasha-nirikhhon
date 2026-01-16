#!/usr/bin/env python3

import sqlite3

class database:

    dbfile = "main.db"

    # HOLE: ID us correlating to the real data
    # SECTION: ID gives order (1 top, 2 middle, 3 bottom)
    #  Start and end range are in cm
    # PLY: ID gives order (1 top, 2 middle, 3 bottom)
    #  Start and end range are in cm

    tables = [
        """CREATE TABLE holes
        (id integer,
        lat real,
        lon real)""",

        """CREATE TABLE segments
        (id integer primary key,
        bid integer,
        seam BOOLEAN,
        sectype text,
        startrange integer,
        endrange integer)""",

        """CREATE TABLE plys
        (id integer primary key,
        bid integer,
        sid integer,
        plytype text,
        startrange integer,
        endrange integer)"""
              ]

    conn = None
    c = None

    def __init__(self):
        self.conn = sqlite3.connect(self.dbfile)
        self.c = self.conn.cursor()

    def build_tables(self):
        for table_string in self.tables:
            self.c.execute(table_string)
        self.conn.commit()

    def run_query(self, qstring):
        return list(self.c.execute(qstring))

    def write_query(self, qstring):
        self.c.execute(qstring)
        self.conn.commit()

    def get_holes(self):
        return self.run_query("""SELECT id, lat, lon
                              FROM holes""")

    # FIXME: SQL INJECTION IS POSSIBLE
    def get_segments(self, holeid):
        return self.run_query("""SELECT id, seam, sectype, startrange, endrange
                              FROM segments
                              WHERE bid={}""".format(holeid))

    # FIXME: SQL INJECTION IS POSSIBLE
    def get_ply(self, holeid, segmentid):
        return self.run_query("""SELECT id, plytype, startrange, endrange
                              FROM plys
                              WHERE bid={} AND sid={}""".format(holeid, segmentid))

    # FIXME: SQL INJECTION IS POSSIBLE
    def put_hole(self, holeid, lat, lon):
        return self.write_query("""INSERT INTO holes
                              VALUES({}, {}, {})""".format(holeid, lat, lon))

    # FIXME: SQL INJECTION IS POSSIBLE
    def put_segment(self, holeid, seam, seamtype, startrange, endrange):
        return self.write_query( """INSERT INTO segments
                         VALUES(Null, {}, {}, '{}', {}, {})
                         """.format(holeid, seam, seamtype, startrange, endrange))

    # FIXME: SQL INJECTION IS POSSIBLE
    def put_ply(self, holeid, segid, seamtype, startrange, endrange):
        return self.write_query("""INSERT INTO plys
                              VALUES(Null, {}, {}, '{}', {}, {})
                              """.format(holeid, segid, seamtype, startrange, endrange))