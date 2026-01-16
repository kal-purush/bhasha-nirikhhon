#!/usr/bin/env python3


import sys
import os
import argparse
import subprocess

DIR = os.path.dirname(os.path.abspath(__file__))
BIN_DIR = "{}/bin".format(DIR)
DATA_DIR = "{}/data".format(DIR)
SOFTWARES = [name for name in os.listdir(BIN_DIR) if os.path.isdir(os.path.join(BIN_DIR, name))]

def get_fastaid(ftp):
    return "f001"

def get_dbid(fp):
    return "d001"

def get_resultid(query, database):
    return "r001"

def id2fasta(fastaid):
    return "{}/{}.fasta".format(DATA_DIR, fastaid)

def id2db(dbid):
    return "{}/{}".format(DATA_DIR, dbid)

def id2result(resultid):
    return "{}/{}.m8".format(DATA_DIR, resultid)

def software_createdb(args):
    fp = args.fasta
    dbid = get_dbid(fp)
    out = id2db(dbid)
    cmd = "{0}/{1}/db.sh {2} {3}".format(BIN_DIR, args.subcommand, fp, out)

    status = subprocess.call(cmd.split())
    if status == 0:
        print("DONE: create {}".format(out))
    else:
        print("ERROR: fail to create database from {}".format(fp), file=sys.stderr)
        exit(1)

def software_search(args):
    query = args.query
    database = args.database
    result = id2result(get_resultid(query, database))
    cmd = "{0}/{1}/search.sh {2} {3} {4}".format(BIN_DIR, args.subcommand, query, database, result)
    print(cmd)
    
    status = subprocess.call(cmd.split())
    if status == 0:
        print("DONE: output result to {}".format(result))
    else:
        print("ERROR: search fail", file=sys.stderr)
        exit(1)
    
def fetch(args):
    ftp = args.ftp
    fastaid = get_fastaid(ftp)
    out = id2fasta(fastaid)
    cmd = "{}/wget.sh {} {}".format(BIN_DIR, ftp, out)
    
    status = subprocess.call(cmd.split())
    if status == 0:
        print("DONE: wget to {}".format(out))
    else:
        print("ERROR: fail to wget {}".format(ftp), file=sys.stderr)
        exit(1)
    
def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand")
    
    parser_fetch = subparsers.add_parser("fetch", help="fetch new FASTA.")
    parser_fetch.add_argument("ftp", type=str, help="ftp address to fetch FASTA from.")
    parser_fetch.set_defaults(func=fetch)
    
    parser_software = subparsers.add_parser("_software", help="run operation for the software specified.", aliases=SOFTWARES)
    subparsers_software = parser_software.add_subparsers()
    
    parser_software_createdb = subparsers_software.add_parser("createdb", help="create a new database.")
    parser_software_createdb.add_argument("fasta", help="FASTA filepath to create a new database.")
    parser_software_createdb.set_defaults(func=software_createdb)
    
    parser_software_query = subparsers_software.add_parser("search", help="search a query against a database.")
    parser_software_query.add_argument("query", help="query filepath.")
    parser_software_query.add_argument("database", help="database to be searched.")
    parser_software_query.set_defaults(func=software_search)
    
    args = parser.parse_args()
    args.func(args)
    

if __name__=="__main__":
    main()