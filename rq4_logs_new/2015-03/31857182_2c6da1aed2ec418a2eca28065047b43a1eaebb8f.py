


import os
import sys
import hashlib

import argparse

parser = argparse.ArgumentParser(description='Ereader')
parser.add_argument('-n', '--np', help='lines per page, default is 40', default = 40)
parser.add_argument('book', help='location of book txt file')
args = vars(parser.parse_args())

np = int(args['np'])


book_file = args['book']
book = open(book_file)



def get_md5(book):
    with book as file_to_check:
        data = file_to_check.read()
    # pipe contents of the file through
        md5_bk = hashlib.md5(data).hexdigest()
    return md5_bk

def get_rc(rc_file = "~/.read_rc"):
    md_dict = {}
    if os.path.isfile(rc_file):
        rcr = open(rc_file, "r")
        tmp = rc_file + "~"
        rcw = open(tmp, "w")
    else:
        make_rcr = open(rc_file, "w")
        make_rcr.write(md5_bk + "\t" + "0")
        make_rcr.close()
        get_rc(rc_file)
    for line in rcr:
        l = line.split("\t")
        pp = int(l[1])
        bk = l[0]
        md_dict[bk] = pp
    return md_dict, rcr, rcw

def read_book(book, np=40, pg=0):
    count = 0
    #book = open("dracula.txt")
    book = open(book_file)
    for line in book:
        count  += 1
        if count > np:
            prompt = "Press n or p for next page, q to quit\n"
            ans = raw_input(prompt)
            if ans == "n"  or ans == "p":
                count = 0
                pg = pg + np
            if ans == "q":
                book.close()
                md_dict[md5_bk] = pg
                return
        else:
            print line

def read_book2(np, pg):
    count = 0
    #book = open("dracula.txt")
    book = open(book_file)
    pg = md_dict[md5_bk]
    if pg > 0:
        skip_lines(book, pg)
    for line in book:
        count  += 1
        if count > np:
            prompt = "Press n or p for next page, q to quit\n"
            ans = raw_input(prompt)
            if ans == "n":
                count = 0
                pg = pg + np
                md_dict[md5_bk] = pg
                print pg
            if ans == "q":
                book.close()
                #md_dict[md5_bk] = pg
                return
        else:
            print line

book = open(book_file)

def skip_lines(book, n):
    line_offset = []
    offset = 0
    for line in book:
        line_offset.append(offset)
        offset += len(line)
    book.seek(0)
# Now, to skip to line n (with the first line being line 0), just do
    book.seek(line_offset[n])

def write_rcw(rcw, md_dict):
    for k, v in md_dict.items():
        key = '%s' %k
        rcw.write(key)
        val = '%d' %v
        rcw.write("\t" + val)
    rcw.close()




#rc_file = "/Users/ashleysdoane/.read_rc"

rc_file = os.path.expanduser("~/.read_rc")


book = open(book_file)
md5_bk = get_md5(book)

md_dict, rcr, rcw = get_rc(rc_file) #return record, read and write rec files

pg = md_dict[md5_bk]


read_book2(np, pg)
#print md_dict
write_rcw(rcw, md_dict)

tmp = rc_file + "~" #filename for rcw

os.rename(tmp, rc_file)



