import os
import fnmatch

def combine_documents(path=os.path.join(os.curdir, "data/processed"), name='corpus.txt'):
    """
    combines all .txt files in a directory into one file with the specified name.
    remakes combined file if it already exists.
    """
    outname=os.path.join(path, name)
    if os.path.exists(outname):
        os.remove(outname)
    filenames = [f for f in os.listdir(path) if fnmatch.fnmatch(f, '*.txt')]
    with open(outname, 'w') as outfile:
        print "Combining documents..."
        for fname in filenames:
            print fname
            with open(os.path.join(path, fname)) as infile:
                outfile.write(infile.read())

def fix_ascii(text):
    """ascii lookup table"""
    ascii = {
            '\x80':'',
            '\x82':",",
            '\xa2':"",
            '\xaa':"",
            '\xac':"",
            '\xb1':"",
            '\xc2':"",
            '\xc3':"",
            '\x85':"...",
            '\x91':"'",
            '\x92':"'",
            '\x93':'"',
            '\x94':'"',
            '\x96':"-",
            '\x97':"-",
            '\xa6':":",
            '\xa9':"",
            '\xe2':"",
            '\xe9':"",
            '\xea':"",
            '\xf1':""
            }
    for k, v in ascii.iteritems():
        text = text.replace(k, v)
    return text

def process_files(inpath=os.path.join(os.curdir, "data/raw"), outpath=os.path.join(os.curdir, "data/processed")):
    filenames = [f for f in os.listdir(inpath) if fnmatch.fnmatch(f, '*.txt')]
    print "fixing ascii encoding..."
    for f in filenames:
        print f
        infile = os.path.join(inpath, f)
        outname = os.path.join(outpath, f)
        with open(outname, 'w') as outfile:
            outfile.write(fix_ascii(open(infile).read()))