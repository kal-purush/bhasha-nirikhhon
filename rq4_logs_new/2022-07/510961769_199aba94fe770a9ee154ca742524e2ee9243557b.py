import sys
import glob
import os.path


def filename_to_seq_number(filename):
    filename = filename.replace('.txt', '')
    seq = filename[:len(filename)//2] # Split filename in half, first part
    pages = filename[len(filename)//2:] # Split filename in half, last part
    seq = seq.lstrip('0')
    pages = pages.lstrip('0')

    return seq, pages


if len(sys.argv) != 2:
    print('usage: python3 text2tei.py <text-folder>')
    exit()

text_path = sys.argv[1] + '/*.txt'
text_files = glob.glob(text_path)


print('<div type="book">')
for file in sorted(text_files):
    filename = os.path.basename(file)
    seq_n, pg_n = filename_to_seq_number(filename)

    with open(file) as fp:
        text = fp.read()

    text = text.replace('&', '&amp;')

    print('<pb n="{}" />'.format(pg_n))
    print('<!-- https://texashistory.unt.edu/ark:/67531/metapth5872/m1/{}/ -->'.format(seq_n))
    print('<p>')
    print(text)
    print('</p>')
print('</div>')