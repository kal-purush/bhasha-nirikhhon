__author__ = 'chhuey'

filename = "shows-slugs-sorted.txt"
single_line = ""

with open(filename,'r') as slugs_file:
    for show_slug in slugs_file:
        #show_slug.rstrip('\n')
        single_line += "{0}|".format(show_slug.rstrip('\n'))
        print show_slug

print single_line