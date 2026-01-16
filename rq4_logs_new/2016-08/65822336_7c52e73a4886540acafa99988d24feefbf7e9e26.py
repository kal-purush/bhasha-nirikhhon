import codecs
import os
import re
from pywikibot import xmlreader

dump = xmlreader.XmlDump('/Users/aaharoni/dev/pwb/hewiki-20160801-pages-meta-current.xml')

common_trails = {}
common_trails_filename = '/Users/aaharoni/dev/pwb/common-trails.he.txt'
common_trails_file = codecs.open(
	common_trails_filename,
	encoding = 'utf-8',
	mode = 'r'
)

for line in common_trails_file:
	common_trails[line.rstrip(os.linesep)] = True

common_trails_file.close()

entry_counter = 0
all_trails_counts = {}
all_trails_titles = {}

for entry in dump.parse():
	title = unicode(entry.title)
	print 'entry #' + str(entry_counter) + ': ' + title

	# Including only main space
	if entry.ns not in ('0'):
		continue

	# Before the space there is a ' ' char,
	# whatever that is
	trails = re.findall(
		r'\]\]([^0-9<*";:|\'.,=+({}?!/  \n\t)[\]-]+)',
		entry.text,
		re.UNICODE
	)

	for raw_trail in trails:
		trail = unicode(raw_trail)

		if trail in common_trails:
			continue

		if trail in all_trails_counts:
			all_trails_counts[trail] += 1
		else:
			all_trails_counts[trail] = 1
			all_trails_titles[trail] = {}

		all_trails_titles[trail][title] = True

	#if entry_counter == 100000:
	#	break

	entry_counter += 1

single_trails_filename = 'trails/_single_trails.txt'
single_trails_file = codecs.open(
	single_trails_filename,
	encoding = 'utf-8',
	mode = 'w'
)

problematic_trails_filename = 'trails/_problematic_trails.txt'
problematic_trails_file = codecs.open(
	problematic_trails_filename,
	encoding = 'utf-8',
	mode = 'w'
)

trail_counter = 0

for trail in sorted(all_trails_counts, key = all_trails_counts.get):
	trail_counter += 1
	trail_counter_str = str(trail_counter)

	instance_count = all_trails_counts[trail]
	instance_count_line = 'Trail #' + trail_counter_str + ' <<' + trail + '>> found ' + str(instance_count) + ' times'
	print instance_count_line

	if instance_count == 1:
		line = '* Trail <<' + trail + '>> found in [[' + all_trails_titles[trail].keys()[0] + "]]\n"
		single_trails_file.write(line)

		continue

	trail_titles_filename = 'trails/trail_' + trail_counter_str + '_' + trail + '_titles.txt'
	trail_titles_filename = trail_titles_filename.replace(
		u'\u200F', # rlm
		'EXPLICITRLM'
	)

	try:
		trail_titles_file = codecs.open(
			trail_titles_filename,
			encoding = 'utf-8',
			mode = 'w'
		)
	except IOError:
		problematic_trails_file.write('problematic trail <<' + trail + ">>\n")
		problematic_trails_file.write("found in:\n")
		for title in all_trails_titles[trail]:
			problematic_trails_file.write(title + "\n")

		continue

	trail_titles_file.write(instance_count_line + "\n")

	for title in all_trails_titles[trail]:
		trail_titles_file.write('* [[' + title + ']]' + "\n")

	trail_titles_file.close()

single_trails_file.close()
problematic_trails_file.close()