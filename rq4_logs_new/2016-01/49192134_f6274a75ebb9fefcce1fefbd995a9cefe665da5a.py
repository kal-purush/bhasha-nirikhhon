#! /usr/bin/env python
'''Take transcripts in fasta format and retrieve the
sequences around translation start site, and first intron's 
start/stop site. Demands the fasta file to have the information organized as
follows: 

">5'UTR End|5'UTR Start|Exon start pos#1;Exon start pos#2...|Exon
end pos#1;Exon end pos#2...|Gene ID|Strand".

Compatible with the output from ensembl's BioMart and the output format is 
compatible with the "flat file" option at the online version of WebLogo.

Input args: fasta_file translation_start_site_output_file_name 
intron_start_site_output_file_name intron_end_site_output_file_name
Output: Flat file (written to chosen file names).'''

from Bio import SeqIO
import sys, os.path, pdb
from operator import itemgetter
from Bio.Seq import Seq

import argparse
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("infile", help="infile in fasta format")
parser.add_argument("transl_start_outfile", help="outfile in flat format")
parser.add_argument("intron_start_outfile", help="outfile in flat format")
parser.add_argument("intron_end_outfile", help="outfile in flat format")
args = parser.parse_args()


# Parse fasta file if it is rendered to be an existing file.
if os.path.isfile(args.infile) == True:
	input_file=SeqIO.parse(args.infile,'fasta')
else:
	sys.stderr.write('Cannot open'+sys.argv[1]+'\n')
	sys.exit()

coordinate_list = []
# Loops over every file entry and retireives a list containing the
# [Translation start site, gene start site, first exon start site (or None if there is only 
# one exon in the transcript), second exon start site (or None if there is only one exon in 
# the transcript),first exon end site, Gene ID, strand, distance between gene start and
# translation start site].
for entry in input_file:
	separated_entries=entry.id.split('|')
	for item in separated_entries:
		# If information about translation start position is missing, the transcript is disregarded.
		if sorted(separated_entries[0].split(';'))[0] == '':
			continue
		elif sorted(separated_entries[0].split(';'))[0] == '':
			continue
		else:
			gene_ID = separated_entries[4]
			# Checks if the transcript is on the positive strand and retrieves the coordinates.
			if separated_entries[5] == '1':
				UTR = int(sorted(separated_entries[0].split(';'),key=int)[0])
				gene_start_site = int(sorted(separated_entries[2].split(';'),key=int)[0])
				first_exon_end = int(sorted(separated_entries[3].split(';'),key=int)[0])
				distance_to_first_intron = first_exon_end - gene_start_site
				# If there is more than one exon, the coordinates for the intron are retrieved.
				if len(separated_entries[2].split(';')) > 1:
					second_exon_start = int(sorted(separated_entries[2].split(';'),key=int)[1])
					coordinate_list.append([UTR,gene_start_site,gene_start_site,second_exon_start,first_exon_end,gene_ID,entry.seq,separated_entries[5],distance_to_first_intron])
				else:
					coordinate_list.append([UTR,gene_start_site,None,None,first_exon_end,gene_ID,entry.seq,separated_entries[5],distance_to_first_intron])
			# Checks if the transcript is on the negative strand and retrieves the coordinates.
			elif separated_entries[5] == '-1':
				UTR = int(sorted(separated_entries[1].split(';'),key=int)[0])
				gene_start_site = int(sorted(separated_entries[3].split(';'),key=int)[-1])
				first_exon_end = int(sorted(separated_entries[2].split(';'),key=int)[-1])
				distance_to_first_intron = gene_start_site - first_exon_end
				# If there is more than one exon, the coordinates for the intron are retrieved.
				if len(separated_entries[2].split(';')) > 1:
					second_exon_start = int(sorted(separated_entries[3].split(';'),key=int)[-2])
					coordinate_list.append([UTR,gene_start_site,gene_start_site,second_exon_start,first_exon_end,gene_ID,entry.seq.reverse_complement(),separated_entries[5],distance_to_first_intron])
				else:
					coordinate_list.append([UTR,gene_start_site,None,None,first_exon_end,gene_ID,entry.seq.reverse_complement(),separated_entries[5],distance_to_first_intron])
# Sorts list according to gene ID followed by the distance between gene start and first intron start.
sorted_coordinate_list = sorted(coordinate_list,key=itemgetter(5,8))
# If the list is empty, "No data found" is written to stderr, otherwise, the data for the first
# transcript is added to the list.
if len(sorted_coordinate_list) != 0:
	unique_geneID_coordinates=[sorted_coordinate_list[0]]
else:
	sys.stderr.write('No data found.'+'\n')
	sys.exit()

# Removes every list component followed by the first transcript belonging to the same gene. 	
for item in range(1,len(sorted_coordinate_list)):
	if sorted_coordinate_list[item][5] != unique_geneID_coordinates[-1][5]:
		unique_geneID_coordinates.append(sorted_coordinate_list[item])
	else:
		pass
pdb.set_trace()
# Creates list containing only the sequences for the sites of interest. 
extracted_sequences = []
for transcript in unique_geneID_coordinates:	
	gene_start_coordinate = transcript[1]
	transcript_list = []
	if transcript[2] != None and transcript[3] != None:
		columns = [0,4,3]
	else:
		columns = [0,5,5]
	for column in columns:
		coordinate=transcript[column]
		if coordinate == transcript[5]:
			transcript_list.append(None)
		else:
			# Checks if the whole sequence is shorter than 20 bases and appends None.
			if len(str(transcript[6]))<20:
				transcript_list.append(None)
			# Checks if the transcript is on the positive strand.
			elif transcript[7] == '1':
				# Checks if the extracted seq is exactly 20 bases and appends that seq.
				if len(str(transcript[6][coordinate-gene_start_coordinate-9:coordinate-gene_start_coordinate+11]))==20:	
					transcript_list.append(str(transcript[6][coordinate-gene_start_coordinate-9:coordinate-gene_start_coordinate+11]))
				# Checks if there are less than 9 bases before the site. Adds '-' in front to a total length of 20 bases.
				elif coordinate-gene_start_coordinate-9<0:
					transcript_list.append(str(transcript[6][0:coordinate-gene_start_coordinate+11]).rjust(20,'-'))
				# Checks if there are less than 11 bases after the site. Adds '-' after to a total length of 20 bases.
				elif coordinate-gene_start_coordinate+11>len(transcript[6]):
					transcript_list.append(str(transcript[6][coordinate-gene_start_coordinate-9:len(transcript[6])+1]).ljust(20,'-'))
			# Checks if the transcript is on the negative strand.
			elif transcript[7] == '-1':
				# Checks if the extracted seq is exactly 20 bases and appends that seq.
				if len(str(transcript[6][coordinate-gene_start_coordinate-11:coordinate-gene_start_coordinate+9]))==20:
					transcript_list.append(str(transcript[6][coordinate-gene_start_coordinate-11:coordinate-gene_start_coordinate+9].reverse_complement()))
				# Checks if there are less than 11 bases before the site. Adds '-' in front to a total length of 20 bases.
				elif coordinate-gene_start_coordinate-11<0:
					transcript_list.append(str(transcript[6][0:coordinate-gene_start_coordinate+9].reverse_complement()).rjust(20,'-'))
				# Checks if there are less than 9 bases after the site. Adds '-' after to a total length of 20 bases.
				elif coordinate-gene_start_coordinate+9>len(transcript[6]):
					transcript_list.append(str(transcript[6][coordinate-gene_start_coordinate-11:len(transcript[6])+1]).ljust(20,'-'))
	extracted_sequences.append(transcript_list)

# Writes sequences into the three chosen filenames. Also makes sure to
# create empty files for writing results in.
output_file_names =[args.transl_start_outfile,args.intron_start_outfile,args.intron_end_outfile]
for file_name in output_file_names:
	file = open(file_name,'w')
for sequence in extracted_sequences:	
	for i in range(0,3):
		with open(output_file_names[i],'a') as out:
			if sequence[i] != None:
				out.writelines(str(sequence[i])+'\n')
			else:
				pass




