#!/usr/bin/env python
from Bio import SeqIO

#this is a comment

reference = 'S288C_reference_orf_trans_all.fasta'

genelist = [rec.id for rec in SeqIO.parse(reference, 'fasta')]

