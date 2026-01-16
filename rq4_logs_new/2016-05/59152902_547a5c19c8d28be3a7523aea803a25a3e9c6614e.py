from Bio import SeqIO, AlignIO
handle = open("/Users/WRShoemaker/github/GenomeDorm/test/core_gene_alignment.aln", "rU")
alignment = AlignIO.read(handle, "fasta")
print("Alignment of length %i" % alignment.get_alignment_length())