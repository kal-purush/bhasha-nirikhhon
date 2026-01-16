import itertools
import re
import sys
import random
import pandas as pd
import numpy as np

class Chromosome(object):
    """
    an object for the chromosome with the attributes:
    id -> gives out the chromosome in integer form 1-16, 17 = mitochondrial chromosome
    fastaname -> is only important for the initiation of the object and used by the function createchromosomes
    sequence -> sequence of the whole chromosome
    revsequence -> the reverse sequence of the whole chromosome

    possible operations are:
    gene.id -> gives the chromosome number 1-16, 17 = mitochondrial chromosome
    gene.sequence -> gives the sequence of the chromosome
    gene.revsequence -> gives out the reverse sequence of the chromosome
    """
    def __init__(self, id ,fastaname):
        self._id=id
        self._fastaname=fastaname
        
        #read in the file, delete the headers, concatenate the single lines of the sequence and store them as a single string in sequence
        with open(self._fastaname) as chr_fasta:
            chr_list = chr_fasta.read().splitlines() 
        chr_list[0] = ""
        self._sequence = "".join(chr_list)

        #generate the reverse sequence
        self._revsequence = self._sequence[::-1]

    #add befehl   
    def __add__(self,chromosome):
        if not isinstance(chromosome,Chromosome):
            raise TypeError
        self.sequence=self.sequence+ chromosome.sequence
        self.revsequence=self.revsequence+ chromosome.revsequence
        return self
    
    
    #getter für id, sequence & revsequence

    @property
    def id(self):
        return self._id
    @id.setter
    def id(self, value):
        if not isinstance(value, int):
            raise TypeError("ID must be an Integer.")
        self._id = value
        
    @property
    def sequence(self):
        return self._sequence
    @sequence.setter
    def sequence(self, value):
        if not isinstance(value, str):
            raise TypeError("Sequence must be a String.")
        self._sequence = value
        
    @property
    def revsequence(self):
        return self._revsequence
    @revsequence.setter
    def revsequence(self, value):
        if not isinstance(value, str):
            raise TypeError("RevSequence must be a String.")
        self._revsequence = value
        
    @property
    def fastaname(self):
        return self._fastaname



class Gene(object):
    """
    an object for the gene sequence with the attributes:
    id -> gene name
    chr -> which chromosome the gene is on in integer form, 1-15 und 17 = mitochondrial chromosome and 18 = 2-micron plasmid
    sequence -> sequence of the dna which is transcribed
    count ->
    sequence_binding -> records if the gene is bound (=1) or currently not bound (=0), important for transcription vs replikation (i think)

    possible operations are:
    gene.mid -> gives the gene name
    gene.chr -> gives the chromosome on which the gene is on
    gene.sequence -> gives the sequence of the gene
    """
    def __init__(self, mid, chr, sequence, count=0):
        #self.__location = location
        self.__mid = mid
        self.__chr  = chr
        self.__sequence = sequence 
        self.sequence_binding=[0]*len(sequence)


    ###### COMMENT for DATA GROUP #######
    #feel free to replace 'sequence'-information by start-, end-positions and strand (+/-)

    ###### COMMENT FOR REPLICATION_GROUP #######
    #count: 1 for unreplicated gene, 2 for copied gene 

    #@property
    #def location(self):
    #    return self.__location

    @property
    def mid(self):
        return self.__mid

    @property
    def chr(self):
        return self.__chr

    @property
    def sequence(self):
        return self.__sequence
        
    @sequence.setter
    def sequence(self, value):
        if not isinstance(value, str):
            raise Exception("sequence must be a string")
            # TODO: check for valid nucleotides here
        self.__sequence = value.upper()



def createchromosomes():
    
    chr1=Chromosome(1,"fsa_sequences/S288C_Chromosome I.fsa")
    chr2=Chromosome(2,"fsa_sequences/S288C_Chromosome II.fsa")
    chr3=Chromosome(3,"fsa_sequences/S288C_Chromosome III.fsa")
    chr4=Chromosome(4,"fsa_sequences/S288C_Chromosome IV.fsa")
    chr5=Chromosome(5,"fsa_sequences/S288C_Chromosome V.fsa")
    chr6=Chromosome(6,"fsa_sequences/S288C_Chromosome VI.fsa")
    chr7=Chromosome(7,"fsa_sequences/S288C_Chromosome VII.fsa")
    chr8=Chromosome(8,"fsa_sequences/S288C_Chromosome VIII.fsa")
    chr9=Chromosome(9,"fsa_sequences/S288C_Chromosome IX.fsa")
    chr10=Chromosome(10,"fsa_sequences/S288C_Chromosome X.fsa")
    chr11=Chromosome(11,"fsa_sequences/S288C_Chromosome XI.fsa")
    chr12=Chromosome(12,"fsa_sequences/S288C_Chromosome XII.fsa")
    chr13=Chromosome(13,"fsa_sequences/S288C_Chromosome XIII.fsa")
    chr14=Chromosome(14,"fsa_sequences/S288C_Chromosome XIV.fsa")
    chr15=Chromosome(15,"fsa_sequences/S288C_Chromosome XV.fsa")
    chr16=Chromosome(16,"fsa_sequences/S288C_Chromosome XVI.fsa")
    chrmito=Chromosome(17,"fsa_sequences/S288C_Chromosome Mito.fsa")

    chr_list=[chr1,chr2,chr3,chr4,chr5,chr6,chr7,chr8,chr9,chr10,chr11,chr12,chr13,chr14,chr15,chr16,chrmito]
    
    return chr_list

def createwholegenome(chr_list):

    whole_genome = ""
    for i in range(len(chr_list)):
        whole_genome += whole_genome + chr_list[i].sequence

    return whole_genome

def creategenes():

    with open("fsa_sequences/orf_coding.fasta") as orf_fasta:       #open the file, read it and create a list, where each element is a gene with header+sequence
        orf_list = orf_fasta.read().replace("i>", "").replace("sub>", "").replace("->", "").split(">")
    
    orf_list = orf_list[1:] #entfernen des ersten nicht vorhandenden elements
    orf_splitlist = [""]*len(orf_list)  #initialise the new list[gen][header=0 or gen=1]
    
    for i in range(len(orf_list)):  #Trennen von header und sequenz
        orf_splitlist[i] = orf_list[i].split("\n", 1)   

    """
    Gen Sequenz
    """

    for i in range(len(orf_list)):  #replace "\n" 
        orf_splitlist[i][1] = orf_splitlist[i][1].replace("\n", "")


    gene_seq = [x[1] for x in orf_splitlist]
    

    """
    Gen ID
    """

    header_list = [x[0] for x in orf_splitlist]
    header_split = [""]*len(header_list)

    for i in range(len(header_list)):
        header_split[i] = header_list[i].split(" ", 1)


    gene_id = [x[0] for x in header_split]

    """
    Which Chr
    """

    which_chr_list = re.findall(",\s(Chr\s([IVX]+|Mito)|2-micron plasmid)", "".join(header_list)) # find out which chr with regular expressions and put them in a nested list

    which_chr = [x[0] for x in which_chr_list]

    converter_dictionary = {"Chr I" : 1, "Chr II": 2, "Chr III": 3, "Chr IV": 4, "Chr V": 5, "Chr VI": 6, "Chr VII": 7, "Chr VIII": 8, "Chr IX": 9, "Chr X": 10, "Chr XI": 11, "Chr XII": 12, "Chr XIII": 13, "Chr XIV": 14, "Chr XV": 15, "Chr XVI": 16, "Chr Mito": 17, "2-micron plasmid": 18}

    for i in range(len(which_chr)):
        which_chr[i] = converter_dictionary[which_chr[i]]


    """
    Location
    """

    loc_list = [0]*len(header_list)

    for i in range(len(header_list)):   #creates a list where loc_list[0] = all the tuples of locations for gene 0 in str form
        loc_list[i] = re.findall("([^-][\d]+?)-([\d]+?),", header_list[i])

    
    gene = {}
    for i in range(len(gene_id)):
        gene[gene_id[i]] = Gene(gene_id[i], which_chr[i], gene_seq[i])

    return gene