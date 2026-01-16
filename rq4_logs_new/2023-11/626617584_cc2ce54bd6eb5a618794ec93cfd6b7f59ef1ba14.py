#!/util/opt/anaconda/deployed-conda-envs/packages/python/envs/python-3.9/bin/python

# functions
def usage():
    usage="""
        Usage:
        ./get_taxonomy_from_taxid.py -d <file.csv> -k <number_of_clusters> -e <epsilon> -i <number_of_iterations> -o <output_dir>
        Options:
           -d (directory)    path to a file with data. A tab delimited file
                             containing contigID, subject, p_value, pident, taxid
           -l (directory)    path to a file with RNAME and TAXID table. A tab deliminted
                             file containing RNAME (reference name) and TAXID number
           -t (integer)      path to the directory containing nodes.dmp and 
                             rankedlineage.dmp filed from NCBI databases
           -o (directory)    output_dir with minimum an output file name
           -h                display this help and exit
    """
    print(usage)
def command_line_interaction():
    import sys, getopt
    # get arguments
    # stores in a tuple
    # https://www.cyberciti.biz/faq/python-command-line-arguments-argv-example/
    opts, args = getopt.getopt(sys.argv[1:], 'd:l:t:o:h')
    # initialize values
    dataset = ""
    n_clusters = -1
    epsilon = -1
    iterations = -1
    output_dir = ""
    for o,a in opts:
        if o == '-d':
            dataset = a     # a csv_file
        if o == '-l':
            tax_list = a     # a csv_file
        if o == '-t':
            tax_db_dir = a  # a path to a directory contating nodes and rankedlineage
        if o == '-o':
            output_dir = str(a)
        if o == '-h':
            usage()
            sys.exit(2)     # exits after -h is used
    return([dataset,tax_list,tax_db_dir,output_dir])

def get_data(dataset,tax_list):
    import pandas as pd
    import math
    data=[]
    # to add column names to the blast output
    # base it on the -outfmt "6 .." option on blastn
    #col_names=["qaccver", "saccver", "sallseqid", "sscinames", "evalue", "pident", "qcovs", "staxids", "sskingdoms"]   # if input is a blast output with these fields specified in the -outftm 6 format
    col_names=["QNAME", "FLAG", "RNAME", "POS", "MAPQ", "CIGAR", "RNEXT", "PNEXT", "TLEN", "SEQ"]   # if input is a sam file with only mandatory fields
    col_names=["QNAME", "RNAME","MAPQ"]   # if input is a sam file with only mandatory fields
    df_sam= pd.read_csv(dataset,sep="\t", header=None, names=col_names).drop_duplicates(subset="RNAME")
    colnames_taxid=["RNAME","TAXID"]
    df_tax=pd.read_csv(tax_list, sep="\t", header=None, names=colnames_taxid).drop_duplicates(subset="RNAME")
    df=pd.merge(df_sam,df_tax,on="RNAME")
    
    return df

# esearch -db nucleotide -query "NC_012783.2" | esummary | xtract -pattern DocumentSummary -element TaxId
# epost -db nucleotide -id NC_012783.2, NC_022098.1 | efetch -format uid | esummary -db nucleotide | xtract -pattern DocumentSummary -element TaxId,AccessionVersion
# get taxids with this command (must have entrez-direct installed)
# list of e-direct database names: https://www.ncbi.nlm.nih.gov/books/NBK25497/table/chapter2.T._entrez_unique_identifiers_ui/?report=objectonly
# for i in `echo $b`;do tax=$(esearch -db nucleotide -query $i | esummary | xtract -pattern DocumentSummary -element TaxId); printf "$i\t$tax\n" >> seqid_to_taxid.txt;done

# import libraries
import os, sys, getopt, time
import pandas as pd
import taxidTools

# getting input and parameters
inputs=command_line_interaction()
dataset=inputs[0]
tax_list=inputs[1]
tax_db_dir=inputs[2]
output_dir=inputs[3]
#print("Looking for %d clusters in dataset %s.\nUsing %d iterations with epsilon=%.2f" % (n_clusters, dataset, iterations, epsilon))

df=get_data(dataset, tax_list)
contigs=df.RNAME.unique()
print(contigs)

# taxonomy ranks to consider form the taxonomy tree
rankFilter=['species', 'genus', 'subfamily', 'family', 'order', 'class', 'phylum', 'kingdom', 'clade', 'superkingdom']
# get taxonomy db into a taxidTools object
nodes=os.path.join(tax_db_dir,"nodes.dmp")
rankedlineage=os.path.join(tax_db_dir,"rankedlineage.dmp")
tax=taxidTools.Taxonomy.from_taxdump(nodes,rankedlineage)

entries=[]
for contig in contigs:
    df_temp=df.query('RNAME == @contig & MAPQ >= 2 & TAXID.notna()')
    # QMAP=-1*log10(Prob of mapping position is wrong)
    if df_temp.empty == False:
        print(df_temp)
        contigID=df_temp["QNAME"].iloc[0]
        subjectACC=df_temp["RNAME"].iloc[0]
        taxID=df_temp["TAXID"].iloc[0]
        print(contigID)
        print(taxID)

        dic={}

        dic["contigID"]=contigID
        dic["subjectACC"]=subjectACC
        dic["taxID"]=taxID
        print(dic)

        # get taxonomy information
        l=tax.getAncestry(taxID)
        rankList=[name.rank for name in l]
        namesList=[name.name for name in l]
        print(rankList)
        print(namesList)

        n=0
        for rankL in rankList:
            if rankL in rankFilter:
                dic[rankL]=namesList[n]
            n+=1
        print(dic)
        entries.append(dic)

print(entries)

col_names=entries[1].keys()
df=pd.DataFrame()

for col_name in col_names:
    temp_vector=[]
    for entry in entries:
        if col_name in entry:
            element=entry[col_name]
        else:
            element=str('unknown_'+col_name)
        temp_vector.append(element)
    temp_df=pd.DataFrame(temp_vector,columns=[col_name])
    df=pd.concat([df,temp_df],axis=1)
print(df)
df.to_csv(output_dir, index=False)

