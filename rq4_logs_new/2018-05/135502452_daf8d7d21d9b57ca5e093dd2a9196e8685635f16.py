
# coding: utf-8

# # Protein to Transcripts
# Quick script for gettin human coding transcripts from protein names,
# some of which may be Cluster of Differentiation (CD) names. If CD
# names are present, uses Uniprot-published CD->Uniprot 
# conversion table (found at https://www.uniprot.org/docs/cdlist.txt).
# 
# If script has been run once already, can use generated --c flag
# to use stored pickled dictionary of conversions.
# 
# After converting all (if any) from CD names:
# 1. Inputs Uniprot IDs or gene symbol/alias to mygene package 
# (http://mygene.info/) to get associated ENSG IDs from Ensemble.
# 2. Sses Ensembl API (http://rest.ensembl.org) to get all transcripts
# as ENST IDs associated with that ENSG ID. 
# 3. Then cross-checks for published list of protein-coding human 
# transcripts from Gencode 
# (https://www.gencodegenes.org/releases/28lift37.html).
# 
# Script can be run both in Jupyter Notebook and on command line. Use 
# 
# `jupyter nbconvert --to=python protoscripts.ipynb`
# 
# to convert this to a CLI script, and **update the next cell.**

# In[2]:


proteins = "markerlist.txt"
pathtogencode = "gencode.v28.pc_transcripts.fa.gz"
numbases = 'all'
outputname = None
CDpkl = True


# If running on jupyter, **do not** run this next cell.

# In[3]:


import argparse

### Setup argument parser ###

parser = argparse.ArgumentParser(description="""
Quick script for gettin human coding transcripts from protein names,
some of which may be Cluster of Differentiation (CD) names. If CD
names are present, uses Uniprot-published CD->Uniprot 
conversion table (found at https://www.uniprot.org/docs/cdlist.txt).

If script has been run once already, can use generated --c flag
to use stored pickled dictionary of conversions.

After converting all (if any) from CD names:
1. Inputs Uniprot IDs or gene symbol/alias to mygene package 
(http://mygene.info/) to get associated ENSG IDs from Ensemble.
2. Sses Ensembl API (http://rest.ensembl.org) to get all transcripts
as ENST IDs associated with that ENSG ID. 
3. Then cross-checks for published list of protein-coding human 
transcripts from Gencode 
(https://www.gencodegenes.org/releases/28lift37.html).
""", formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('p', metavar='proteins', type=str,
                    help='path to .txt w/ protein names each on new line')

parser.add_argument('-o', metavar='outputname', type=str, default=None,
                    help='output filename (default will use timestamp.fa)')

parser.add_argument('-g', metavar='pathtogencode', type=str,
                    default = 'gencode.v28.pc_transcripts.fa.gz',
                    help='path to Gencode protein-coding transcripts .gz')

parser.add_argument('-n', metavar='numbases', type=int, default=0,
                    help='number of bases (e.g. 200: first 200; -100: last 100')

parser.add_argument('--m', action='store_true', default=False,
                    help='option to use stored ./CDdict.pkl')

parser.add_argument('--c', action='store_true', default=False,
                    help='option to use stored ./CDdict.pkl')

args = parser.parse_args()  # Parse arguments

proteins = args.p
outputname = args.o
pathtogencode = args.g
numbases = args.n
CDpkl = args.c
if numbases == 0:
    numbases = 'all'


# Import necessary packages.

# In[4]:


import timeit
print('Beginning program.')
tic = timeit.default_timer()
import gzip
from itertools import islice
import mygene # this package gets transcipts IDs from UniProt IDs
from  tqdm import tqdm
import pandas as pd
import urllib.request
from time import localtime, strftime
import pickle as pkl
import requests
import ast
import math
mg = mygene.MyGeneInfo() # the function used to query


# ### Convert from Cluster of Differentiation (CD) to UniProt
# Uniprot IDs are easier to work with and have corresponding Ensembl transcript IDs.

# In[5]:


if not CDpkl:
    print('Querying UniProt for CD conversion table.')
    '''
    Uniprot has a website to convert CDs into Uniprot IDs.
    I assume it is updated frequently; latest release April 2018.
    The URL is a text file but has poor delimitters, so parsing is
    a bit challenging.
    '''
    
    #  this header is where the table starts.
    header =     'CD      ' +     'Swiss-Prot           ' +     'MIM     ' +      'Gene              '  +     'Name(s) for the protein\n'
    
    dflines = list() #  make a list of lines for DataFrame conversion
    take = False
    with urllib.request.urlopen('https://www.uniprot.org/docs/cdlist.txt') as a:
        for i in [j.decode("utf-8") for j in a.readlines()]: # need to decode the website using utf-8
            if i == header:
                take = True # start taking the lines
                first = True # the first line to take
            if take:
                if first == True:
                    first = False
                    dflines.append(['CD Number','Swiss-Prot Entry Name','UniProt ID']) # this will be the first line
                    continue
                if i == '\n':
                    break
                if i[:2] == 'CD': # if the line starts with CD, take it
                    # only take the first three columns, all  other  columns 
                    # contain spaces,  hard to parse
                    goodsplit = i.split()[:3] 
                    if goodsplit[-1] != 'N.A.': # only if the uniprot ID exists
                        dflines.append(goodsplit)

    # Make a dictionary with the conversions
    CDdict = dict(zip([i[0] for i in dflines[1:]],[i[2] for i in dflines[1:]]))
    pkl.dump(CDdict,open('CDdict.pkl','wb')) # dump the dictionary for future use
else:
    CDdict = pkl.load(open('CDdict.pkl','rb'))


# ###  Input gene names
# Gene names are fed as a text file with a name on each line. Names in the text files can be a gene symbol/alias/CD string, with no spaces, or, if it goes by two names, with a single space between the two names. The text file can also have comments with lines starting with '#' that will be ignored.
# 
# For example:
# 
# `#These proteins are for my project`
# 
# `#Gene protein names start here:`
# 
# `CD137`
# 
# `CD69`
# 
# `ICOS (CD278)`
# 
# `OX40 (CD134)`
# 
# `HAVCR2`
# 
# For now querying will use only the first name. Future plans to make it use the second if no results are returned, or ask for manual input of a UniProt ID.

# In[6]:


# get markers from somewhere
inputmarks = [i.strip() for i in open('markerlist.txt','r').readlines() if i[0] != '#']
marksdict = dict()
convmarks = list() # add to a new list

for i in inputmarks:  # for  each  marker value
    if len(i.split(' ')) == 2:
        i = i.split(' ')[0] #  separated by a space
    try:
        convmarks.append(CDdict[i])
    except KeyError:
        convmarks.append(i)

for i in range(len(inputmarks)):
    marksdict[inputmarks[i]] = {'Converted': convmarks[i]}


# ###  Query the MyGene package
# 
# This next call queries the mygene package. If duplicate hits are found, all of them will be returned. If no hits are found, that transcript will not appear in the generated FASTA. Note that many duplicates might be returned for  gene symbols/aliases inputs, usually because the gene summary (from Ensembl) inlcudes the name (e.g. its an associated/related protein) or they share an alias. The resulting FASTA files should be manually curated by Uniprot ID to ensure only the correct transcript is present. 

# In[7]:


print('Querying MyGene.')
dictout = mg.querymany(convmarks, scopes=['uniprot','symbol','alias'], 
                       fields=['ensembl.gene','uniprot'], 
                       species='human', returnall=True)


# ###  Flatten MyGene output
# The mygene output finds all possible transcript IDs matching the UniProt ID. This is a bit cumbersome because output is either a list, dict, or list of dicts, with some of those even containing lists. It's good though because it generalizes the single UniProt ID and corresponding gene to all possible transcripts generated from that locus. Crossed with the gencode human coding transcripts, only those that actually code proteins will be kept.
# 
# Flatten the ouput, then put everything into a pandas dataframe.

# In[8]:


def flatten(genes): # function for flattening
    genes = str(genes)
    genelist = list()
    unilist = list()
    for i in range(len(genes)):
        if genes[i:i+4] == 'ENSG':
            genelist.append(genes[i:i+15])
#         input(genes[i:i+15])
        if genes[i:i+15] == "'Swiss-Prot': '":
            unilist.append(genes[i+15:i+21])
    return genelist, unilist

invmarksdict = {v['Converted']: k for k, v in marksdict.items()}
for i in dictout['out']: # the output
    ensglist, unilist =  flatten(i) # get list of all the IDs
    if 'ENSGs' in marksdict[invmarksdict[i['query']]].keys():
        marksdict[invmarksdict[i['query']]]['ENSGs'] += ensglist
    else:
        marksdict[invmarksdict[i['query']]]['ENSGs'] = ensglist
    if 'Uniprot' in marksdict[invmarksdict[i['query']]].keys():
        marksdict[invmarksdict[i['query']]]['Uniprot'] += unilist
    else:
        marksdict[invmarksdict[i['query']]]['Uniprot'] = unilist
dflist =  list()
for i in marksdict.keys():
    for j in marksdict[i]['Uniprot']:
        for k in marksdict[i]['ENSGs']:
            dflist.append([i,j,k])
conversiondf = pd.DataFrame(dflist,columns=['Input','UniProt','ENSG'])


# ### Query Ensembl API
# The ensembl API can be querried for transcript IDs and transcript support levels (TSL). TSLs may be important for a single gene coding for a single protein that has multiple documented coding transcripts. Those transcripts that have less support (i.e. closer to TSL=5 (TSL=1 is highly supported)) might want to be dropped. 

# In[1]:


print('Querying Ensembl API.')
take = False
ensgs = conversiondf['ENSG'].values
server = "http://rest.ensembl.org"

transcripts =  dict()

for i in tqdm(ensgs):
    transcripts[i] =  dict()
    ext = "/overlap/id/%s?feature=transcript" % i
    r = requests.get(server+ext, headers={ "Content-Type" : "application/json"})
    if not r.ok:
        r.raise_for_status()
        sys.exit()
    decoded = r.json()
    jsondicts = ast.literal_eval(repr(decoded))
    for j in jsondicts:
        try:
            tsl =  j['transcript_support_level'] # catch those that don't have TSL
        except:
            continue
        transcripts[i][j['transcript_id']] = tsl


# ### Write a new FASTA File
# Comb through the protein-coding human transcripts and catch all transcripts that match those from the provided proteins. 

# In[2]:


print('Writing FASTA file.')
def evensplit(n,split): # quick generator for splitting sequence across 80-char lines
    startnext = 0
    stoplast = split
    for i in range(math.ceil(n/split)):
        yield (startnext,stoplast)
        startnext = stoplast
        stoplast += split
        if stoplast > n:
            stoplast = n

seq = ''
numbases = -10
if outputname == None:
    outputname = '%s_transcripts.fa' % strftime("%Y%m%d%H%M", localtime())
elif outputname[-3:] != '.fa':
    outputname += '.fa'
with gzip.open(pathtogencode, 'rt') as fastas, open(outputname,'w') as file:
    lines = islice(fastas, 0, None)
    counttxnscripts = 0
    for line in lines:
        if line[0] == '>':
            if seq != '':
                if numbases != 'all':
                    if numbases < 0:
                        seq = seq[numbases:]
                    elif numbases > 0:
                        seq = seq[:numbases]
                    else:
                        raise ValueError
                for i in evensplit(len(seq),80): # FASTA format recommends no more than 80 chars per line
                    file.write(seq[i[0]:i[1]]+'\n')
                seq = ''
            take = False
            splitline = line.split('|')
            ENSG = splitline[1].split('.')[0]  #  get ENSG
            ENST = splitline[0].split('.')[0][1:]  #  get ENSG
            if ENSG in transcripts.keys():
                if ENST in transcripts[ENSG].keys():
                    counttxnscripts += 1
                    index =  conversiondf.index[conversiondf['ENSG'] == ENSG].tolist()[0]
                    newhead = '>' + ('|').join(conversiondf.iloc[index]) + '|%s|' % ENST +                     'TSL:%s' % str(transcripts[ENSG][ENST]) + '\n'
#                     if counttxnscripts > 170:
#                         input(line)
#                         input(newhead)
                    file.write(newhead)
                    take = True
                    continue
        if take == True:
            seq += line.strip('\n')
toc = timeit.default_timer()
print("Done. Total program time was %d seconds." % (toc - tic))


# ### Additional Code
# Code for future iterations of this program.

# In[11]:


# def manualinput(notfoundmarks):
#     length = len(notfoundmarks)
#     print("There are %d marker(s) not found. You can choose to ignore these or input UniProt IDs for some or" + \
#          " all of them. To skip all, type "n". To input UniProt IDs, type 'y'." % length)
#     answer = input("Manually input UniProt IDs? (y/n) ")
#     if answer == 'y':
#         print("Either type in UniProt ID or '-' to skip.")
#         uniprotlist = list()
#         for i in notfoundmarks:
#             uniprotlist.append(input('Uniprot ID for %s' % i))
#         return uniprotlist
#     else:
#         return ['-']*length


# In[12]:


# if len(dictout['missing']) != 0:
#     print('Trying other names.')
#     for i in dictout['missing']:
#         for j in inputmarks:
#             if i in j and len(i.split(' ')) == 2:
#                 str2 = splitted[1][1:len(splitted[1])-1]
#                 singledictout = mg.query(str2,scopes=['uniprot','symbol','alias'], 
#                                          fields=['ensembl.gene','uniprot'], 
#                                          species='human', returnall=True)
#                 if len(singledictout['missing']) == 1:
#                     stillnoluck.append(i)
#                 else:
#                     dictout['out'].append(singledictout)
#             else:
#                 stillnoluck.append(i)
#     manualuni = manualinput(stillnoluck)
#     for k in manualuni:
#         if i != '-':
#             singledictout = mg.query(k,scopes=['uniprot','symbol','alias'], 
#                          fields=['ensembl.gene','uniprot'], 
#                          species='human', returnall=True)
#             if len(singledictout['missing']) == 1:
#                 print('Provided Uniprot ID %s not found. Skipping.' % k)
#             else:
#                 dictout['out'].append(singledictout)
#         else:
#             dictout['out'].append({'query': i'skipped': True})


# In[13]:


# while len(goodscripts) == 0:
#                         TSL += 1
#                         goodscripts = [i['transcript_id'] for i in jsondicts if int(i['transcript_support_level']) <= TSL]
