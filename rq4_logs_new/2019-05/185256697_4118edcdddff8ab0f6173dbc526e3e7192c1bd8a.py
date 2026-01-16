import pandas as pd
import pickle
import numpy as np

import pyBigWig as bw
from seekr.fasta_reader import Reader
from collections import defaultdict

import gtfparse

phyloP = bw.open('../mm10.60way.phyloP60wayPlacental.bw')

mouseGTF = gtfparse.read_gtf('/Users/danielsprague/Downloads/gencode.vM21.annotation.gtf')
mouseGTF['gene_id'] = [i[:-2] for i in mouseGTF['gene_id']]

mouseGTF.head()

hits = pickle.load(open('fixed_mm10_pctranscripts_4_99_250win_25slide_scores.p','rb'))

df = pd.DataFrame.from_dict(hits,orient='index')

df.head()

query_dict = defaultdict(list)

newHits = {}

for row in df.iterrows():
    query_dict = defaultdict(list)
    name = row[0]
    data = row[1:][0][0]
    for i in data:
        query_dict[i[1]].append(i[0])
    hits[name] = query_dict.copy()
    query_dict.clear()

df_2 = pd.DataFrame.from_dict(hits,orient='index')

qfa = Reader('./queries.fa')
q_h = qfa.get_headers()

q_map = dict(zip(list(range(13)),q_h))

q_map

df_2.rename(columns=q_map,inplace=True)

from tqdm import tqdm_notebook as tqdm

def dSeekrCoords(df):
    hitDict = {}
    for row in tqdm(df.iterrows(),total=len(df)):
        loc,data = row[0],row[1]
        wtf = []
        for idx,queryHitLocs in enumerate(data):
            if isinstance(queryHitLocs,list):
                continuousCoords =  [np.arange(start=i*100,stop=i*100+1000,step=1) for i in queryHitLocs]
                mergedCoords = np.concatenate(continuousCoords)
                flatCoords = np.unique(mergedCoords)
                wtf.append(flatCoords)
        hitDict[loc] = np.unique(np.concatenate(wtf))
    return hitDict

wtf = dSeekrCoords(df_2)

seekrAgg=[]
intronsAgg=[]
exonsAgg=[]

for info,coords in tqdm(wtf.items()):
    infoSplit = info.split('|')
    coordsArray = np.array(coords,dtype=int)
    gene,transcript,strand,start,end = infoSplit[0][1:],infoSplit[1],int(infoSplit[2]),int(infoSplit[3]),int(infoSplit[4])
    if strand == 1:
        coordsArray+=start
    elif strand == -1:
        coordsArray = end - coordsArray
    if not mouseGTF[mouseGTF['gene_id']==gene].empty:
        chrom = mouseGTF[mouseGTF['gene_id']==gene]['seqname'].unique()[0]
        exons = mouseGTF[(mouseGTF['gene_id']==gene)&(mouseGTF['feature']=='exon')]
        if not exons.empty:
            exonCoords = list(zip(exons['start'],exons['end']))
            exonCoords = [np.arange(istart,iend) for istart,iend in exonCoords]
            exonCoords = np.unique(np.concatenate(exonCoords))
        else:
            exonCoords = np.array([])
        allTranscriptCoords = np.arange(start,end)
        setAllTranscriptCoords,setExonCoords,setSeekrCoords = set(allTranscriptCoords),set(exonCoords),set(coordsArray)
        exonsInTranscript = setAllTranscriptCoords.intersection(exonCoords)
        intronsNoSeekrHits = setAllTranscriptCoords - exonsInTranscript - setSeekrCoords
        seekrNoExons = setSeekrCoords - exonsInTranscript
        SEEKR,INTRONS,EXONS = sorted(list(seekrNoExons)),sorted(list(intronsNoSeekrHits)),sorted(list(exonsInTranscript))

        if (SEEKR) and (len(SEEKR)>1):
            seekrPhyloP = np.array([phyloP.values(chrom,start=i,end=i+1) for i in SEEKR])
        if (INTRONS) and (len(INTRONS)>1):
            intronsPhyloP = np.array([phyloP.values(chrom,start=i,end=i+1) for i in INTRONS])
        if (EXONS) and (len(EXONS)>1):
            exonsPhyloP = np.array([phyloP.values(chrom,start=i,end=i+1) for i in EXONS])
        seekrAgg.append(np.median(seekrPhyloP))
        intronsAgg.append(np.median(intronsPhyloP))
        exonsAgg.append(np.median(exonsPhyloP))


df = pd.DataFrame([seekrAgg,intronsAgg,exonsAgg])
df = df.T
df.columns=['seekr','introns','exons']


import seaborn as sns
import matplotlib.pyplot as plt

sns.set_context('talk')
plt.figure(figsize=(4,7))
sns.boxplot(data=df,showfliers=False)
plt.axhline(y=0,linestyle='--',color='black')
plt.tight_layout()
plt.savefig('./proteincoding_seekrhits_phyloP_new.pdf',bbox_inches='tight')