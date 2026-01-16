## Get mutation count from trios and genotype files 
## load to database
## Usage: python sql_load.py inputGT gtstatsFile databaseName hapSize

import sys
import pandas as pd
import numpy as np
import sqlite3

gtfile = sys.argv[1]
maffile = sys.argv[2]
database = sys.argv[3]
n = int(sys.argv[4])

connex = sqlite3.connect(database)

## load gtstats for MAF
maf = pd.read_csv(maffile, sep="\t")
maf.columns=["CHROM", "POS", "REF", "ALT", "MAC", "MAF"]
maf.to_sql(name="maf", con=connex, if_exists="replace")

## load vcf file into multiple tables if sample size (haplotype) is greater than 1000
vcfinfo = pd.read_csv(gtfile, sep="\t", header=0, index_col=False, usecols=range(9))
vcfinfo.to_sql(name="vcfinfo", con=connex, if_exists="replace")

resid = n%1000
nbin = n//1000

if nbin == 0:
  colsindex = range(9, n+9)
  for chunk in pd.read_csv(gtfile, sep="\t", header=0, index_col=False, usecols=colsindex, chunksize=20000):
    chunk.to_sql(name="vcf1", con=connex, if_exists="append")
else:
  for i in range(nbin):
    colsindex = range(i*1000+9, (i+1)*1000+9)
    for chunk in pd.read_csv(gtfile, sep="\t", header=0, index_col=False, usecols=colsindex, chunksize=20000):
      chunk.to_sql(name="vcf"+str(i+1), con=connex, if_exists="append")
  if resid != 0:
    colsindex = range(nbin*1000+9, n+9)
    for chunk in pd.read_csv(gtfile, sep="\t", header=0, index_col=False, usecols=colsindex, chunksize=20000):
      chunk.to_sql(name="vcf"+str(nbin+1), con=connex, if_exists="append")

connex.commit()
connex.close()