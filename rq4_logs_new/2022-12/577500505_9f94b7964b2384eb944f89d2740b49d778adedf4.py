#!/usr/bin/env python3

import glob
import os
import anndata
import pandas as pd
import scipy.sparse as sp
import io
import csv
import argparse

def gen_gene_table(rdir):

    input_htseqs = [file for file in glob.glob(os.path.join(rdir, "*/Pass1/htseq-count.txt"))]

    gene_lists = set()
    gene_counts = list()

    sample_names = tuple(os.path.basename(fn) for fn in glob.glob(os.path.join(rdir, "*")))

    for htseq_file_path in input_htseqs:
        htseq_file = open(htseq_file_path)

        htseq_file.seek(0)
        gene_list, gene_count = list(
            zip(*[map(str.strip, line.split("\t")) for line in htseq_file])
        )

        gene_lists.add(gene_list)
        gene_counts.append(gene_count)

    gene_list = gene_lists.pop()


    gene_cell_counts = sp.vstack([sp.csr_matrix(list(map(int, gc))) for gc in gene_counts])
    adata = anndata.AnnData(
        gene_cell_counts,
        obs=pd.DataFrame(index=sample_names),
        var=pd.DataFrame(index=gene_list),
    )

    adata.write_h5ad(os.path.join(rdir, "output"))

if __name__ == "__main__":       

    parser = argparse.ArgumentParser()

    parser.add_argument('--results_dir',   
        type=str, 
        help='''where to put the data analyzed by this script''', 
        default="/data/results")

    args = parser.parse_args()

    gen_gene_table(args.results_dir)