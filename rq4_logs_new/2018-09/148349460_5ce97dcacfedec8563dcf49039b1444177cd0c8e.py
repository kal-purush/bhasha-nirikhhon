import pandas as pd
import csv

def parser(infile):
    table = pd.read_csv(infile, sep="\t")
    list_comp = table["Parameter Value[Compound]"].tolist()
    t = pd.read_csv("metadata.txt", sep="\t")
    restricted = t.loc[t["Parameter Value[Compound]"].isin(list_comp)]
    restricted.to_csv("select2.txt", sep="\t")

if __name__ == "__main__":
    parser("second_select.txt")