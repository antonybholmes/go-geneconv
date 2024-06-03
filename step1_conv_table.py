import collections
import numpy as np
import pandas as pd
import os
import gzip


df = pd.read_csv(
    "data/modules/geneconv/HOM_MouseHumanSequence.rpt",
    sep="\t",
    header=0,
    keep_default_na=False,
)

genes = collections.defaultdict(lambda: collections.defaultdict(tuple))

for i in range(df.shape[0]):
    key = df["DB Class Key"].values[i]
    tax = df["NCBI Taxon ID"].values[i]
    mgi = df["Mouse MGI ID"].values[i]
    hugo = df["HGNC ID"].values[i]
    symbol = df["Symbol"].values[i]
    entrez = df["EntrezGene ID"].values[i]

    if tax == 9606:
        genes[key][tax] = (hugo, symbol, entrez)
    if tax == 10090:
        genes[key][tax] = (mgi, symbol, entrez)

with open("data/modules/geneconv/conversion.sql", "w") as f:
    for key in genes:
        if len(genes[key]) < 2:
            continue

        human = genes[key][9606]
        mouse = genes[key][10090]

        #print(human)
        #print(mouse)

        print(
            f"INSERT INTO conversion (human_gene_id, mouse_gene_id, human_gene_symbol, mouse_gene_symbol, human_entrez, mouse_entrez) VALUES ('{human[0]}', '{mouse[0]}', '{human[1]}', '{mouse[1]}', {human[2]}, {mouse[2]});",
            file=f,
        )
