import collections
import re
import numpy as np
import pandas as pd
import os
import gzip

genes = collections.defaultdict(lambda: collections.defaultdict(set))

df = pd.read_csv(
    "data/modules/geneconv/MGI_EntrezGene.rpt",
    sep="\t",
    header=0,
    keep_default_na=False,
    index_col=0,
)

for i in range(df.shape[0]):
    mgi = df.index.values[i]
    entrez = df.iloc[i,7] #df["6. Entrez gene id"].values[i]

    #print(mgi, entrez, type(entrez))
         
    try:
        genes[mgi]["entrez"].add(int(entrez))
    except:
        pass #genes[mgi]["entrez"].add(-1)

 


df = pd.read_csv(
    "data/modules/geneconv/MRK_Sequence.rpt",
    sep="\t",
    header=0,
    keep_default_na=False,
)

for i in range(df.shape[0]):
    mgi = df["MGI Marker Accession ID"].values[i]
    refseq = df["RefSeq transcript IDs"].values[i].split("|")
    ensembl = df["Ensembl transcript IDs"].values[i].split("|")

    genes[mgi]["refseq"].update(refseq)
    genes[mgi]["ensembl"].update(ensembl)


tables = [
    pd.read_csv(
        "data/modules/geneconv/MRK_List1.rpt",
        sep="\t",
        header=0,
        keep_default_na=False,
    ),
    pd.read_csv(
        "data/modules/geneconv/MRK_List2.rpt",
        sep="\t",
        header=0,
        keep_default_na=False,
    ),
]

# some old symbols also have alternative names
widthdrawn_alt_names = collections.defaultdict(set)


for df in tables:
    for i in range(df.shape[0]):
        mgi = df["MGI Accession ID"].values[i]
        symbol = df["Marker Symbol"].values[i]

        if df["Marker Synonyms (pipe-separated)"].values[i] != "":
            symbols = df["Marker Synonyms (pipe-separated)"].values[i].split("|")
        else:
            symbols = []

        status = df["Status"].values[i]
        marker = df["Marker Name"].values[i]
        marker_type = df["Marker Type"].values[i]

        if marker_type != "Gene":
            continue

        if status == "O":
            genes[mgi]["symbol"].add(symbol)
            genes[mgi]["aliases"].add(symbol)
            if len(symbols) > 0:
                genes[mgi]["aliases"].update(symbols)
        elif status == "W":
            withdrawn_alt_name = ""

            matcher = re.search(r"withdrawn, = (.+)", marker)

            if matcher:
                withdrawn_alt_name = matcher.group(1)

            if withdrawn_alt_name != "":
                widthdrawn_alt_names[symbol].add(withdrawn_alt_name)


with open("data/modules/geneconv/mouse.sql", "w") as f:
    for mgi in sorted(genes):

        # merge aliases with withdrawn
        aliases = set(genes[mgi]["aliases"])

        for alias in genes[mgi]["aliases"]:
            if alias in widthdrawn_alt_names:
                aliases.update(widthdrawn_alt_names[alias])

        symbol = "|".join(sorted(genes[mgi]["symbol"])).replace("'", "")
        aliases = "|".join(sorted(aliases)).replace("'", "")

        entrez = list(sorted(genes[mgi]["entrez"]))[0] if len(genes[mgi]["entrez"]) > 0 else -1 #"|".join(sorted(genes[mgi]["entrez"])).replace("'", "")
        refseq = "|".join(sorted(genes[mgi]["refseq"])).replace("'", "")
        ensembl = "|".join(sorted(genes[mgi]["ensembl"])).replace("'", "")

 
        print(
            f"INSERT INTO mouse (gene_id, gene_symbol, aliases, entrez, refseq, ensembl) VALUES ('{mgi}', '{symbol}', '{aliases}', {entrez}, '{refseq}', '{ensembl}');",
            file=f,
        )

        

with open("data/modules/geneconv/mouse_terms.sql", "w") as f:
    for mgi in sorted(genes):
        for id in genes[mgi]["aliases"]:
            id = id.replace("'", "''")

            print(
                f"INSERT INTO mouse_terms (gene_id, term) VALUES ('{mgi}', '{id}');",
                file=f,
            )

        for id in genes[mgi]["entrez"]:
            print(
                f"INSERT INTO mouse_terms (gene_id, term) VALUES ('{mgi}', '{id}');",
                file=f,
            )

        for id in genes[mgi]["refseq"]:
            print(
                f"INSERT INTO mouse_terms (gene_id, term) VALUES ('{mgi}', '{id}');",
                file=f,
            )

        for id in genes[mgi]["ensembl"]:
            print(
                f"INSERT INTO mouse_terms (gene_id, term) VALUES ('{mgi}', '{id}');",
                file=f,
            )
