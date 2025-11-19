import collections
import re
from uuid_utils import uuid7
import numpy as np
import pandas as pd
import os
import gzip

genes = collections.defaultdict(lambda: collections.defaultdict(set))

df = pd.read_csv(
    "data/modules/geneconv/hugo_20240524.tsv",
    sep="\t",
    header=0,
    keep_default_na=False,
)

for i in range(df.shape[0]):
    hgi = df["HGNC ID"].values[i]
    symbol = df["Approved symbol"].values[i]
    previous_symbols = df["Previous symbols"].values[i]

    if df["Previous symbols"].values[i] != "":
        previous_symbols = df["Previous symbols"].values[i].split(", ")
    else:
        previous_symbols = []

    alias_symbols = df["Alias symbols"].values[i]

    if df["Alias symbols"].values[i] != "":
        alias_symbols = df["Alias symbols"].values[i].split(", ")
    else:
        alias_symbols = []

    status = df["Status"].values[i]

    if status == "Approved":
        genes[hgi]["symbol"].add(symbol)
        genes[hgi]["aliases"].add(symbol)
        if len(alias_symbols) > 0:
            genes[hgi]["aliases"].update(alias_symbols)
        if len(previous_symbols) > 0:
            genes[hgi]["aliases"].update(previous_symbols)
        if df["RefSeq IDs"].values[i] != "":
            genes[hgi]["refseq"].update(df["RefSeq IDs"].values[i].split(", "))
        if df["Ensembl gene ID"].values[i] != "":
            genes[hgi]["ensembl"].update(df["Ensembl gene ID"].values[i].split(", "))
        if df["NCBI Gene ID"].values[i] != "":
            genes[hgi]["entrez"].update(df["NCBI Gene ID"].values[i].split(", "))


with open("data/modules/geneconv/human.sql", "w") as f:
    for hgi in sorted(genes):

        # merge aliases with withdrawn
        aliases = set(genes[hgi]["aliases"])

        symbol = "|".join(sorted(genes[hgi]["symbol"])).replace("'", "")
        aliases = "|".join(sorted(aliases)).replace("'", "")

        entrez = (
            list(sorted(genes[hgi]["entrez"]))[0]
            if len(genes[hgi]["entrez"]) > 0
            else -1
        )  # "|".join(sorted(genes[hgi]["entrez"])).replace("'", "")
        refseq = "|".join(sorted(genes[hgi]["refseq"])).replace("'", "")
        ensembl = "|".join(sorted(genes[hgi]["ensembl"])).replace("'", "")

        id = uuid7()

        print(
            f"INSERT INTO human (id, gene_id, gene_symbol, aliases, entrez, refseq, ensembl) VALUES ('{id}', '{hgi}', '{symbol}', '{aliases}', {entrez}, '{refseq}', '{ensembl}');",
            file=f,
        )


with open("data/modules/geneconv/human_terms.sql", "w") as f:
    for hgi in sorted(genes):
        for term in genes[hgi]["aliases"]:
            id = uuid7()
            term = term.replace("'", "''")
            print(
                f"INSERT INTO human_terms (id, gene_id, term) VALUES ('{id}', '{hgi}', '{term}');",
                file=f,
            )

        for term in genes[hgi]["entrez"]:
            id = uuid7()
            print(
                f"INSERT INTO human_terms (id, gene_id, term) VALUES ('{id}', '{hgi}', '{term}');",
                file=f,
            )

        for term in genes[hgi]["entrez"]:
            id = uuid7()
            print(
                f"INSERT INTO human_terms (id, gene_id, term) VALUES ('{id}', '{hgi}', '{term}');",
                file=f,
            )

        for term in genes[hgi]["refseq"]:
            id = uuid7()
            print(
                f"INSERT INTO human_terms (id, gene_id, term) VALUES ('{id}', '{hgi}', '{term}');",
                file=f,
            )

        for term in genes[hgi]["ensembl"]:
            id = uuid7()
            print(
                f"INSERT INTO human_terms (id, gene_id, term) VALUES ('{id}', '{hgi}', '{term}');",
                file=f,
            )
