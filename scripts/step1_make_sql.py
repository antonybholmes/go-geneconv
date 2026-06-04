import collections
import os
import sqlite3
import sys

import pandas as pd
from uuid_utils import uuid7

human_id_map = collections.defaultdict(lambda: collections.defaultdict(str))
mouse_id_map = collections.defaultdict(lambda: collections.defaultdict(str))

alias_map = {}

df_hugo = pd.read_csv(
    "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/hugo/hugo_approved_20260409.tsv",
    sep="\t",
    header=0,
    keep_default_na=False,
)

for i in range(df_hugo.shape[0]):
    hgnc = df_hugo["HGNC ID"].values[i]

    if hgnc == "":
        continue

    ensembl = df_hugo["Ensembl gene ID"].values[i].replace("'", "")
    entrez = df_hugo["NCBI Gene ID"].values[i].replace("'", "")
    symbol = df_hugo["Approved symbol"].values[i].replace("'", "")
    refseq = "|".join(
        [x.strip() for x in df_hugo["RefSeq IDs"].values[i].replace("'", "").split(",")]
    )
    aliases = set()  # [hgnc, ensembl, entrez, symbol])
    aliases.update(
        [
            x.strip()
            for x in df_hugo["Previous symbols"].values[i].replace("'", "").split(",")
        ]
    )
    # aliases.update(
    #     [
    #         x.strip()
    #         for x in df_hugo["Alias symbols"].values[i].replace("'", "").split(",")
    #     ]
    # )

    human_id_map[hgnc]["index"] = len(human_id_map) + 1
    human_id_map[hgnc]["hgnc"] = hgnc

    if ensembl != "":
        human_id_map[hgnc]["ensembl"] = ensembl
    if entrez.isdigit():
        human_id_map[hgnc]["entrez"] = int(entrez)
    if symbol != "":
        human_id_map[hgnc]["symbol"] = symbol
    if refseq != "":
        human_id_map[hgnc]["refseq"] = refseq

    human_id_map[hgnc]["aliases"] = aliases

    for alias in aliases:
        if alias != "" and alias not in alias_map:
            alias_map[alias] = len(alias_map) + 1


df_mgi = pd.read_csv(
    "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/mgi/MGI_Gene_Model_Coord_20240531.txt",
    sep="\t",
    header=0,
    keep_default_na=False,
)


for i, row in df_mgi.iterrows():
    mgi = row["1. MGI accession id"]

    if mgi == "":
        continue

    ensembl = row["11. Ensembl gene id"].replace("'", "")
    entrez = row["6. Entrez gene id"].replace("'", "")
    symbol = row["3. marker symbol"].replace("'", "")

    mouse_id_map[mgi]["index"] = len(mouse_id_map) + 1
    mouse_id_map[mgi]["mgi"] = mgi

    if ensembl != "":
        mouse_id_map[mgi]["ensembl"] = ensembl

    if entrez.isdigit():
        mouse_id_map[mgi]["entrez"] = int(entrez)

    if symbol != "":
        mouse_id_map[mgi]["symbol"] = symbol

    aliases = set()  # [mgi, ensembl, entrez, symbol])
    mouse_id_map[mgi]["aliases"] = aliases

    for alias in aliases:
        if alias != "" and alias not in alias_map:
            alias_map[alias] = len(alias_map) + 1


df_mgi = pd.read_csv(
    "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/mgi/MGI_EntrezGene_20240531.rpt",
    sep="\t",
    header=0,
    keep_default_na=False,
)
df_mgi = df_mgi[df_mgi["Status"] == "W"]

for i in range(df_mgi.shape[0]):
    mgi = df_mgi["MGI Marker Accession ID"].values[i]
    # old name
    symbol = df_mgi["Marker Symbol"].values[i]

    if mgi in mouse_id_map:
        mouse_id_map[mgi]["aliases"].add(symbol)

        if symbol != "" and symbol not in alias_map:
            alias_map[symbol] = len(alias_map) + 1


df_conv = pd.read_csv(
    "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/mgi/HOM_MouseHumanSequence_20240531_dleu2.tsv",
    sep="\t",
    header=0,
    keep_default_na=False,
)
df_conv[df_conv["NCBI Taxon ID"].isin([10090, 9606])]

# classes are pairs of human and mouse genes that are considered orthologs by MGI.
# We will use these classes to find the mapping between human and mouse genes.
classes = df_conv["DB Class Key"].unique()


human_mouse_map = {}
mouse_human_map = {}

for c in classes:
    df_conv_class = df_conv[df_conv["DB Class Key"] == c]

    # print(c, df_conv_class)

    df_human = df_conv_class[df_conv_class["NCBI Taxon ID"] == 9606]

    if df_human.shape[0] == 0:
        continue

    df_mouse = df_conv_class[df_conv_class["NCBI Taxon ID"] == 10090]

    if df_mouse.shape[0] == 0:
        continue

    human_entrez = df_human["EntrezGene ID"].values[0]
    human_symbol = df_human["Symbol"].values[0].replace("'", "")
    hgnc = df_human["HGNC ID"].values[0].replace("'", "")

    mouse_symbol = df_mouse["Symbol"].values[0].replace("'", "")
    mouse_entrez = df_mouse["EntrezGene ID"].values[0]
    mgi = df_mouse["Mouse MGI ID"].values[0].replace("'", "")

    refseq = "|".join(
        [
            x.strip()
            for x in df_mouse["Nucleotide RefSeq IDs"]
            .values[0]
            .replace("'", "")
            .split(",")
        ]
    )

    # print(human_entrez, mouse_entrez)

    if hgnc in human_id_map and mgi in mouse_id_map:
        human_mouse_map[hgnc] = mgi
        mouse_human_map[mgi] = hgnc

        # fix the mouse refseq
        if refseq != "":
            mouse_id_map[mgi]["refseq"] = refseq

    # human_id_map[hgnc]['entrez'] = human_entrez
    # human_id_map[hgnc]['gene_symbol'] = human_symbol
    # human_id_map[hgnc]['hgnc'] = hgnc

    # mouse_id_map[mgi]['entrez'] = mouse_entrez
    # mouse_id_map[mgi]['gene_symbol'] = mouse_symbol
    # mouse_id_map[mgi]['mgi'] = mgi


db = "../data/modules/geneconv/geneconv-20260603.db"

if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")

cursor.execute("""CREATE TABLE aliases (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);""")

cursor.execute("""CREATE INDEX idx_aliases_name ON aliases (LOWER(name));""")

for alias, index in alias_map.items():
    print(f"Inserting alias: {alias} with index: {index}")
    cursor.execute(
        """
    INSERT INTO aliases (id, name) VALUES (?, ?);
    """,
        (index, alias),
    )


cursor.execute("""
CREATE TABLE human (
    id INTEGER PRIMARY KEY,
    gene_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    entrez INTEGER,
    refseq TEXT,
    ensembl TEXT
);
""")

cursor.execute("""CREATE INDEX idx_human_gene_id ON human (LOWER(gene_id));""")
cursor.execute("""CREATE INDEX idx_human_symbol ON human (LOWER(symbol));""")
cursor.execute("""CREATE INDEX idx_human_entrez ON human (entrez);""")
cursor.execute("""CREATE INDEX idx_human_refseq ON human (LOWER(refseq));""")
cursor.execute("""CREATE INDEX idx_human_ensembl ON human (LOWER(ensembl));""")

cursor.execute("""
               CREATE TABLE human_previous_symbols (
                   human_id INTEGER NOT NULL,
                   alias_id INTEGER NOT NULL,
                   PRIMARY KEY (human_id, alias_id),
                   FOREIGN KEY (human_id) REFERENCES human(id) ON DELETE CASCADE,
                   FOREIGN KEY (alias_id) REFERENCES aliases(id) ON DELETE CASCADE
               );""")
cursor.execute(
    """CREATE INDEX idx_human_previous_symbols_alias_id ON human_previous_symbols (alias_id);"""
)
cursor.execute(
    """CREATE INDEX idx_human_previous_symbols_human_id ON human_previous_symbols (human_id);"""
)

cursor.execute("""
CREATE TABLE mouse (
    id INTEGER PRIMARY KEY,
    gene_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    entrez INTEGER,
    refseq TEXT,
    ensembl TEXT
);
""")
cursor.execute("""CREATE INDEX idx_mouse_gene_id ON mouse (LOWER(gene_id));""")
cursor.execute("""CREATE INDEX idx_mouse_symbol ON mouse (LOWER(symbol));""")
cursor.execute("""CREATE INDEX idx_mouse_entrez ON mouse (entrez);""")
cursor.execute("""CREATE INDEX idx_mouse_refseq ON mouse (LOWER(refseq));""")
cursor.execute("""CREATE INDEX idx_mouse_ensembl ON mouse (LOWER(ensembl));""")

cursor.execute("""
CREATE TABLE mouse_previous_symbols (
    mouse_id INTEGER NOT NULL,
    alias_id INTEGER NOT NULL,
    PRIMARY KEY (mouse_id, alias_id),
    FOREIGN KEY (mouse_id) REFERENCES mouse(id) ON DELETE CASCADE,
    FOREIGN KEY (alias_id) REFERENCES aliases(id) ON DELETE CASCADE
);""")
cursor.execute(
    """CREATE INDEX idx_mouse_previous_symbols_alias_id ON mouse_previous_symbols (alias_id);"""
)
cursor.execute(
    """CREATE INDEX idx_mouse_previous_symbols_mouse_id ON mouse_previous_symbols (mouse_id);"""
)

cursor.execute("""CREATE TABLE human_mouse (
    human_id INTEGER NOT NULL,
    mouse_id INTEGER NOT NULL,
    PRIMARY KEY (human_id, mouse_id),
    FOREIGN KEY (human_id) REFERENCES human(id) ON DELETE CASCADE,
    FOREIGN KEY (mouse_id) REFERENCES mouse(id) ON DELETE CASCADE
);""")

for hgnc in sorted(human_mouse_map):
    index = human_id_map[hgnc]["index"]
    symbol = human_id_map[hgnc]["symbol"].replace("'", "")
    entrez = human_id_map[hgnc]["entrez"]
    ensembl = human_id_map[hgnc]["ensembl"].replace("'", "")
    refseq = human_id_map[hgnc]["refseq"].replace("'", "")

    print(index, hgnc, symbol, entrez, ensembl)

    cursor.execute(
        """
    INSERT INTO human (id, gene_id, symbol, entrez, ensembl, refseq) VALUES (?, ?, ?, ?, ?, ?);
    """,
        (index, hgnc, symbol, entrez, ensembl, refseq),
    )

    aliases = human_id_map[hgnc]["aliases"]

    for alias in aliases:
        if alias != "":
            alias_id = alias_map[alias]
            cursor.execute(
                """
            INSERT INTO human_previous_symbols (human_id, alias_id) VALUES (?, ?);
            """,
                (index, alias_id),
            )

for mgi in sorted(mouse_human_map):

    index = mouse_id_map[mgi]["index"]
    symbol = mouse_id_map[mgi]["symbol"].replace("'", "")
    entrez = mouse_id_map[mgi]["entrez"]
    ensembl = mouse_id_map[mgi]["ensembl"].replace("'", "")
    refseq = mouse_id_map[mgi]["refseq"].replace("'", "")

    print(index, mgi, symbol, entrez, ensembl, refseq)
    cursor.execute(
        """
    INSERT INTO mouse (id, gene_id, symbol, entrez, ensembl, refseq) VALUES (?, ?, ?, ?, ?, ?);
    """,
        (index, mgi, symbol, entrez, ensembl, refseq),
    )

    aliases = mouse_id_map[mgi]["aliases"]

    for alias in aliases:
        if alias != "":
            alias_id = alias_map[alias]
            cursor.execute(
                """
            INSERT INTO mouse_previous_symbols (mouse_id, alias_id) VALUES (?, ?);
            """,
                (index, alias_id),
            )

for hgnc in sorted(human_mouse_map):
    mgi = human_mouse_map[hgnc]

    human_index = human_id_map[hgnc]["index"]
    mouse_index = mouse_id_map[mgi]["index"]

    cursor.execute(
        """
    INSERT INTO human_mouse (human_id, mouse_id) VALUES (?, ?);
    """,
        (human_index, mouse_index),
    )

conn.commit()
conn.close()
