import collections
import sys
import pandas as pd
from uuid_utils import uuid7


human_id_map = collections.defaultdict(lambda: collections.defaultdict(str))
mouse_id_map = collections.defaultdict(lambda: collections.defaultdict(str))

df_hugo = pd.read_csv(
    "../data/modules/geneconv/hugo_20240524.tsv",
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
    aliases = set([hgnc, ensembl, entrez, symbol])
    aliases.update(
        [
            x.strip()
            for x in df_hugo["Previous symbols"].values[i].replace("'", "").split(",")
        ]
    )
    aliases.update(
        [
            x.strip()
            for x in df_hugo["Alias symbols"].values[i].replace("'", "").split(",")
        ]
    )

    human_id_map[hgnc]["hgnc"] = hgnc

    if ensembl != "":
        human_id_map[hgnc]["ensembl"] = ensembl
    if entrez != "":
        human_id_map[hgnc]["entrez"] = entrez
    if symbol != "":
        human_id_map[hgnc]["symbol"] = symbol

    if len(aliases) > 0:
        human_id_map[hgnc]["tags"] = aliases


df_mgi = pd.read_csv(
    "../data/modules/geneconv/MGI_Gene_Model_Coord_20240531.rpt",
    sep="\t",
    header=0,
    index_col=None,
    keep_default_na=False,
)


for i in range(df_mgi.shape[0]):
    mgi = df_mgi["1. MGI accession id"].values[i]

    if mgi == "":
        continue

    ensembl = df_mgi["11. Ensembl gene id"].values[i].replace("'", "")
    entrez = df_mgi["6. Entrez gene id"].values[i].replace("'", "")
    symbol = df_mgi["3. marker symbol"].values[i].replace("'", "")
    aliases = set([mgi, ensembl, entrez] + symbol.split(","))

    mouse_id_map[mgi]["mgi"] = mgi

    if ensembl != "":
        mouse_id_map[mgi]["ensembl"] = ensembl

    if entrez != "":
        mouse_id_map[mgi]["entrez"] = entrez

    if symbol != "":
        mouse_id_map[mgi]["symbol"] = symbol

    if len(aliases) > 0:
        mouse_id_map[mgi]["tags"] = aliases

    print(mgi)


df_mgi = pd.read_csv(
    "../data/modules/geneconv/MGI_EntrezGene_20240531.rpt",
    sep="\t",
    header=0,
    keep_default_na=False,
)
df_mgi = df_mgi[df_mgi["Status"] == "W"]

for i in range(df_mgi.shape[0]):
    mgi = df_mgi["MGI Marker Accession ID"].values[i]
    # old name
    symbol = df_mgi["MGI Marker Accession ID"].values[i]

    if mgi in mouse_id_map:
        mouse_id_map[mgi]["tags"].add(symbol)


df_conv = pd.read_csv(
    "../data/modules/geneconv/HOM_MouseHumanSequence_20240531_dleu2.tsv",
    sep="\t",
    header=0,
    keep_default_na=False,
)
df_conv[df_conv["NCBI Taxon ID"].isin([10090, 9606])]

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

    # print(human_entrez, mouse_entrez)

    human_mouse_map[hgnc] = mgi
    mouse_human_map[mgi] = hgnc

    # human_id_map[hgnc]['entrez'] = human_entrez
    # human_id_map[hgnc]['gene_symbol'] = human_symbol
    # human_id_map[hgnc]['hgnc'] = hgnc

    # mouse_id_map[mgi]['entrez'] = mouse_entrez
    # mouse_id_map[mgi]['gene_symbol'] = mouse_symbol
    # mouse_id_map[mgi]['mgi'] = mgi


print(len(human_mouse_map))


with open("../data/modules/geneconv/conv.sql", "w") as f:

    print("BEGIN TRANSACTION;", file=f)

    for hgnc in sorted(human_mouse_map):
        mgi = human_mouse_map[hgnc]

        symbol = human_id_map[hgnc]["symbol"].replace("'", "")
        entrez = human_id_map[hgnc]["entrez"].replace("'", "")
        ensembl = human_id_map[hgnc]["ensembl"].replace("'", "")

        human_tags = "|".join(
            filter(lambda x: len(x) > 0, sorted(human_id_map[hgnc]["tags"]))
        )
        mouse_tags = "|".join(
            filter(lambda x: len(x) > 0, sorted(mouse_id_map[mgi]["tags"]))
        )

        id = uuid7()

        # add a comma at the end of tags for exact search e.g. exactly BCL6 => 'BCL6,'
        print(
            f"INSERT INTO human (id, gene_symbol, entrez, ensembl, human_tags, mouse_tags) VALUES ('{hgnc}', '{symbol}', '{entrez}', '{ensembl}', '{human_tags}', '{mouse_tags}');",
            file=f,
        )

    print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)

    for mgi in sorted(mouse_human_map):
        hgnc = mouse_human_map[mgi]

        symbol = mouse_id_map[mgi]["symbol"]

        entrez = mouse_id_map[mgi]["entrez"]

        ensembl = mouse_id_map[mgi]["ensembl"]

        human_tags = "|".join(
            filter(lambda x: len(x) > 0, sorted(human_id_map[hgnc]["tags"]))
        )
        mouse_tags = "|".join(
            filter(lambda x: len(x) > 0, sorted(mouse_id_map[mgi]["tags"]))
        )

        id = uuid7()

        print(
            f"INSERT INTO mouse (id, gene_symbol, entrez, ensembl, human_tags, mouse_tags) VALUES ('{mgi}', '{symbol}', '{entrez}', '{ensembl}', '{human_tags}', '{mouse_tags}');",
            file=f,
        )

    print("COMMIT;", file=f)
