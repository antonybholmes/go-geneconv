rm data/modules/geneconv/geneconv.db
cat tables.sql | sqlite3 data/modules/geneconv/geneconv.db
cat data/modules/geneconv/conversion.sql | sqlite3 data/modules/geneconv/geneconv.db
