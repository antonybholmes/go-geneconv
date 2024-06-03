PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS conversion;
CREATE TABLE conversion (
    id INTEGER PRIMARY KEY ASC, 
    human_id TEXT NOT NULL, 
    mouse_id TEXT NOT NULL,
    human_gene_symbol TEXT NOT NULL, 
    mouse_gene_symbol TEXT NOT NULL,
    human_entrez INTEGER NOT NULL, 
    mouse_entrez INTEGER NOT NULL,
    UNIQUE(human_id, mouse_id));
CREATE INDEX conversion_human_id_idx ON conversion (human_id);
CREATE INDEX conversion_mouse_id_idx ON conversion (mouse_id);
 
