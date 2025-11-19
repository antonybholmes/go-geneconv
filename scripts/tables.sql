PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- CREATE TABLE human_mouse (
	-- id INTEGER PRIMARY KEY ASC,
	-- db TEXT NOT NULL,
	-- gene_symbol TEXT NOT NULL,
	-- entrez TEXT NOT NULL,
	-- ensembl TEXT NOT NULL,
	-- human_tags TEXT NOT NULL,
	-- mouse_tags TEXT NOT NULL);
-- CREATE INDEX human_mouse_human_tags_idx ON human_mouse (human_tags);
-- CREATE INDEX human_mouse_mouse_tags_idx ON human_mouse (mouse_tags);

-- CREATE TABLE mouse_human (
	-- id INTEGER PRIMARY KEY ASC,
	-- db TEXT NOT NULL,
	-- gene_symbol TEXT NOT NULL,
	-- entrez TEXT NOT NULL,
	-- ensembl TEXT NOT NULL,
	-- human_tags TEXT NOT NULL,
	-- mouse_tags TEXT NOT NULL);
-- CREATE INDEX mouse_human_human_tags_idx ON human_mouse (human_tags);
-- CREATE INDEX mouse_human_mouse_tags_idx ON human_mouse (mouse_tags);

CREATE VIRTUAL TABLE human USING fts5(
	id,
	gene_symbol,
	entrez,
	ensembl,
	human_tags,
	mouse_tags);

CREATE VIRTUAL TABLE mouse USING fts5(
	id,
	gene_symbol,
	entrez,
	ensembl,
	human_tags,
	mouse_tags);
