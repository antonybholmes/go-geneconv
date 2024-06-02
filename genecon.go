package genecon

import (
	"database/sql"
	"strings"

	"github.com/antonybholmes/go-sys"
)

const MOUSE_TO_HUMAN_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
	FROM mouse_to_human, human
 	WHERE LOWER(mouse_to_human.name) = LOWER(?1) AND human.gene_id = mouse_to_human.gene_id`

const HUMAN_TO_MOUSE_SQL = `SELECT human_to_mouse.human_id, mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
	FROM human_to_mouse, mouse
	WHERE LOWER(human_to_mouse.name) = LOWER(?1) AND mouse.gene_id = human_to_mouse.gene_id`

const MOUSE_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
	FROM mouse_official, mouse
 	WHERE LOWER(mouse_official.name) = LOWER(?1) AND mouse.gene_id = mouse_official.gene_id`

const HUMAN_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
	FROM human_official, human
 	WHERE LOWER(human_official.name) = LOWER(?1) AND human.gene_id = human_official.gene_id`

const HUMAN_TAXONOMY_ID = 9606
const MOUSE_TAXONOMY_ID = 10090

const HUMAN_SPECIES = "human"
const MOUSE_SPECIES = "mouse"

const STATUS_FOUND = "found"
const STATUS_NOT_FOUND = "not found"

type BaseGene struct {
	Id         string `json:"id"`
	TaxonomyId uint   `json:"taxonomyId"`
	Species    string `json:"species"`
}

type Gene struct {
	BaseGene
	GeneSymbol string   `json:"geneSymbol"`
	Aliases    []string `json:"aliases"`
	Entrez     uint     `json:"entrez"`
	RefSeq     []string `json:"refseq"`
	Ensembl    []string `json:"ensembl"`
	Status     string   `json:"status"`
}

type Conversion struct {
	BaseGene
	Gene Gene
}

type GeneConDB struct {
	db *sql.DB
}

func NewGeneConDB(file string) *GeneConDB {
	db := sys.Must(sql.Open("sqlite3", file))

	return &GeneConDB{db: db}
}

func (genedb *GeneConDB) Close() {
	genedb.db.Close()
}

func (genedb *GeneConDB) Convert(id string, species string) (*Conversion, error) {
	var aliases string
	var refseq string
	var ensembl string
	var sql string

	species = strings.ToLower(species)

	var ret = Conversion{}

	ret.Species = species

	if species == HUMAN_SPECIES {
		ret.TaxonomyId = HUMAN_TAXONOMY_ID
		ret.Gene.TaxonomyId = MOUSE_TAXONOMY_ID
		ret.Gene.Species = MOUSE_SPECIES
		sql = HUMAN_TO_MOUSE_SQL
	} else {
		ret.TaxonomyId = MOUSE_TAXONOMY_ID
		ret.Gene.TaxonomyId = HUMAN_TAXONOMY_ID
		ret.Gene.Species = HUMAN_SPECIES
		sql = MOUSE_TO_HUMAN_SQL
	}

	err := genedb.db.QueryRow(sql, id).Scan(&ret.Id,
		&ret.Gene.Id,
		&ret.Gene.GeneSymbol,
		&aliases,
		&ret.Gene.Entrez,
		&refseq,
		&ensembl)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	ret.Gene.Aliases = strings.Split(aliases, ",")
	ret.Gene.RefSeq = strings.Split(refseq, ",")
	ret.Gene.Ensembl = strings.Split(ensembl, ",")

	return &ret, nil
}

func (genedb *GeneConDB) Gene(name string, species string) (*Gene, error) {
	var aliases string
	var refseq string
	var ensembl string
	var sql string

	species = strings.ToLower(species)

	var ret = Gene{}

	if species == HUMAN_SPECIES {
		ret.TaxonomyId = HUMAN_TAXONOMY_ID
		ret.Species = HUMAN_SPECIES
		sql = HUMAN_SQL
	} else {
		ret.TaxonomyId = MOUSE_TAXONOMY_ID
		ret.Species = MOUSE_SPECIES
		sql = MOUSE_SQL
	}

	ret.TaxonomyId = HUMAN_TAXONOMY_ID
	ret.Species = HUMAN_SPECIES

	err := genedb.db.QueryRow(sql, name).Scan(&ret.Id,
		&ret.GeneSymbol,
		&aliases,
		&ret.Entrez,
		&refseq,
		&ensembl)

	if err != nil {
		ret.Id = name
		ret.Status = STATUS_NOT_FOUND

		return &ret, nil //fmt.Errorf("there was an error with the database query")
	}

	ret.Status = STATUS_FOUND
	ret.Aliases = strings.Split(aliases, ",")
	ret.RefSeq = strings.Split(refseq, ",")
	ret.Ensembl = strings.Split(ensembl, ",")

	return &ret, nil
}
