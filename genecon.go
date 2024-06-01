package genecon

import (
	"database/sql"

	"github.com/antonybholmes/go-sys"
)

const MOUSE_TO_HUMAN_SQL = `SELECT human.id, human.gene_symbol, human.entrez, human.refseq, human.ensembl
	FROM mouse_to_human, human
 	WHERE LOWER(mouse_to_human.name) = LOWER(?1) AND human.gene_id = mouse_to_human.gene_id`

const HUMAN_TO_MOUSE_SQL = `SELECT human_to_mouse.human_id, mouse.id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
	FROM human_to_mouse, mouse
	WHERE LOWER(human_to_mouse.name) = LOWER(?1) AND mouse.gene_id = human_to_mouse.gene_id`

const HUMAN_SQL = `SELECT human.id, human.gene_symbol, human.entrez, human.refseq, human.ensembl
	FROM human_official, human
 	WHERE LOWER(human_official.name) = LOWER(?1) AND human.gene_id = human_official.gene_id`

const HUMAN_TAXONOMY_ID = 9606
const MOUSE_TAXONOMY_ID = 10090

type Gene struct {
	Id         string   `json:"id"`
	GeneSymbol string   `json:"geneSymbol"`
	Aliases    []string `json:"aliases"`
	Entrez     uint     `json:"entrez"`
	RefSeq     string   `json:"refseq"`
	Ensembl    string   `json:"ensembl"`
	TaxonomyId uint     `json:"taxonomyId"`
	Species    string   `json:"species"`
}

type HumanToMouseConversion struct {
	HumanId string `json:"humanId"`
	Gene
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

func (genedb *GeneConDB) HumanToMouse(id string) (*HumanToMouseConversion, error) {
	var ret = HumanToMouseConversion{}

	ret.TaxonomyId = MOUSE_TAXONOMY_ID

	err := genedb.db.QueryRow(HUMAN_TO_MOUSE_SQL,
		id).Scan(&ret.HumanId, &ret.Id, &ret.GeneSymbol, &ret.Aliases)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	return &ret, nil
}

func (genedb *GeneConDB) Human(id string) (*Gene, error) {
	var ret = Gene{}

	ret.TaxonomyId = HUMAN_TAXONOMY_ID

	err := genedb.db.QueryRow(HUMAN_SQL,
		id).Scan(&ret.Id, &ret.GeneSymbol, &ret.Aliases)

	if err != nil {
		return nil, err //fmt.Errorf("there was an error with the database query")
	}

	return &ret, nil
}
