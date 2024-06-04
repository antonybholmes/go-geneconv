package geneconv

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/antonybholmes/go-sys"
)

const MOUSE_TO_HUMAN_EXACT_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
	FROM mouse_terms, conversion, human
 	WHERE LOWER(mouse_terms.term) = LOWER(?1) AND conversion.mouse_gene_id = mouse_terms.gene_id AND human.gene_id = conversion.human_gene_id`

const HUMAN_TO_MOUSE_EXACT_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
	FROM human_terms, conversion, mouse
	WHERE LOWER(human_terms.term) = LOWER(?1) AND conversion.human_gene_id = human_terms.gene_id AND mouse.gene_id = conversion.mouse_gene_id`

const MOUSE_TO_HUMAN_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
	FROM mouse_terms, conversion, human
 	WHERE mouse_terms.term LIKE ?1 AND conversion.mouse_gene_id = mouse_terms.gene_id AND human.gene_id = conversion.human_gene_id`

const HUMAN_TO_MOUSE_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
	FROM human_terms, conversion, mouse
	WHERE human_terms.term LIKE ?1 AND conversion.human_gene_id = human_terms.gene_id AND mouse.gene_id = conversion.mouse_gene_id`

const MOUSE_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
	FROM mouse_terms, mouse
 	WHERE mouse_terms.term LIKE ?1 AND mouse.gene_id = mouse_terms.gene_id`

const HUMAN_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
	FROM human_terms, human
 	WHERE human_terms.term LIKE ?1 AND human.gene_id = human_terms.gene_id`

const MOUSE_EXACT_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
	FROM mouse_terms, mouse
	WHERE LOWER(mouse_terms.term) = LOWER(?1) AND mouse.gene_id = mouse_terms.gene_id`

const HUMAN_EXACT_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
	FROM human_terms, human
	WHERE LOWER(human_terms.term) = LOWER(?1) AND human.gene_id = human_terms.gene_id`

const HUMAN_TAXONOMY_ID = 9606
const MOUSE_TAXONOMY_ID = 10090

const HUMAN_SPECIES = "human"
const MOUSE_SPECIES = "mouse"

type Taxonomy struct {
	Id      uint64 `json:"id"`
	Species string `json:"species"`
}

var HUMAN_TAX = Taxonomy{
	Id:      HUMAN_TAXONOMY_ID,
	Species: HUMAN_SPECIES,
}

var MOUSE_TAX = Taxonomy{
	Id:      MOUSE_TAXONOMY_ID,
	Species: MOUSE_SPECIES,
}

type BaseGene struct {
	Taxonomy Taxonomy `json:"taxonomy"`
	Id       string   `json:"id"`
}

type Gene struct {
	BaseGene
	Symbol  string   `json:"symbol"`
	Aliases []string `json:"aliases"`
	Entrez  int      `json:"entrez"`
	RefSeq  []string `json:"refseq"`
	Ensembl []string `json:"ensembl"`
}

type Conversion struct {
	Search string `json:"id"`
	Genes  []Gene `json:"genes"`
}

type ConversionResults struct {
	From        Taxonomy     `json:"from"`
	To          Taxonomy     `json:"to"`
	Conversions []Conversion `json:"conversions"`
}

type GeneResult struct {
	Id    string `json:"id"`
	Genes []Gene `json:"genes"`
}

type GeneConvDB struct {
	db *sql.DB
}

func NewGeneConvDB(file string) *GeneConvDB {
	db := sys.Must(sql.Open("sqlite3", file))

	return &GeneConvDB{db: db}
}

func (geneconvdb *GeneConvDB) Close() {
	geneconvdb.db.Close()
}

func (geneconvdb *GeneConvDB) Convert(search string, fromSpecies string, toSpecies string, exact bool) (Conversion, error) {
	var sql string

	var ret Conversion

	ret.Search = search
	ret.Genes = make([]Gene, 0, 5)

	fromSpecies = strings.ToLower(fromSpecies)

	var tax Taxonomy

	if fromSpecies == MOUSE_SPECIES {
		if exact {
			sql = MOUSE_TO_HUMAN_EXACT_SQL
		} else {
			sql = MOUSE_TO_HUMAN_SQL
		}

		tax = HUMAN_TAX
	} else {
		// default to assuming human to mouse if poorly specified
		if exact {
			sql = HUMAN_TO_MOUSE_EXACT_SQL
		} else {
			sql = HUMAN_TO_MOUSE_SQL
		}

		tax = MOUSE_TAX
	}

	if !exact {
		search = fmt.Sprintf("%%%s%%", search)
	}

	//log.Debug().Msgf("%s", sql)

	rows, err := geneconvdb.db.Query(sql, search)

	if err != nil {
		return ret, err
	}

	defer rows.Close()

	genes, err := rowsToGenes(rows, tax)

	if err != nil {
		return ret, err
	}

	ret.Genes = append(ret.Genes, genes...)

	return ret, nil
}

func (geneconvdb *GeneConvDB) GeneInfo(search string, species string, exact bool) ([]Gene, error) {

	var sql string

	species = strings.ToLower(species)

	var ret = make([]Gene, 0, 5)

	var tax Taxonomy

	if species == MOUSE_SPECIES {
		if exact {
			sql = MOUSE_EXACT_SQL
		} else {
			sql = MOUSE_SQL
		}

		tax = MOUSE_TAX
	} else {
		if exact {
			sql = HUMAN_EXACT_SQL
		} else {
			sql = HUMAN_SQL
		}

		tax = HUMAN_TAX
	}

	if !exact {
		search = fmt.Sprintf("%%%s%%", search)
	}

	rows, err := geneconvdb.db.Query(sql, search)

	if err != nil {
		return ret, err
	}

	defer rows.Close()

	return rowsToGenes(rows, tax)
}

func rowsToGenes(rows *sql.Rows, tax Taxonomy) ([]Gene, error) {
	var aliases string
	//var entrez string
	var refseq string
	var ensembl string

	var ret = make([]Gene, 0, 5)

	for rows.Next() {
		var gene Gene
		gene.Entrez = -1
		gene.Taxonomy = tax

		err := rows.Scan(&gene.Id,
			&gene.Symbol,
			&aliases,
			&gene.Entrez,
			&refseq,
			&ensembl)

		//log.Debug().Msgf("err %s", err)

		// keep going even if there is a failure
		if err == nil {
			// convert entrez to numbers
			// for _, e := range strings.Split(entrez, ",") {
			// 	n, err := strconv.ParseUint(e, 10, 64)

			// 	if err == nil {
			// 		gene.Entrez = append(gene.Entrez, n)
			// 	}
			// }

			gene.Aliases = strings.Split(aliases, "|")
			gene.RefSeq = strings.Split(refseq, "|")
			gene.Ensembl = strings.Split(ensembl, "|")
		}

		ret = append(ret, gene)
	}

	return ret, nil
}
