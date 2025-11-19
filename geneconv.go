package geneconv

import (
	"database/sql"
	"strings"

	"github.com/antonybholmes/go-sys"
	"github.com/rs/zerolog/log"
)

// const MOUSE_TO_HUMAN_EXACT_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
// 	FROM mouse_terms, conversion, human
//  	WHERE LOWER(mouse_terms.term) = LOWER(?1) AND conversion.mouse_gene_id = mouse_terms.gene_id AND human.gene_id = conversion.human_gene_id`

// const HUMAN_TO_MOUSE_EXACT_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
// 	FROM human_terms, conversion, mouse
// 	WHERE LOWER(human_terms.term) = LOWER(?1) AND conversion.human_gene_id = human_terms.gene_id AND mouse.gene_id = conversion.mouse_gene_id`

// const MOUSE_TO_HUMAN_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
// 	FROM mouse_terms, conversion, human
//  	WHERE mouse_terms.term LIKE ?1 AND conversion.mouse_gene_id = mouse_terms.gene_id AND human.gene_id = conversion.human_gene_id`

// const HUMAN_TO_MOUSE_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
// 	FROM human_terms, conversion, mouse
// 	WHERE human_terms.term LIKE ?1 AND conversion.human_gene_id = human_terms.gene_id AND mouse.gene_id = conversion.mouse_gene_id`

// const MOUSE_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
// 	FROM mouse_terms, mouse
//  	WHERE mouse_terms.term LIKE ?1 AND mouse.gene_id = mouse_terms.gene_id`

// const HUMAN_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
// 	FROM human_terms, human
//  	WHERE human_terms.term LIKE ?1 AND human.gene_id = human_terms.gene_id`

// const MOUSE_EXACT_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
// 	FROM mouse_terms, mouse
// 	WHERE LOWER(mouse_terms.term) = LOWER(?1) AND mouse.gene_id = mouse_terms.gene_id`

// const HUMAN_EXACT_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
// 	FROM human_terms, human
// 	WHERE LOWER(human_terms.term) = LOWER(?1) AND human.gene_id = human_terms.gene_id`

const (
	HumanToMouseSql = `SELECT mouse.id, mouse.gene_symbol, mouse.entrez, mouse.ensembl, mouse.mouse_tags
 	FROM mouse
   	WHERE mouse.human_tags MATCH ?1 ORDER BY rank, mouse.gene_symbol`

	HumanToHumanSql = `SELECT human.id, human.gene_symbol, human.entrez, human.ensembl, human.human_tags
	FROM human
	WHERE human.human_tags MATCH ?1 ORDER BY rank, human.gene_symbol`

	MouseToHumanSql = `SELECT human.id, human.gene_symbol, human.entrez, human.ensembl, human.human_tags
	FROM human
	WHERE human.mouse_tags MATCH ?1 ORDER BY rank, human.gene_symbol`

	MouseToMouseSql = `SELECT mouse.id, mouse.gene_symbol, mouse.entrez, mouse.ensembl, mouse.mouse_tags
	FROM mouse
	WHERE mouse.mouse_tags MATCH ?1 ORDER BY rank, mouse.gene_symbol`

	TaxonomyHumanId = 9606
	TaxonomyMouseId = 10090

	SpeciesHuman = "human"
	SpeciesMouse = "mouse"
)

type Taxonomy struct {
	Species string `json:"species"`
	Id      uint   `json:"id"`
}

var (
	HUMAN_TAX = Taxonomy{
		Id:      TaxonomyHumanId,
		Species: SpeciesHuman,
	}

	MOUSE_TAX = Taxonomy{
		Id:      TaxonomyMouseId,
		Species: SpeciesMouse,
	}
)

// type BaseGene struct {
// 	Taxonomy Taxonomy `json:"taxonomy"`
// 	Id       string   `json:"id"`
// }

type Gene struct {
	Id      string   `json:"id"`
	Symbol  string   `json:"symbol"`
	Entrez  string   `json:"entrez"`
	Ensembl string   `json:"ensembl"`
	Aliases []string `json:"-"`
}

type Conversion struct {
	Search string  `json:"id"`
	Genes  []*Gene `json:"genes"`
}

type ConversionResults struct {
	From        Taxonomy  `json:"from"`
	To          Taxonomy  `json:"to"`
	Conversions [][]*Gene `json:"conversions"`
}

// type GeneResult struct {
// 	Id    string `json:"id"`
// 	Genes []Gene `json:"genes"`
// }

type GeneConvDB struct {
	db *sql.DB
}

func NewGeneConvDB(file string) *GeneConvDB {
	db := sys.Must(sql.Open("sqlite3", file))

	return &GeneConvDB{db}
}

func (geneconvdb *GeneConvDB) Close() {
	geneconvdb.db.Close()
}

func (geneconvdb *GeneConvDB) Convert(search string, fromSpecies string, toSpecies string, exact bool) ([]*Gene, error) {
	var sql string

	//var ret Conversion

	//ret.Search = search
	ret := make([]*Gene, 0, 5)

	fromSpecies = strings.ToLower(fromSpecies)
	toSpecies = strings.ToLower(toSpecies)

	if !exact {
		search = search + "*"
	}

	//var tax Taxonomy

	if fromSpecies == SpeciesHuman {
		if toSpecies == SpeciesMouse {
			sql = HumanToMouseSql
		} else {
			sql = HumanToHumanSql
		}

		//tax = MOUSE_TAX
	} else {
		if toSpecies == SpeciesHuman {
			sql = MouseToHumanSql
		} else {
			sql = MouseToMouseSql
		}

		//tax = HUMAN_TAX
	}

	// if !exact {
	// 	search = fmt.Sprintf("%%%s%%", search)
	// }

	//log.Debug().Msgf("%s %s", sql, search)

	rows, err := geneconvdb.db.Query(sql, search)

	if err != nil {
		log.Debug().Msgf("%s", err)
		return ret, err
	}

	genes, err := rowsToGenes(rows)

	if err != nil {
		return ret, err
	}

	ret = append(ret, genes...)

	return ret, nil
}

// func (geneconvdb *GeneConvDB) GeneInfo(search string, species string, exact bool) ([]*Gene, error) {

// 	var sql string

// 	species = strings.ToLower(species)

// 	var ret = make([]*Gene, 0, 5)

// 	var tax Taxonomy

// 	if species == MOUSE_SPECIES {
// 		if exact {
// 			sql = MOUSE_EXACT_SQL
// 		} else {
// 			sql = MOUSE_SQL
// 		}

// 		tax = MOUSE_TAX
// 	} else {
// 		if exact {
// 			sql = HUMAN_EXACT_SQL
// 		} else {
// 			sql = HUMAN_SQL
// 		}

// 		tax = HUMAN_TAX
// 	}

// 	if !exact {
// 		search = fmt.Sprintf("%%%s%%", search)
// 	}

// 	rows, err := geneconvdb.db.Query(sql, search)

// 	if err != nil {
// 		return ret, err
// 	}

// 	defer rows.Close()

// 	return rowsToGenes(rows, tax)
// }

func rowsToGenes(rows *sql.Rows) ([]*Gene, error) {
	defer rows.Close()

	//log.Debug().Msgf("row to gene")

	var aliases string
	//var entrez string
	//var refseq string
	//var ensembl string

	var ret = make([]*Gene, 0, 5)

	for rows.Next() {
		var gene Gene

		//gene.Taxonomy = tax

		err := rows.Scan(
			&gene.Id,
			&gene.Symbol,
			&gene.Entrez,
			&gene.Ensembl,
			&aliases)

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

			gene.Aliases = strings.Split(aliases, ",")
			//gene.RefSeq = strings.Split(refseq, "|")
			//gene.Ensembl = strings.Split(ensembl, "|")
		}

		ret = append(ret, &gene)
	}

	return ret, nil
}
