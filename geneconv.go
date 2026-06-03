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

type (
	Taxonomy struct {
		Species string `json:"species"`
		Id      int    `json:"id"`
	}

	// type BaseGene struct {
	// 	Taxonomy Taxonomy `json:"taxonomy"`
	// 	Id       string   `json:"id"`
	// }

	Gene struct {
		GeneId  string   `json:"geneId"`
		Symbol  string   `json:"symbol"`
		Ensembl string   `json:"ensembl"`
		Aliases []string `json:"-"`
		Entrez  int      `json:"entrez"`
	}

	// Conversion struct {
	// 	Search string  `json:"id"`
	// 	Genes  []*Gene `json:"genes"`
	// }

	ConversionResults struct {
		From        Taxonomy  `json:"from"`
		To          Taxonomy  `json:"to"`
		Conversions [][]*Gene `json:"conversions"`
	}

	//  GeneResult struct {
	// 	Id    string `json:"id"`
	// 	Genes []Gene `json:"genes"`
	// }

	GeneConvDB struct {
		db *sql.DB
	}
)

const (
	HumanToMouseSql = `SELECT DISTINCT
		m.gene_id, 
		m.symbol, 
		m.entrez, 
		m.ensembl,
		a.name AS alias
		FROM mouse m
		JOIN human_mouse hm ON m.id = hm.mouse_id
		JOIN human h ON h.id = hm.human_id
		JOIN human_aliases ha ON ha.human_id = h.id
		JOIN aliases a ON a.id = ha.alias_id
		WHERE a.name LIKE :q
		ORDER BY m.symbol, a.name`

	HumanToHumanSql = `SELECT DISTINCT
		h.gene_id, 
		h.symbol, 
		h.entrez, 
		h.ensembl,
		a.name AS alias
		FROM human h
		JOIN human_aliases ha ON ha.human_id = h.id
		JOIN aliases a ON a.id = ha.alias_id
		WHERE a.name LIKE :q
		ORDER BY h.symbol, a.name`

	MouseToHumanSql = `SELECT DISTINCT
		h.gene_id, 
		h.symbol, 
		h.entrez, 
		h.ensembl,
		a.name AS alias
		FROM human h
		JOIN human_mouse hm ON h.id = hm.human_id
		JOIN mouse m ON m.id = hm.mouse_id
		JOIN mouse_aliases ma ON ma.mouse_id = m.id
		JOIN aliases a ON a.id = ma.alias_id
		WHERE h.gene_id = :search OR h.symbol LIKE :q OR h.entrez = :search OR h.ensembl = :search
		ORDER BY h.symbol, a.name`

	MouseToHumanAliasSql = `SELECT DISTINCT
		h.gene_id, 
		h.symbol, 
		h.entrez, 
		h.ensembl,
		a.name AS alias
		FROM human h
		JOIN human_mouse hm ON h.id = hm.human_id
		JOIN mouse m ON m.id = hm.mouse_id
		JOIN mouse_aliases ma ON ma.mouse_id = m.id
		JOIN aliases a ON a.id = ma.alias_id
		WHERE a.name LIKE :q
		ORDER BY h.symbol, a.name`

	MouseToMouseSql = `SELECT DISTINCT
		m.id, 
		m.symbol, 
		m.entrez, 
		m.ensembl,
		a.name AS alias
		FROM mouse m
		JOIN mouse_aliases ma ON ma.mouse_id = m.id
		JOIN aliases a ON a.id = ma.alias_id
		WHERE a.name LIKE :q
		ORDER BY m.symbol, a.name`

	TaxonomyHumanId = 9606
	TaxonomyMouseId = 10090

	MaxSearches = 1000

	SpeciesHuman = "human"
	SpeciesMouse = "mouse"
)

var (
	HumanTaxo = Taxonomy{
		Id:      TaxonomyHumanId,
		Species: SpeciesHuman,
	}

	MouseTaxo = Taxonomy{
		Id:      TaxonomyMouseId,
		Species: SpeciesMouse,
	}
)

func NewGeneConvDB(file string) *GeneConvDB {

	return &GeneConvDB{db: sys.Must(sql.Open("sqlite3", file))}
}

func (geneconvdb *GeneConvDB) Close() error {
	return geneconvdb.db.Close()
}

func (geneconvdb *GeneConvDB) Convert(search string, fromSpecies string, toSpecies string, exact bool) ([]*Gene, error) {
	var query string

	//var ret Conversion

	//ret.Search = search
	ret := make([]*Gene, 0, 5)

	fromSpecies = strings.ToLower(fromSpecies)
	toSpecies = strings.ToLower(toSpecies)

	q := search

	if !exact {
		q += "*"
	}

	//var tax Taxonomy

	if fromSpecies == SpeciesHuman {
		if toSpecies == SpeciesMouse {
			query = HumanToMouseSql
		} else {
			query = HumanToHumanSql
		}

		//tax = MOUSE_TAX
	} else {
		if toSpecies == SpeciesHuman {
			query = MouseToHumanAliasSql
		} else {
			query = MouseToMouseSql
		}

		//tax = HUMAN_TAX
	}

	// if !exact {
	// 	search = fmt.Sprintf("%%%s%%", search)
	// }

	log.Debug().Msgf("%s %s", query, search)

	rows, err := geneconvdb.db.Query(query, sql.Named("search", search), sql.Named("q", q))

	if err != nil {
		log.Debug().Msgf("%s", err)
		return ret, err
	}

	genes, err := rowsToGenes(rows, 1)

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

func rowsToGenes(rows *sql.Rows, records int) ([]*Gene, error) {
	defer rows.Close()

	//log.Debug().Msgf("row to gene")

	var alias string
	//var entrez string
	//var refseq string
	//var ensembl string

	var ret = make([]*Gene, 0, 5)

	var currentGene *Gene = nil

	for rows.Next() {
		var gene Gene

		//gene.Taxonomy = tax

		err := rows.Scan(
			&gene.GeneId,
			&gene.Symbol,
			&gene.Entrez,
			&gene.Ensembl,
			&alias,
		)

		// keep going even if there is a failure
		if err != nil {
			log.Debug().Msgf("err %s", err)

			// convert entrez to numbers
			// for _, e := range strings.Split(entrez, ",") {
			// 	n, err := strconv.Parseint(e, 10, 64)

			// 	if err == nil {
			// 		gene.Entrez = append(gene.Entrez, n)
			// 	}
			// }

			//gene.Aliases = strings.Split(aliases, "|")
			//gene.RefSeq = strings.Split(refseq, "|")
			//gene.Ensembl = strings.Split(ensembl, "|")
			continue
		}

		if currentGene == nil || gene.GeneId != currentGene.GeneId {
			// once we have found 1 gene, stop the search
			if len(ret) == records {
				break
			}

			currentGene = &gene
			ret = append(ret, currentGene)
		}

		currentGene.Aliases = append(currentGene.Aliases, alias)

	}

	return ret, nil
}
