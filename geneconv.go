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
		GeneId          string   `json:"geneId"`
		Symbol          string   `json:"symbol"`
		Ensembl         string   `json:"ensembl"`
		PreviousSymbols []string `json:"previousSymbols,omitempty"`
		Entrez          int      `json:"entrez"`
	}

	Mapping struct {
		From *Gene `json:"from"`
		To   *Gene `json:"to"`
	}

	Conversion struct {
		Search   string     `json:"search"`
		Mappings []*Mapping `json:"mappings"`
	}

	ConversionResults struct {
		From        Taxonomy      `json:"from"`
		To          Taxonomy      `json:"to"`
		Conversions []*Conversion `json:"conversions"`
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
	SameSpeciesSql = `SELECT
		from_gene_id, 
		from_symbol, 
		from_entrez, 
		from_ensembl,
		from_previous_symbol,
		to_gene_id, 
		to_symbol, 
		to_entrez, 
		to_ensembl
	FROM (
		SELECT DISTINCT
			s1.gene_id AS from_gene_id, 
			s1.symbol AS from_symbol, 
			s1.entrez AS from_entrez, 
			s1.ensembl AS from_ensembl,
			a.name AS from_previous_symbol,
			s1.gene_id AS to_gene_id, 
			s1.symbol AS to_symbol, 
			s1.entrez AS to_entrez, 
			s1.ensembl AS to_ensembl,
			1 as src
		FROM <<S1>> s1
		JOIN <<S1>>_previous_symbols s1p ON s1p.<<S1>>_id = s1.id
		JOIN aliases a ON a.id = s1p.alias_id
		WHERE s1.gene_id = :search OR 
			s1.symbol LIKE :q OR 
			s1.entrez = :search OR 
			s1.ensembl = :search OR
			'|' || s1.refseq || '|' LIKE :refseq
		UNION ALL
		SELECT DISTINCT
			s1.gene_id AS from_gene_id, 
			s1.symbol AS from_symbol, 
			s1.entrez AS from_entrez, 
			s1.ensembl AS from_ensembl,
			a.name AS from_previous_symbol,
			s1.gene_id AS to_gene_id, 
			s1.symbol AS to_symbol, 
			s1.entrez AS to_entrez, 
			s1.ensembl AS to_ensembl,
			2 as src
		FROM <<S1>> s1
		JOIN <<S1>>_previous_symbols s1p ON s1p.<<S1>>_id = s1.id
		JOIN aliases a ON a.id = s1p.alias_id
		WHERE a.name LIKE :q
	)
	ORDER BY src, from_symbol, from_previous_symbol, to_symbol`

	DifferentSpeciesSql = `SELECT
		from_gene_id, 
		from_symbol, 
		from_entrez, 
		from_ensembl,
		from_previous_symbol,
		to_gene_id, 
		to_symbol, 
		to_entrez, 
		to_ensembl
	FROM (
		SELECT DISTINCT
			s1.gene_id AS from_gene_id, 
			s1.symbol AS from_symbol, 
			s1.entrez AS from_entrez, 
			s1.ensembl AS from_ensembl,
			a.name AS from_previous_symbol,
			s2.gene_id AS to_gene_id, 
			s2.symbol AS to_symbol, 
			s2.entrez AS to_entrez, 
			s2.ensembl AS to_ensembl,
			1 as src
		FROM <<S2>> s2
		JOIN human_mouse hm ON s2.id = hm.<<S2>>_id
		JOIN <<S1>> s1 ON s1.id = hm.<<S1>>_id
		JOIN <<S1>>_previous_symbols s1p ON s1p.<<S1>>_id = s1.id
		JOIN aliases a ON a.id = s1p.alias_id
		WHERE s1.gene_id = :search OR 
			s1.symbol LIKE :q OR 
			s1.entrez = :search OR 
			s1.ensembl = :search OR
			'|' || s1.refseq || '|' LIKE :refseq
		UNION ALL
		SELECT DISTINCT
			s1.gene_id AS from_gene_id, 
			s1.symbol AS from_symbol, 
			s1.entrez AS from_entrez, 
			s1.ensembl AS from_ensembl,
			a.name AS from_previous_symbol,
			s2.gene_id AS to_gene_id, 
			s2.symbol AS to_symbol, 
			s2.entrez AS to_entrez, 
			s2.ensembl AS to_ensembl,
			2 as src
		FROM <<S2>> s2
		JOIN human_mouse hm ON s2.id = hm.<<S2>>_id
		JOIN <<S1>> s1 ON s1.id = hm.<<S1>>_id
		JOIN <<S1>>_previous_symbols s1p ON s1p.<<S1>>_id = s1.id
		JOIN aliases a ON a.id = s1p.alias_id
		WHERE a.name LIKE :q
	)
	ORDER BY src, from_symbol, from_previous_symbol, to_symbol`

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

func (geneconvdb *GeneConvDB) Convert(search string, fromSpecies string, toSpecies string, exact bool) (*Conversion, error) {

	//var ret Conversion

	//ret.Search = search

	var ret Conversion = Conversion{Search: search, Mappings: make([]*Mapping, 0, 5)}

	fromSpecies = strings.ToLower(fromSpecies)
	toSpecies = strings.ToLower(toSpecies)

	q := search

	if !exact {
		q += "*"
	}

	//var tax Taxonomy

	var query string

	if fromSpecies != toSpecies {
		query = DifferentSpeciesSql

	} else {
		query = SameSpeciesSql
	}

	query = strings.ReplaceAll(query, "<<S1>>", fromSpecies)
	query = strings.ReplaceAll(query, "<<S2>>", toSpecies)

	// if fromSpecies == SpeciesHuman {
	// 	if toSpecies == SpeciesMouse {
	// 		query = HumanToMouseSql
	// 	} else {
	// 		query = HumanToHumanSql
	// 	}

	// 	//tax = MOUSE_TAX
	// } else {
	// 	if toSpecies == SpeciesHuman {
	// 		query = MouseToHumanSql
	// 	} else {
	// 		query = MouseToMouseSql
	// 	}

	// 	//tax = HUMAN_TAX
	// }

	// if !exact {
	// 	search = fmt.Sprintf("%%%s%%", search)
	// }

	log.Debug().Msgf("%s %s", query, search)

	// refseq search for exact match of id between pipe chars
	rows, err := geneconvdb.db.Query(query, sql.Named("search", search), sql.Named("q", q), sql.Named("refseq", "|"+q+"|"))

	if err != nil {
		log.Debug().Msgf("%s", err)
		return nil, err
	}

	genes, err := rowsToMappings(rows, 1)

	if err != nil {
		return nil, err
	}

	ret.Mappings = append(ret.Mappings, genes...)

	return &ret, nil
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

func rowsToMappings(rows *sql.Rows, records int) ([]*Mapping, error) {
	defer rows.Close()

	//log.Debug().Msgf("row to gene")

	var alias string
	//var entrez string
	//var refseq string
	//var ensembl string

	var ret = make([]*Mapping, 0, 5)

	var currentFromGene *Gene = nil

	for rows.Next() {
		var fromGene Gene
		var toGene Gene

		//gene.Taxonomy = tax

		err := rows.Scan(
			&fromGene.GeneId,
			&fromGene.Symbol,
			&fromGene.Entrez,
			&fromGene.Ensembl,
			&alias,
			&toGene.GeneId,
			&toGene.Symbol,
			&toGene.Entrez,
			&toGene.Ensembl,
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

		if currentFromGene == nil || fromGene.GeneId != currentFromGene.GeneId {
			// once we have found 1 gene, stop the search
			if len(ret) == records {
				break
			}

			currentFromGene = &fromGene
			ret = append(ret, &Mapping{From: currentFromGene, To: &toGene})
		}

		currentFromGene.PreviousSymbols = append(currentFromGene.PreviousSymbols, alias)

	}

	return ret, nil
}
