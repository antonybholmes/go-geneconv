package geneconvdb

import (
	"sync"

	geneconv "github.com/antonybholmes/go-gene-conversion"
)

var instance *geneconv.GeneConvDB
var once sync.Once

func InitCache(file string) *geneconv.GeneConvDB {
	once.Do(func() {
		instance = geneconv.NewGeneConvDB(file)
	})

	return instance
}

func GetInstance() *geneconv.GeneConvDB {
	return instance
}

func Convert(search string, fromSpecies string, toSpecies string, exact bool) ([]*geneconv.Gene, error) {
	return instance.Convert(search, fromSpecies, toSpecies, exact)
}

// func GeneInfo(search string, species string, exact bool) ([]*geneconv.Gene, error) {
// 	return instance.GeneInfo(search, species, exact)
// }
