package genecondb

import (
	"sync"

	genecon "github.com/antonybholmes/go-gene-conversion"
)

var instance *genecon.GeneConDB
var once sync.Once

func InitCache(dir string) *genecon.GeneConDB {
	once.Do(func() {
		instance = genecon.NewGeneConDB(dir)
	})

	return instance
}

func GetInstance() *genecon.GeneConDB {
	return instance
}

func Convert(name string, species string) (*genecon.Conversion, error) {
	return instance.Convert(name, species)
}

func Gene(name string, species string) (*genecon.Gene, error) {
	return instance.Gene(name, species)
}
