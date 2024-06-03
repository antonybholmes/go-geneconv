package geneconvdb

import (
	"sync"

	geneconv "github.com/antonybholmes/go-gene-conversion"
)

var instance *geneconv.GeneConDB
var once sync.Once

func InitCache(dir string) *geneconv.GeneConDB {
	once.Do(func() {
		instance = geneconv.NewGeneConDB(dir)
	})

	return instance
}

func GetInstance() *geneconv.GeneConDB {
	return instance
}

func Convert(name string, species string) (*geneconv.Conversion, error) {
	return instance.Convert(name, species)
}

func Gene(name string, species string) (*geneconv.Gene, error) {
	return instance.Gene(name, species)
}
