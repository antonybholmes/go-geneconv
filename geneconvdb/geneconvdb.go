package geneconvdb

import (
	"sync"

	geneconv "github.com/antonybholmes/go-geneconv"
)

var instance *geneconv.GeneConvDB
var once sync.Once

func InitGeneConvDB(file string) *geneconv.GeneConvDB {
	once.Do(func() {
		instance = geneconv.NewGeneConvDB(file)
	})

	return instance
}

func Instance() *geneconv.GeneConvDB {
	return instance
}
