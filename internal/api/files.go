package api

import (
	"compress/gzip"
	"io"
	"os"

	"github.com/bfontaine/jsons"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

func NewTmpFileWriter(dataset string) (*os.File, func(), error) {
	file, err := os.CreateTemp("", dataset)
	if err != nil {
		return nil, nil, err
	}
	finally := func() { os.Remove(file.Name()) }
	return file, finally, nil
}

func WriteAsGzippedNDJson(file io.Writer, entities []*egdm.Entity, dataset string) error {
	zipWriter := gzip.NewWriter(file)
	j := jsons.NewWriter(zipWriter)
	for _, entity := range entities {
		err := j.Add(entity)
		if err != nil {
			return err
		}
	}

	// flush and close
	return zipWriter.Close()
}
