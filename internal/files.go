// Copyright 2024 MIMIRO AS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package layer

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

func WriteAsGzippedNDJson(file io.Writer, entities []*egdm.Entity, _ string) error {
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
