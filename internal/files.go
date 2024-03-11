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
	"encoding/json"
	"io"
	"os"

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
	j := newWriter(zipWriter)
	for _, entity := range entities {
		err := j.Add(entity)
		if err != nil {
			return err
		}
	}

	// flush and close
	return zipWriter.Close()
}

type Writer struct {
	enc *json.Encoder
}

// NewWriter returns a new Writer, which writes JSON-encoded data
// in the given io.Writer implementation.
func newWriter(w io.Writer) Writer {
	return Writer{enc: json.NewEncoder(w)}
}

// Add encodes the given value and write it as a JSON object.
func (jw Writer) Add(v interface{}) error {
	return jw.enc.Encode(v)
}

// AddAll is equivalent to calling Add on each of its arguments
func (jw Writer) AddAll(args ...interface{}) (err error) {
	for _, v := range args {
		if err = jw.Add(v); err != nil {
			return
		}
	}
	return
}
