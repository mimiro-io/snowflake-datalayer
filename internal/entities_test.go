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
	"github.com/DATA-DOG/go-sqlmock"
	common "github.com/mimiro-io/common-datalayer"
	"testing"
)

func TestDataset_Entities(t *testing.T) {
	var subject *Dataset
	var cnt int
	var tDB *testDB
	setup := func() {
		conf, metrics, logger := testDeps()
		_tDB, err := newTestDB(cnt, conf, logger, metrics)
		if err != nil {
			t.Fatal(err)
		}
		tDB = _tDB

		cnt++
		dd := &common.DatasetDefinition{SourceConfig: map[string]any{
			"database":   "testdb",
			"schema":     "testschema",
			"table_name": "testtable",
		}}
		subject = &Dataset{
			logger:            logger,
			db:                tDB,
			datasetDefinition: dd,
			sourceConfig:      dd.SourceConfig,
			name:              "testds",
		}
	}

	t.Run("should apply correct params to entities query", func(t *testing.T) {
		setup()
		tDB.mock.ExpectQuery("SELECT \\* FROM testdb.testschema.testtable").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test"))
		result, err := subject.Entities("", 0)
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("result is nil")
		}
		if result.(*testIter).sinceColumn != "" {
			t.Fatal("since column should be empty")
		}
		if result.(*testIter).sinceToken != "" {
			t.Fatal("since token should be empty")
		}
		if result.(*testIter).limit != 0 {
			t.Fatal("limit should be 0")
		}

		// since token is ignored if no since column is set
		subject.db.(*testDB).ExpectConn()
		tDB.mock.ExpectQuery("SELECT \\* FROM testdb.testschema.testtable").
			WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test"))
		since := "SGVpCg=="
		result, err = subject.Entities(since, 7)
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("result is nil")
		}
		if result.(*testIter).sinceColumn != "" {
			t.Fatal("since column should be empty")
		}
		if result.(*testIter).sinceToken != "" {
			t.Fatal("since token should be empty")
		}
		if result.(*testIter).limit != 7 {
			t.Fatal("limit should be 7")
		}

		// since token is used if since column is set
		subject.db.(*testDB).ExpectConn()
		tDB.mock.ExpectQuery("SELECT MAX\\(test_col\\)").WillReturnRows(sqlmock.NewRows(nil))
		tDB.mock.ExpectQuery("SELECT \\* FROM testdb.testschema.testtable").WillReturnRows(sqlmock.NewRows(nil))
		subject.datasetDefinition.SourceConfig[SinceColumn] = "test_col"
		result, err = subject.Entities(since, 7)
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("result is nil")
		}
		if result.(*testIter).sinceColumn != "test_col" {
			t.Fatal("since column should be test_col")
		}
		if result.(*testIter).sinceToken != "SGVpCg==" {
			t.Fatal("since token should be	SGVpCg==")
		}
		if result.(*testIter).limit != 7 {
			t.Fatal("limit should be 7")
		}
	})
}
