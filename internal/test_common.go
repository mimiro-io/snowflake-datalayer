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
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

// TODO: provide mocks in common-datalayer?
type (
	testMetrics struct {
		metrics map[string]any
	}
	testLogger struct {
		logs []string
	}
	testDB struct {
		db         *sql.DB
		mock       sqlmock.Sqlmock
		NewTmpFile func(ds string) (*os.File, func(), error)
		sfDB       *SfDB
	}
	testQuery struct {
		sinceColumn string
		sinceToken  string
		limit       int
		sfQ         *sfQuery
	}
	testIter struct {
		sinceColumn string
		sinceToken  string
		limit       int
		sfIter      *entIter
	}
)

func (tdb *testDB) close() error {
	return tdb.db.Close()
}

func (t testIter) Context() *egdm.Context {
	return t.sfIter.Context()
}

func (t testIter) Next() (*egdm.Entity, common.LayerError) {
	return t.sfIter.Next()
}

func (t testIter) Token() (*egdm.Continuation, common.LayerError) {
	return t.sfIter.Token()
}

func (t testIter) Close() common.LayerError {
	return t.sfIter.Close()
}

// run implements query.
func (q *testQuery) run(ctx context.Context, releaseConn func()) (common.EntityIterator, common.LayerError) {
	it, err := q.sfQ.run(ctx, releaseConn)
	if err != nil {
		return nil, err
	}
	return &testIter{
		limit:       q.limit,
		sinceColumn: q.sinceColumn,
		sinceToken:  q.sinceToken,
		sfIter:      it.(*entIter),
	}, err
}

// withLimit implements query.
func (q *testQuery) withLimit(limit int) (query, error) {
	q.limit = limit
	return q.sfQ.withLimit(limit)
}

// withSince implements query.
func (q *testQuery) withSince(sinceColumn string, sinceToken string) (query, error) {
	q.sinceColumn = sinceColumn
	q.sinceToken = sinceToken
	return q.sfQ.withSince(sinceColumn, sinceToken)
}

func newTestDB(cnt int, conf *common.Config, logger common.Logger, metrics common.Metrics) (*testDB, error) {
	dbNew, mock, err := sqlmock.NewWithDSN("M_DB:@host2:443?database=TESTDB&schema=TESTSCHEMA&rnd=" + fmt.Sprint(cnt))
	if err != nil {
		return nil, err
	}
	mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON';").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("USE SECONDARY ROLES ALL;").WillReturnResult(sqlmock.NewResult(1, 1))
	sfDB, err := newSfDB(conf, logger, metrics)
	if err != nil {
		return nil, err
	}
	sfDB.db = dbNew
	return &testDB{db: dbNew, mock: mock, sfDB: sfDB}, err
}

func (tdb *testDB) ExpectConn() {
	tdb.mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON';").WillReturnResult(sqlmock.NewResult(1, 1))
	tdb.mock.ExpectExec("USE SECONDARY ROLES ALL;").WillReturnResult(sqlmock.NewResult(1, 1))
}

// createQuery implements db.
func (tdb *testDB) createQuery(ctx context.Context, datasetDefinition *common.DatasetDefinition) (query, error) {
	q, err := tdb.sfDB.createQuery(ctx, datasetDefinition)
	return &testQuery{
		sfQ: q.(*sfQuery),
	}, err
}

// getFsStage implements db.
func (tdb *testDB) getFsStage(syncId string, datasetDefinition *common.DatasetDefinition) string {
	return tdb.sfDB.getFsStage(syncId, datasetDefinition)
}

// loadFilesInStage implements db.
func (tdb *testDB) loadFilesInStage(ctx context.Context, files []string, stage string, loadTime int64, datasetDefinition *common.DatasetDefinition) error {
	return tdb.sfDB.loadFilesInStage(ctx, files, stage, loadTime, datasetDefinition)
}

// loadStage implements db.
func (tdb *testDB) loadStage(ctx context.Context, stage string, loadTime int64, datasetDefinition *common.DatasetDefinition) error {
	return tdb.sfDB.loadStage(ctx, stage, loadTime, datasetDefinition)
}

// mkStage implements db.
func (tdb *testDB) mkStage(ctx context.Context, syncID string, datasetName string, datasetDefinition *common.DatasetDefinition) (string, error) {
	return tdb.sfDB.mkStage(ctx, syncID, datasetName, datasetDefinition)
}

// newConnection implements db.
func (tdb *testDB) newConnection(ctx context.Context) (*sql.Conn, error) {
	return tdb.db.Conn(ctx)
}

// putEntities implements db.
func (tdb *testDB) putEntities(ctx context.Context, datasetName string, stage string, entities []*egdm.Entity) ([]string, error) {
	tdb.sfDB.NewTmpFile = tdb.NewTmpFile
	return tdb.sfDB.putEntities(ctx, datasetName, stage, entities)
}

var _ db = &testDB{} // interface assertion

func (l *testLogger) log(message string, level string, args ...any) {
	msg := fmt.Sprint(append([]any{message, level}, args...))
	l.logs = append(l.logs, msg)
}

// Debug implements common_datalayer.Logger.
func (l *testLogger) Debug(message string, args ...any) {
	l.log(message, "DEBUG", args...)
}

// Error implements common_datalayer.Logger.
func (l *testLogger) Error(message string, args ...any) {
	l.log(message, "ERROR", args...)
}

// Info implements common_datalayer.Logger.
func (l *testLogger) Info(message string, args ...any) {
	l.log(message, "INFO", args...)
}

// Warn implements common_datalayer.Logger.
func (l *testLogger) Warn(message string, args ...any) {
	l.log(message, "WARN", args...)
}

// With implements common_datalayer.Logger.
func (l *testLogger) With(name string, value string) common.Logger {
	// ignore
	return l
}

func (*testMetrics) Gauge(s string, f float64, tags []string, i int) common.LayerError {
	panic("unimplemented")
}
func (*testMetrics) Incr(s string, tags []string, i int) common.LayerError { panic("unimplemented") }
func (*testMetrics) Timing(s string, timed time.Duration, tags []string, i int) common.LayerError {
	panic("unimplemented")
}

func testDeps() (*common.Config, common.Metrics, common.Logger) {
	// minimal valid config
	conf := &common.Config{
		NativeSystemConfig: map[string]any{
			SnowflakeDB:        "testdb",
			SnowflakeSchema:    "testschema",
			SnowflakeAccount:   "testaccount",
			SnowflakeUser:      "testuser",
			SnowflakeWarehouse: "testwh",
			SnowflakePrivateKey: `MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAkEAxIXbFdo7AhvdobX4
F+gjkgGD3wM2zH6GhvJSnCLmKvlYPGwwX9J+xgEBPLSEH4R4zW/YFySOYxGU/Dbo
ZIpXfwIDAQABAkBKOch643cgH8hBGMrAtNQihGH7bGpZKHzFIWdkQ6YtmmBu/O5F
tBNJQgsFsWnOydURrJzGoG1ezMQArNBdFUUJAiEA40p9KnnaA/NWb608yolfArKH
cQJ+iXx1d2HkeVMbCSUCIQDdWHj+0VWZ00iNh5plqFov8EKNAMImYEi/1geBHcQ2
0wIgeaNGovG9NDoI+xEqJHYp66ahh2A/WdLKho5UGH3aTSUCIBqeDgbOk5Wo87uZ
R/bblOTY5pfgNHi68WSoT0S2mKbjAiBnG28oMs8D+vGKZMawf2BKbq33MjRsMJmc
jmMHJqy7ow==`,
		},
		LayerServiceConfig: &common.LayerServiceConfig{
			ServiceName: "test",
			Port:        "17866",
			//LogFormat:   "text",
		},
		DatasetDefinitions: []*common.DatasetDefinition{},
	}
	return conf, &testMetrics{}, &testLogger{}
}
