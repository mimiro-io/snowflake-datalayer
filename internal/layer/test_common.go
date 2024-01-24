package layer

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
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
		db   *sql.DB
		mock sqlmock.Sqlmock
	}
	testQuery struct {
		limit       int
		sinceColumn string
		sinceToken  string
	}
	testIter struct {
		sinceColumn string
		sinceToken  string
		entIter
		limit int
	}
)

// run implements query.
func (q *testQuery) run(ctx context.Context, releaseConn func()) (common.EntityIterator, common.LayerError) {
	return &testIter{
		limit:       q.limit,
		sinceColumn: q.sinceColumn,
		sinceToken:  q.sinceToken,
	}, nil
}

// withLimit implements query.
func (q *testQuery) withLimit(limit int) (query, error) {
	q.limit = limit
	return q, nil
}

// withSince implements query.
func (q *testQuery) withSince(sinceColumn string, sinceToken string) (query, error) {
	q.sinceColumn = sinceColumn
	q.sinceToken = sinceToken
	return q, nil
}

func newTestDB(cnt int) *testDB {
	ginkgo.GinkgoHelper()
	db, mock, err := sqlmock.NewWithDSN("M_DB:@host2:443?database=TESTDB&schema=TESTSCHEMA&rnd=" + fmt.Sprint(cnt))
	mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON';").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("USE SECONDARY ROLES ALL;").WillReturnResult(sqlmock.NewResult(1, 1))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return &testDB{db: db, mock: mock}
}

func (tdb *testDB) ExpectConn() {
	tdb.mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON';").WillReturnResult(sqlmock.NewResult(1, 1))
	tdb.mock.ExpectExec("USE SECONDARY ROLES ALL;").WillReturnResult(sqlmock.NewResult(1, 1))
}

// createQuery implements db.
func (*testDB) createQuery(ctx context.Context, datasetDefinition *common.DatasetDefinition) (query, error) {
	return &testQuery{}, nil
}

// getFsStage implements db.
func (*testDB) getFsStage(syncId string, datasetDefinition *common.DatasetDefinition) string {
	panic("unimplemented")
}

// loadFilesInStage implements db.
func (*testDB) loadFilesInStage(ctx context.Context, files []string, stage string, loadTime int64, datasetDefinition *common.DatasetDefinition) error {
	panic("unimplemented")
}

// loadStage implements db.
func (*testDB) loadStage(ctx context.Context, stage string, loadTime int64, datasetDefinition *common.DatasetDefinition) error {
	panic("unimplemented")
}

// mkStage implements db.
func (*testDB) mkStage(ctx context.Context, syncID string, datasetName string, datasetDefinition *common.DatasetDefinition) (string, error) {
	panic("unimplemented")
}

// newConnection implements db.
func (tdb *testDB) newConnection(ctx context.Context) (*sql.Conn, error) {
	return tdb.db.Conn(ctx)
}

// putEntities implements db.
func (*testDB) putEntities(ctx context.Context, datasetName string, stage string, entities []*egdm.Entity) ([]string, error) {
	panic("unimplemented")
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

// Gauge implements common_datalayer.Metrics.
func (*testMetrics) Gauge(s string, f float64, tags []string, i int) common.LayerError {
	panic("unimplemented")
}

// Incr implements common_datalayer.Metrics.
func (*testMetrics) Incr(s string, tags []string, i int) common.LayerError {
	panic("unimplemented")
}

// Timing implements common_datalayer.Metrics.
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
			Port:        "5555",
		},
		DatasetDefinitions: []*common.DatasetDefinition{},
	}
	return conf, &testMetrics{}, &testLogger{}
}
