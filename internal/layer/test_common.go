package layer

import (
	"fmt"
	"time"

	common "github.com/mimiro-io/common-datalayer"
)

// TODO: provide mocks in common-datalayer?
type (
	testMetrics struct {
		metrics map[string]any
	}
	testLogger struct {
		logs []string
	}
)

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
