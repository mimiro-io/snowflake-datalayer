package layer

import (
	"fmt"
	"testing"

	common "github.com/mimiro-io/common-datalayer"
)

func TestConfig(t *testing.T) {
	var subject common.DataLayerService
	setup := func() {
		conf, metrics, logger := testDeps()
		var err error
		subject, err = NewSnowflakeDataLayer(conf, logger, metrics)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Run("should ignore empty updates", func(t *testing.T) {
		setup()
		err := subject.UpdateConfiguration(&common.Config{})
		if err != nil {
			t.Errorf("empty config should be ignored: %v", err)
		}
	})
	t.Run("should add dataset definitions", func(t *testing.T) {
		if subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test"}},
		}) != nil {
			t.Fatal("failed to add dataset definition")
		}
		ds, err := subject.Dataset("test")
		if err != nil {
			t.Fatal(err)
		}
		if ds == nil {
			t.Fatal("dataset is nil")
		}
		if len(ds.MetaData()) != 0 {
			t.Fatal("empty here means non implicit")
		}
	})
	t.Run("should update dataset definitions", func(t *testing.T) {
		if subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test"}},
		}) != nil {
			t.Fatal("failed to add dataset definition")
		}
		ds, err := subject.Dataset("test")
		if err != nil {
			t.Fatal(err)
		}
		if ds == nil {
			t.Fatal("dataset is nil")
		}
		if len(ds.MetaData()) != 0 {
			t.Fatal("empty here means non implicit")
		}

		if subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test", SourceConfig: map[string]any{"test": "test"}}},
		}) != nil {
			t.Fatal("failed to update dataset definition")
		}
		ds, err = subject.Dataset("test")
		if err != nil {
			t.Fatal(err)

		}
		if ds == nil {
			t.Fatal("dataset is nil")
		}
		if ds.MetaData()["test"] != "test" {
			t.Fatal("source config not updated")
		}
	})
	t.Run("should remove dataset definitions", func(t *testing.T) {
		if subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test"}},
		}) != nil {
			t.Fatal("failed to add dataset definition")
		}
		ds, err := subject.Dataset("test")
		if err != nil {
			t.Fatal(err)
		}
		if ds == nil {
			t.Fatal("dataset is nil")
		}
		if len(ds.MetaData()) != 0 {
			t.Fatal("empty here means non implicit")
		}

		if subject.UpdateConfiguration(&common.Config{}) != nil {
			t.Fatal("failed to remove dataset definition")
		}
		ds, err = subject.Dataset("test")
		if err != nil {
			t.Fatal(err)
		}
		if ds == nil {
			t.Fatal("dataset is nil")
		}
		if len(ds.MetaData()) == 0 {
			t.Fatal("implicit config expected")
		}
		if ds.MetaData()["raw_column"] != "ENTITY" {
			t.Fatal("implicit config expected")
		}
	})
	t.Run("should fail on missing layer_config", func(t *testing.T) {
		conf, metrics, logger := testDeps()
		conf.LayerServiceConfig = nil
		_, err := NewSnowflakeDataLayer(conf, logger, metrics)
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "missing required layer_config block" {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("should fail on missing system_config", func(t *testing.T) {
		conf, metrics, logger := testDeps()
		conf.NativeSystemConfig = nil
		_, err := NewSnowflakeDataLayer(conf, logger, metrics)
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "missing required system_config block" {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("should fail on missing required config", func(t *testing.T) {
		conf, metrics, logger := testDeps()
		// remove required config param
		delete(conf.NativeSystemConfig, SnowflakeDB)
		_, err := NewSnowflakeDataLayer(conf, logger, metrics)
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "missing required config value snowflake_db" {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("with EnvOverrides", func(t *testing.T) {
		t.Setenv("SNOWFLAKE_DB", "overridden_test")
		t.Run("should override config with env vars", func(t *testing.T) {
			conf, metrics, logger := testDeps()
			EnvOverrides(conf)
			fmt.Println(conf)
			subject, err := NewSnowflakeDataLayer(conf, logger, metrics)
			if err != nil {
				t.Fatal(err)
			}
			if subject == nil {
				t.Fatal("subject is nil")
			}

			ds, err := subject.Dataset("implicit_test")
			if err != nil {
				t.Fatal(err)

			}
			if ds == nil {
				t.Fatal("dataset is nil")

			}
			if ds.MetaData()["database"] != "overridden_test" {
				t.Fatal("source config not updated")
			}
		})
	})
}
