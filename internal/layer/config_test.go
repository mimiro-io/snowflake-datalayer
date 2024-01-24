package layer

import (
	"os"
	"testing"

	common "github.com/mimiro-io/common-datalayer"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLayer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Snowflake Layer Test Suite")
}

var _ = Describe("Config", Serial, func() {
	var subject common.DataLayerService
	JustBeforeEach(func() {
		conf, metrics, logger := testDeps()
		var err error
		subject, err = NewSnowflakeDataLayer(conf, logger, metrics)
		Expect(err).To(BeNil())
	})
	It("should ignore empty updates", func() {
		err := subject.UpdateConfiguration(&common.Config{})
		Expect(err).To(BeNil(), "empty config should be ignored")
	})
	It("should add dataset definitions", func() {
		Expect(subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test"}},
		})).To(BeNil())
		ds, err := subject.Dataset("test")
		Expect(err).To(BeNil())
		Expect(ds).NotTo(BeNil())
		Expect(ds.MetaData()).To(BeEmpty(), "empty here means non implicit")
	})
	It("should update dataset definitions", func() {
		Expect(subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test"}},
		})).To(BeNil())
		ds, err := subject.Dataset("test")
		Expect(err).To(BeNil())
		Expect(ds).NotTo(BeNil())
		Expect(ds.MetaData()).To(BeEmpty())
		Expect(ds.MetaData()).To(BeEmpty(), "empty here means non implicit")

		Expect(subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test", SourceConfig: map[string]any{"test": "test"}}},
		})).To(BeNil())
		ds, err = subject.Dataset("test")
		Expect(err).To(BeNil())
		Expect(ds).NotTo(BeNil())
		Expect(ds.MetaData()).To(ContainElement("test"))
	})
	It("should remove dataset definitions", func() {
		Expect(subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test"}},
		})).To(BeNil())
		ds, err := subject.Dataset("test")
		Expect(err).To(BeNil())
		Expect(ds).NotTo(BeNil())
		Expect(ds.MetaData()).To(BeEmpty(), "empty here means non implicit")

		Expect(subject.UpdateConfiguration(&common.Config{})).To(BeNil())
		ds, err = subject.Dataset("test")
		Expect(err).To(BeNil())
		Expect(ds).NotTo(BeNil())
		Expect(ds.MetaData()).NotTo(BeEmpty())
		Expect(ds.MetaData()["raw_column"]).To(Equal("ENTITY"), "indicates implicit config")
	})
	It("should fail on missing layer_config", func() {
		conf, metrics, logger := testDeps()
		conf.LayerServiceConfig = nil
		_, err := NewSnowflakeDataLayer(conf, logger, metrics)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("missing required layer_config block"))
	})
	It("should fail on missing system_config", func() {
		conf, metrics, logger := testDeps()
		conf.NativeSystemConfig = nil
		_, err := NewSnowflakeDataLayer(conf, logger, metrics)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("missing required system_config block"))
	})
	It("should fail on missing required config", func() {
		conf, metrics, logger := testDeps()
		// remove required config param
		delete(conf.NativeSystemConfig, SnowflakeDB)
		_, err := NewSnowflakeDataLayer(conf, logger, metrics)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("missing required config value snowflake_db"))
	})
	Context("with EnvOverrides", func() {
		BeforeEach(func() {
			os.Setenv("SNOWFLAKE_DB", "overridden_test")
		})
		AfterEach(func() {
			os.Setenv("SNOWFLAKE_DB", "")
		})
		It("should override config with env vars", func() {
			conf, metrics, logger := testDeps()
			EnvOverrides(conf)
			subject, err := NewSnowflakeDataLayer(conf, logger, metrics)
			Expect(err).To(BeNil())
			Expect(subject).NotTo(BeNil())
			ds, err := subject.Dataset("implicit_test")
			Expect(err).To(BeNil())
			Expect(ds).NotTo(BeNil())
			Expect(ds.MetaData()).To(ContainElement("overridden_test"))
		})
	})
})
