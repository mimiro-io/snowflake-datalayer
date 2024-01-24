package layer

import (
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
	BeforeEach(func() {
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
})
