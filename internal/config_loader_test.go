package internal

import (
	. "github.com/onsi/ginkgo/v2"

	. "github.com/onsi/gomega"
)

var _ = Describe("The config loader", func() {
	It("should load config", func() {
		c := &ConfigLoader{}
		// LoadLogger("console", "test", "debug")
		c.loadConfig = c.loadFile
		cfg := &Config{ConfigLocation: "../testdata/config.json"}
		res := c.update(cfg)
		Expect(res).To(BeTrue(), "config should have been updated after first load")
		Expect(len(cfg.DsMappings)).To(Equal(1), "should have 1 mapping")
		res = c.update(cfg)
		Expect(res).To(BeFalse(), "config should not have been updated after second load without change")
		Expect(len(cfg.DsMappings)).To(Equal(1), "should still have 1 mapping")
		Expect(cfg.DsMappings[0].DatasetName).To(Equal("huzzah"), "should have name huzzah")

		cfg.ConfigLocation = "../testdata/config2.json"
		res = c.update(cfg)
		Expect(res).To(BeTrue(), "config should have been updated after this load since file is changed")
		Expect(len(cfg.DsMappings)).To(Equal(2), "should have 2 mappings")
	})

	It("should map all fields correctly", func() {
		c := &ConfigLoader{}
		// LoadLogger("console", "test", "debug")
		c.loadConfig = c.loadFile
		cfg := &Config{ConfigLocation: "../testdata/config.json"}
		res := c.update(cfg)
		Expect(res).To(BeTrue(), "config should have been updated after first load")

		Expect(len(cfg.DsMappings)).To(Equal(1), "should have 1 mapping")
		Expect(cfg.DsMappings[0].DatasetName).To(Equal("huzzah"), "should have name huzzah")
		Expect(cfg.DsMappings[0].SourceConfig[TableName]).To(Equal("ns_huzzah"))
		Expect(cfg.DsMappings[0].SourceConfig[Schema]).To(Equal("datahub"))
		Expect(cfg.DsMappings[0].SourceConfig[Database]).To(Equal("raw"))
		Expect(cfg.DsMappings[0].SourceConfig[RawColumn]).To(Equal("DB_ENTITY"))
		// Expect(cfg.DsMappings[0].SourceConfig[DefaultType]).To(Equal("http://data.mimiro.io/Enthusiasm"))
	})

	It("should unpack datahub content format", func() {
		c := &ConfigLoader{}
		// LoadLogger("console", "test", "debug")
		c.loadConfig = c.loadFile
		cfg := &Config{ConfigLocation: "../testdata/content.json"}
		res := c.update(cfg)
		Expect(res).To(BeTrue(), "config should have been updated after first load")

		Expect(len(cfg.DsMappings)).To(Equal(1), "should have 1 mapping")
		Expect(cfg.DsMappings[0].DatasetName).To(Equal("customer"))
		Expect(cfg.DsMappings[0].SourceConfig[TableName]).To(Equal("customers"))
	})
})

