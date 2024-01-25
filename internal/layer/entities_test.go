package layer

import (
	common "github.com/mimiro-io/common-datalayer"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Dataset", func() {
	var subject *Dataset
	var cnt int
	BeforeEach(func() {
		_, _, logger := testDeps()
		db := newTestDB(cnt)
		cnt++
		dd := &common.DatasetDefinition{SourceConfig: map[string]any{}}
		subject = &Dataset{
			logger:            logger,
			db:                db,
			datasetDefinition: dd,
			sourceConfig:      dd.SourceConfig,
			name:              "testds",
		}
	})
	It("should apply correct params to entities query", func() {
		result, err := subject.Entities("", 0)
		Expect(err).To(BeNil())
		Expect(result).NotTo(BeNil())
		Expect(result.(*testIter).sinceColumn).To(Equal(""))
		Expect(result.(*testIter).sinceToken).To(Equal(""))
		Expect(result.(*testIter).limit).To(Equal(0))

		// since token is ignored if no since column is set
		subject.db.(*testDB).ExpectConn()
		since := "dGVzdA"
		result, err = subject.Entities(since, 7)
		Expect(err).To(BeNil())
		Expect(result).NotTo(BeNil())
		Expect(result.(*testIter).sinceColumn).To(Equal(""))
		Expect(result.(*testIter).sinceToken).To(Equal(""))
		Expect(result.(*testIter).limit).To(Equal(7))

		// since token is used if since column is set
		subject.db.(*testDB).ExpectConn()
		subject.datasetDefinition.SourceConfig[SinceColumn] = "test_col"
		result, err = subject.Entities(since, 7)
		Expect(err).To(BeNil())
		Expect(result).NotTo(BeNil())
		Expect(result.(*testIter).sinceColumn).To(Equal("test_col"))
		Expect(result.(*testIter).sinceToken).To(Equal("dGVzdA"))
		Expect(result.(*testIter).limit).To(Equal(7))
	})
})
