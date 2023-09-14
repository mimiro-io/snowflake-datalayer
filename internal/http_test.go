package internal

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestHttp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Snowflake Layer Suite")
}

var _ = Describe("The web server", func() {
	var mock sqlmock.Sqlmock
	var db *sql.DB
	var server *Server
	var cfg *Config
	BeforeEach(func() {
		//LoadLogger("console", "test", "debug")
		var err error
		db, mock, err = sqlmock.New() //WithDSN("M_DB:@host:443?database=TESTDB&schema=TESTSCHEMA")
		Expect(err).NotTo(HaveOccurred())
		cfg = &Config{}
		server, err = NewServer(cfg)
		Expect(err).NotTo(HaveOccurred())

		p = &pool{db: db}
		go func() {
			server.E.Start(":17866")
		}()

	})
	AfterEach(func() {
		db.Close()
		server.E.Shutdown(context.Background())
	})
	Context("when getting entities with no-config (implicit) dataset names", func() {
		It("should return 200 if table found", func() {
			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}).
					AddRow(`{"id": "1", "props": {"foo": "bar"}, "refs": {}}`).
					AddRow(`{"id": "2", "props": {"foo": "bar2"}, "refs":{}}`),
				)

			resp, err := http.Get("http://localhost:17866/datasets/foo.bar.baz/entities")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200))
			bodyBytes, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			//GinkgoLogr.Info(string(bodyBytes))
			Expect(string(bodyBytes)).To(Equal(`[{"id": "@context", "namespaces": {
"_": "http://snowflake/foo/bar/baz",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}},
{"id": "1", "props": {"foo": "bar"}, "refs": {}},
{"id": "2", "props": {"foo": "bar2"}, "refs":{}}]
`))
		})
		It("should return 400 if implicit parsing fails", func() {
			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}).
					AddRow(`{"id": "1", "props": {"foo": "bar"}, "refs": {}}`),
				)

			resp, err := http.Get("http://localhost:17866/datasets/foo-bar.baz/entities")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(400))
			bodyBytes, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(bodyBytes)).To(Equal("{\"message\":\"No mapping for dataset\"}\n"))
		})
		// ideally, it should return 400, but it returns 500 because we dont check what the underlying query error actually is caused by
		It("should return 500 if table not found", func() {
			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}).
					AddRow(`{"id": "1", "props": {"foo": "bar"}, "refs": {}}`),
				)

			resp, err := http.Get("http://localhost:17866/datasets/foo.bar.notfound/entities")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(500))
			bodyBytes, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(bodyBytes)).To(Equal("{\"message\":\"Failed to query snowflake\"}\n"))
		})
	})
	Context("when getting entities with configured (explicit) dataset names", func() {
		It("should return 200 if table found", func() {
			cfg.DsMappings = []DatasetDefinition{
				{
					DatasetName: "cucumber",
					SourceConfiguration: SourceConfiguration{
						TableName: "baz",
						Schema:    "bar",
						Database:  "foo",
						RawColumn: "ENTITY",
					},
				},
			}
			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}).
					AddRow(`{"id": "1", "props": {"foo": "bar"}, "refs": {}}`).
					AddRow(`{"id": "2", "props": {"foo": "bar2"}, "refs":{}}`),
				)

			resp, err := http.Get("http://localhost:17866/datasets/cucumber/entities")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200))
			bodyBytes, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			//GinkgoLogr.Info(string(bodyBytes))
			Expect(string(bodyBytes)).To(Equal(`[{"id": "@context", "namespaces": {
"_": "http://snowflake/foo/bar/baz",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}},
{"id": "1", "props": {"foo": "bar"}, "refs": {}},
{"id": "2", "props": {"foo": "bar2"}, "refs":{}}]
`))
		})
	})
})