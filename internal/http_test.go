package internal

import (
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	common_datalayer "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestHttp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Snowflake Layer Suite")
}

var _ = Describe("The web server", Serial, func() {
	var mock sqlmock.Sqlmock
	var db *sql.DB
	var server *Server
	var cfg *Config
	cnt := 0
	BeforeEach(func() {
		//LoadLogger("console", "test", "debug")
		cnt++
		var err error
		db, mock, err = sqlmock.NewWithDSN("M_DB:@host:443?database=TESTDB&schema=TESTSCHEMA&rnd=" + fmt.Sprint(cnt))
		Expect(err).NotTo(HaveOccurred())
		cfg = &Config{SnowflakeDB: "sfdb", SnowflakeSchema: "sfs"}
		p = &pool{db: db}
		mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON'").WillReturnResult(sqlmock.NewResult(1, 1))
		server, err = NewServer(cfg)
		Expect(err).NotTo(HaveOccurred())

		go func() {
			_ = server.E.Start(":17866")
		}()
	})
	AfterEach(func() {
		mock.ExpectClose()
		Expect(db.Close()).To(BeNil())
		Expect(mock.ExpectationsWereMet()).To(BeNil())
		Expect(server.E.Shutdown(context.Background())).To(BeNil())
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
			GinkgoLogr.Info(string(bodyBytes))
			Expect(string(bodyBytes)).To(Equal(`[{"id": "@context", "namespaces": {
"_": "http://snowflake/foo/bar/baz/",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}},
{"id": "1", "props": {"foo": "bar"}, "refs": {}},
{"id": "2", "props": {"foo": "bar2"}, "refs":{}}]
`))
		})
		It("should return 400 if implicit parsing fails", func() {
			resp, err := http.Get("http://localhost:17866/datasets/foo-bar.baz/entities")
			Expect(err).NotTo(HaveOccurred())
			bodyBytes, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(bodyBytes)).To(Equal("{\"message\":\"No mapping for dataset\"}\n"))
			Expect(resp.StatusCode).To(Equal(400))
		})
		// ideally, it should return 400, but it returns 500 because we dont check what the underlying query error actually is caused by
		It("should return 500 if table not found", func() {
			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.notfound").
				WillReturnError(sql.ErrNoRows)

			resp, err := http.Get("http://localhost:17866/datasets/foo.bar.notfound/entities")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(500))
			// bodyBytes, err := io.ReadAll(resp.Body)
			// Expect(err).NotTo(HaveOccurred())
			// Expect(string(bodyBytes)).To(Equal("{\"message\":\"Failed to query snowflake\"}\n"))
		})
	})
	Context("when getting entities with configured (explicit) dataset names", func() {
		It("should return 200 if table found", func() {
			cfg.DsMappings = []*common_datalayer.DatasetDefinition{
				{
					DatasetName: "cucumber",
					SourceConfig: map[string]any{
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
			// GinkgoLogr.Info(string(bodyBytes))
			Expect(string(bodyBytes)).To(Equal(`[{"id": "@context", "namespaces": {
"_": "http://snowflake/foo/bar/baz/",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}},
{"id": "1", "props": {"foo": "bar"}, "refs": {}},
{"id": "2", "props": {"foo": "bar2"}, "refs":{}}]
`))
		})
		It("should return a continuation token when since column is configured", func() {
			cfg.DsMappings = []*common_datalayer.DatasetDefinition{
				{
					DatasetName: "cucumber",
					SourceConfig: map[string]any{
						TableName:   "baz",
						Schema:      "bar",
						Database:    "foo",
						RawColumn:   "ENTITY",
						SinceColumn: "ts",
					},
				},
			}

			mock.ExpectQuery("SELECT MAX\\(ts\\) FROM foo.bar.baz").
				WillReturnRows(sqlmock.NewRows([]string{"MAX"}).AddRow(165565655567))

			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz WHERE ts <= 165565655567").
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
"_": "http://snowflake/foo/bar/baz/",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}},
{"id": "1", "props": {"foo": "bar"}, "refs": {}},
{"id": "2", "props": {"foo": "bar2"}, "refs":{}}, {"id": "@continuation", "token": "MTY1NTY1NjU1NTY3"}]
`))
		})
		It("should apply since tokens in where clause", func() {
			cfg.DsMappings = []*common_datalayer.DatasetDefinition{
				{
					DatasetName: "cucumber",
					SourceConfig: map[string]any{
						TableName:   "baz",
						Schema:      "bar",
						Database:    "foo",
						RawColumn:   "ENTITY",
						SinceColumn: "ts",
					},
				},
			}

			mock.ExpectQuery("SELECT MAX\\(ts\\) FROM foo.bar.baz WHERE ts > 165565655567").
				WillReturnRows(sqlmock.NewRows([]string{"MAX"}).AddRow(165565655568))

			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz WHERE ts > 165565655567 and ts <= 165565655568").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}).
					AddRow(`{"id": "3", "props": {}, "refs": {}}`),
				)

			resp, err := http.Get("http://localhost:17866/datasets/cucumber/entities?since=MTY1NTY1NjU1NTY3")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200))
			bodyBytes, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			//GinkgoLogr.Info(string(bodyBytes))
			Expect(string(bodyBytes)).To(Equal(`[{"id": "@context", "namespaces": {
"_": "http://snowflake/foo/bar/baz/",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}},
{"id": "3", "props": {}, "refs": {}}, {"id": "@continuation", "token": "MTY1NTY1NjU1NTY4"}]
`))
		})
		It("should apply since tokens in where clause and return it again when nothing found", func() {
			cfg.DsMappings = []*common_datalayer.DatasetDefinition{
				{
					DatasetName: "cucumber",
					SourceConfig: map[string]any{
						TableName:   "baz",
						Schema:      "bar",
						Database:    "foo",
						RawColumn:   "ENTITY",
						SinceColumn: "ts",
					},
				},
			}

			mock.ExpectQuery("SELECT MAX\\(ts\\) FROM foo.bar.baz WHERE ts > 165565655567").
				WillReturnRows(sqlmock.NewRows([]string{"MAX"}))

			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz WHERE ts > 165565655567 and ts <= 165565655567").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}),
				)

			resp, err := http.Get("http://localhost:17866/datasets/cucumber/entities?since=MTY1NTY1NjU1NTY3")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200))
			bodyBytes, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			GinkgoLogr.Info(string(bodyBytes))
			Expect(string(bodyBytes)).To(Equal(`[{"id": "@context", "namespaces": {
"_": "http://snowflake/foo/bar/baz/",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}}, {"id": "@continuation", "token": "MTY1NTY1NjU1NTY3"}]
`))
		})
	})
	Context("when getting rows with mappings", func() {
		It("should return entities with selected mapped props and refs", func() {
			cfg.DsMappings = []*common_datalayer.DatasetDefinition{
				{
					DatasetName: "banana",
					SourceConfig: map[string]any{
						TableName: "banana",
						Schema:    "bar",
						Database:  "foo",
					}, OutgoingMappingConfig: &common_datalayer.OutgoingMappingConfig{
						BaseURI: "http://banana/test/",
						Constructions: []*common_datalayer.PropertyConstructor{{
							PropertyName: "id",
							Operation:    "replace",
							Arguments:    []string{"id", "ns65:", ""},
						}, {
							PropertyName: "origin",
							Operation:    "replace",
							Arguments:    []string{"origin", " ", "_"},
						}},
						PropertyMappings: []*common_datalayer.ItemToEntityPropertyMapping{
							{
								Required:        true,
								Property:        "id",
								Datatype:        "string",
								IsIdentity:      true,
								URIValuePattern: "http://banana/test/id/{value}",
							}, {
								Property:       "name",
								EntityProperty: "Name",
							}, {
								Property:       "color",
								EntityProperty: "Color",
							}, {
								Property:        "origin",
								EntityProperty:  "From",
								IsReference:     true,
								URIValuePattern: "http://banana/test/origin/{value}",
							}, {
								Property:       "amt",
								EntityProperty: "Amount",
							}, {
								Property:       "for_sale",
								EntityProperty: "ForSale",
							},
						},
						MapAll: false,
					},
				},
			}
			mock.ExpectQuery("SELECT id, name, color, origin, amt, for_sale FROM foo.bar.banana").
				WillReturnRows(sqlmock.
					NewRows([]string{"id", "name", "color", "origin", "amt", "for_sale"}).
					AddRow("ns65:1", "Dole", "green", "Colombia", 546554, true).
					AddRow("ns65:2", "Chiquita", "yellow", "Costa Rica", 157556, false),
				)
			resp, err := http.Get("http://localhost:17866/datasets/banana/entities")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200))
			bodyBytes, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			// GinkgoLogr.Info(string(bodyBytes))
			nsm := egdm.NewNamespaceContext()
			parser := egdm.NewEntityParser(nsm).WithExpandURIs()
			coll, err := parser.LoadEntityCollection(strings.NewReader(string(bodyBytes)))
			Expect(err).NotTo(HaveOccurred())
			Expect(coll.Entities).To(HaveLen(2))
			e := coll.Entities[0]
			Expect(e.ID).To(Equal("http://banana/test/id/1"))
			Expect(e.Properties).To(HaveLen(4))
			Expect(e.Properties["http://banana/test/Name"]).To(Equal("Dole"))
			Expect(e.Properties["http://banana/test/Color"]).To(Equal("green"))
			Expect(e.Properties["http://banana/test/Amount"]).To(BeEquivalentTo(546554))
			Expect(e.Properties["http://banana/test/ForSale"]).To(Equal(true))
			Expect(e.References).To(HaveLen(1))
			Expect(e.References["http://banana/test/From"]).To(Equal("http://banana/test/origin/Colombia"))
			e = coll.Entities[1]
			Expect(e.ID).To(Equal("http://banana/test/id/2"))
			Expect(e.Properties).To(HaveLen(4))
			Expect(e.Properties["http://banana/test/Name"]).To(Equal("Chiquita"))
			Expect(e.Properties["http://banana/test/Color"]).To(Equal("yellow"))
			Expect(e.Properties["http://banana/test/Amount"]).To(BeEquivalentTo(157556))
			Expect(e.Properties["http://banana/test/ForSale"]).To(Equal(false))
			Expect(e.References).To(HaveLen(1))
			Expect(e.References["http://banana/test/From"]).To(Equal("http://banana/test/origin/Costa_Rica"))
		})
		It("should return entities with all fields as props and selected refs", func() {
			cfg.DsMappings = []*common_datalayer.DatasetDefinition{
				{
					DatasetName: "banana",
					SourceConfig: map[string]any{
						TableName: "banana",
						Schema:    "bar",
						Database:  "foo",
					}, OutgoingMappingConfig: &common_datalayer.OutgoingMappingConfig{
						BaseURI: "http://banana/test/",
						Constructions: []*common_datalayer.PropertyConstructor{{
							PropertyName: "id",
							Operation:    "replace",
							Arguments:    []string{"id", "ns65:", ""},
						}, {
							PropertyName: "origin",
							Operation:    "replace",
							Arguments:    []string{"origin", " ", "_"},
						}},
						PropertyMappings: []*common_datalayer.ItemToEntityPropertyMapping{{
							Required:        true,
							EntityProperty:  "id",
							Property:        "id",
							Datatype:        "string",
							IsReference:     false,
							IsIdentity:      true,
							URIValuePattern: "http://banana/test/id/{value}",
						}, {
							Property:        "origin",
							EntityProperty:  "From",
							IsReference:     true,
							URIValuePattern: "http://banana/test/origin/{value}",
						}},
						MapAll: true,
					},
				},
			}

			mock.ExpectQuery("SELECT \\* FROM foo.bar.banana").
				WillReturnRows(sqlmock.
					NewRows([]string{"id", "name", "color", "origin", "amt", "for_sale"}).
					AddRow("ns65:1", "Dole", "green", "Colombia", 546554, true).
					AddRow("ns65:2", "Chiquita", "yellow", "Costa Rica", 157556, false),
				)
			resp, err := http.Get("http://localhost:17866/datasets/banana/entities")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200))

			bodyBytes, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			// GinkgoLogr.Info(string(bodyBytes))
			nsm := egdm.NewNamespaceContext()
			parser := egdm.NewEntityParser(nsm).WithExpandURIs()
			coll, err := parser.LoadEntityCollection(strings.NewReader(string(bodyBytes)))
			Expect(err).NotTo(HaveOccurred())
			Expect(coll.Entities).To(HaveLen(2))
			e := coll.Entities[0]
			Expect(e.ID).To(Equal("http://banana/test/id/1"))
			Expect(e.Properties).To(HaveLen(6))
			Expect(e.Properties["http://banana/test/name"]).To(Equal("Dole"))
			Expect(e.Properties["http://banana/test/color"]).To(Equal("green"))
			Expect(e.Properties["http://banana/test/amt"]).To(BeEquivalentTo(546554))
			Expect(e.Properties["http://banana/test/for_sale"]).To(Equal(true))
			Expect(e.References).To(HaveLen(1))
			Expect(e.References["http://banana/test/From"]).To(Equal("http://banana/test/origin/Colombia"))
			e = coll.Entities[1]
			Expect(e.ID).To(Equal("http://banana/test/id/2"))
			Expect(e.Properties).To(HaveLen(6))
			Expect(e.Properties["http://banana/test/name"]).To(Equal("Chiquita"))
			Expect(e.Properties["http://banana/test/color"]).To(Equal("yellow"))
			Expect(e.Properties["http://banana/test/amt"]).To(BeEquivalentTo(157556))
			Expect(e.Properties["http://banana/test/for_sale"]).To(Equal(false))
			Expect(e.References).To(HaveLen(1))
			Expect(e.References["http://banana/test/From"]).To(Equal("http://banana/test/origin/Costa_Rica"))
		})
	})
	Context("when posting entities in incremental mode", Serial, func() {
		It("PUT gzipped entity files in a stage and load specified files", func() {
			f, err := os.CreateTemp("", "zip")
			Expect(err).NotTo(HaveOccurred())
			server.handler.ds.sf.NewTmpFile = func(ds string) (*os.File, error, func()) {
				return f, err, func() {}
			}
			defer os.Remove(f.Name())

			// not checking for actual sql, this is regex and it does like all syntax as is
			mock.ExpectExec(`CREATE STAGE IF NOT EXISTS SFDB.SFS.S_POTATOE`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectQuery(fmt.Sprintf(`PUT file://%v`, f.Name())).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			mock.ExpectBegin()
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS SFDB.SFS.POTATOE \\( id varchar, recorded integer," +
				" deleted boolean, dataset varchar, entity variant \\);").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectQuery("COPY INTO SFDB.SFS.POTATOE\\(id, recorded, deleted, dataset, entity\\) FROM \\( " +
				"SELECT \\$1:id::varchar, \\d+::integer, \\$1:deleted::boolean, 'potatoe'::varchar, \\$1::variant FROM @SFDB.SFS.S_POTATOE" +
				"\\) FILE_FORMAT = \\(TYPE='json' COMPRESSION=GZIP\\) FILES = \\('zip.*'\\);",
			).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))
			mock.ExpectCommit()

			res, err := http.Post("http://localhost:17866/datasets/potatoe/entities", "application/json",
				strings.NewReader(`[{"id": "@context", "namespaces": {
"x": "http://snowflake/foo/",
"y": "http://snowflake/bar/",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}},
{"id": "x:1", "recorded": 1456456456, "props": {"x:foo": "bar"}, "refs": {"x:baz": "y:hello"}},
{"id": "x:2", "recorded": 1456456457, "props": {"x:foo": "bar2"}, "refs":{"x:baz": ["y:hi", "y:bye"]}}]
`))
			Expect(err).NotTo(HaveOccurred())
			Expect(res.StatusCode).To(Equal(200))
			f2, err := os.Open(f.Name())
			Expect(err).NotTo(HaveOccurred())
			r, err := gzip.NewReader(f2)
			Expect(err).NotTo(HaveOccurred())
			bytes, err := io.ReadAll(r)
			Expect(err).NotTo(HaveOccurred())
			//println(string(bytes))
			Expect(string(bytes)).To(Equal(
				`{"id":"http://snowflake/foo/1","recorded":1456456456,"refs":{"http://snowflake/foo/baz":"http://snowflake/bar/hello"},"props":{"http://snowflake/foo/foo":"bar"}}
{"id":"http://snowflake/foo/2","recorded":1456456457,"refs":{"http://snowflake/foo/baz":["http://snowflake/bar/hi","http://snowflake/bar/bye"]},"props":{"http://snowflake/foo/foo":"bar2"}}
`))
		})
		It("PUT gzipped mapped files in a stage and load specified files", func() {
			f, err := os.CreateTemp("", "zip")
			Expect(err).NotTo(HaveOccurred())
			server.handler.ds.sf.NewTmpFile = func(ds string) (*os.File, error, func()) {
				return f, err, func() {}
			}
			defer os.Remove(f.Name())

			cfg.DsMappings = []*common_datalayer.DatasetDefinition{{
				DatasetName: "potatoes",
				SourceConfig: map[string]any{
					TableName: "potatoe",
					Schema:    "SFS2",
					Database:  "SFDB2",
				},
				IncomingMappingConfig: &common_datalayer.IncomingMappingConfig{
					MapNamed: false,
					PropertyMappings: []*common_datalayer.EntityToItemPropertyMapping{{
						EntityProperty:       "",
						Property:             "",
						Datatype:             "",
						DefaultValue:         "",
						StripReferencePrefix: false,
						Required:             false,
						IsIdentity:           false,
						IsReference:          false,
						IsDeleted:            false,
						IsRecorded:           false,
					}},
					BaseURI: "http://potatoe/test/",
				},
			}}

			// not checking for actual sql, this is regex and it does like all syntax as is
			mock.ExpectExec(`CREATE STAGE IF NOT EXISTS SFDB2.SFS2.S_POTATOE`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectQuery(fmt.Sprintf(`PUT file://%v`, f.Name())).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			mock.ExpectBegin()
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS SFDB2.SFS2.POTATOE \\( id varchar, recorded integer," +
				" deleted boolean, dataset varchar, foo varchar, ok boolean, num integer, baz varchar \\);").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectQuery("COPY INTO SFDB2.SFS2.POTATOE\\(id, recorded, deleted, dataset, foo, baz\\) FROM \\( " +
				"SELECT \\$1:id::varchar, \\d+::integer, \\$1:deleted::boolean, 'potatoe'::varchar, \\$1::variant FROM @SFDB.SFS.S_POTATOE" +
				"\\) FILE_FORMAT = \\(TYPE='json' COMPRESSION=GZIP\\) FILES = \\('zip.*'\\);",
			).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))
			mock.ExpectCommit()

			res, err := http.Post("http://localhost:17866/datasets/potatoes/entities", "application/json",
				strings.NewReader(`[{"id": "@context", "namespaces": {
"x": "http://snowflake/foo/",
"y": "http://snowflake/bar/",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}},
{"id": "x:1", "props": {"x:foo": "bar", "x:ok": true, "x:num": 12}, "refs": {"x:baz": "y:hello"}},
{"id": "x:2", "props": {"x:foo": "bar2"}, "refs":{"x:baz": ["y:hi", "y:bye"]}},
{"id": "x:3", "deleted": true, "recorded": 126456789123, "props": {"x:foo": "bar3", "x:ok": true, "x:num": 12},"refs": {"x:baz": "y:hello"}}]
`))
			Expect(err).NotTo(HaveOccurred())
			Expect(res.StatusCode).To(Equal(200))
			f2, err := os.Open(f.Name())
			Expect(err).NotTo(HaveOccurred())
			r, err := gzip.NewReader(f2)
			Expect(err).NotTo(HaveOccurred())
			bytes, err := io.ReadAll(r)
			Expect(err).NotTo(HaveOccurred())
			println(string(bytes))
			Expect(string(bytes)).To(Equal(
				`{"id":"http://snowflake/foo/1","recorded":0,"deleted":false,"refs":{"http://snowflake/foo/baz":"http://snowflake/bar/hello"},"props":{"http://snowflake/foo/foo":"bar"}}
{"id":"http://snowflake/foo/2","recorded":0,"deleted":false,"refs":{"http://snowflake/foo/baz":["http://snowflake/bar/hi","http://snowflake/bar/bye"]},"props":{"http://snowflake/foo/foo":"bar2"}}
`))
		})
	})
})
