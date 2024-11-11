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
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	common_datalayer "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

func TestWebServer(t *testing.T) {
	var mock sqlmock.Sqlmock
	var server *common_datalayer.ServiceRunner
	var cfg *common_datalayer.Config
	var testLayer *SnowflakeDataLayer
	var tDB db
	cnt := 0
	setup := func() {
		cnt++
		// fmt.Printf("setup: %v\n", cnt)
		cfg, _, _ = testDeps()
		cfg.LayerServiceConfig.LogLevel = "error"

		tmpDir := t.TempDir()
		jsonConf, _ := json.Marshal(cfg)
		os.WriteFile(tmpDir+"/config.json", jsonConf, 0644)

		server = common_datalayer.NewServiceRunner(func(conf *common_datalayer.Config, logger common_datalayer.Logger, metrics common_datalayer.Metrics) (common_datalayer.DataLayerService, error) {
			err := validateConfig(conf)
			if err != nil {
				return nil, err
			}

			//sfdb, err := newSfDB(conf, logger, metrics)
			//if err != nil {
			//	return nil, err
			//}
			tDB, err = newTestDB(cnt, conf, logger, metrics)
			if err != nil {
				return nil, err
			}
			mock = tDB.(*testDB).mock

			l := &SnowflakeDataLayer{
				datasets: map[string]*Dataset{},
				logger:   logger,
				metrics:  metrics,
				config:   conf,
				db:       tDB,
			}
			testLayer = l
			err = l.UpdateConfiguration(conf)
			if err != nil {
				return nil, err
			}
			return l, nil
		})
		server.WithConfigLocation(tmpDir)
		err := server.Start()
		if err != nil {
			t.Fatalf("failed to create snowflake data layer: %v", err)
		}
		ts := time.Now()
		for {
			health, err := http.Get("http://localhost:17866/health")
			if err != nil {
				t.Fatalf("failed to check health: %v", err)
			}
			if health.StatusCode == 200 {
				break
			}
			if time.Since(ts) > 10*time.Second {
				t.Fatalf("failed to start server")
			}
		}
	}
	cleanup := func() {
		// fmt.Printf("cleaning up: %v\n", cnt)
		err := mock.ExpectationsWereMet()
		if err != nil {
			t.Fatalf("there were unfulfilled expectations: %s", err)
		}
		mock.ExpectClose()
		err = tDB.(*testDB).db.Close()
		if err != nil {
			t.Fatalf("failed to close db: %v", err)
		}

		err = server.Stop()
		if err != nil {
			t.Fatalf("failed to stop server: %v", err)
		}
		err = testLayer.Stop(context.Background())
		if err != nil {
			t.Fatalf("failed to stop layer: %v", err)
		}

		tDB.close()
	}
	t.Run("when getting entities with no-config (implicit) dataset names", func(t *testing.T) {
		t.Run("should return 200 if table found", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}).
					AddRow(`{"id": "1", "props": {"foo": "bar"}, "refs": {}}`).
					AddRow(`{"id": "2", "props": {"foo": "bar2"}, "refs":{}}`),
				)

			resp, err := http.Get("http://localhost:17866/datasets/foo.bar.baz/entities")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			// t.Log(string(bodyBytes))
			expected := `[
{"id":"@context","namespaces":{}},
{"id":"1","refs":{},"props":{"foo":"bar"}},
{"id":"2","refs":{},"props":{"foo":"bar2"}},
{"id":"@continuation","token":""}]
`
			if string(bodyBytes) != expected {
				t.Fatalf("unexpected response body: %s. wanted:\n%s", string(bodyBytes), expected)
			}
		})
		t.Run("should return 500 if implicit parsing fails", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			mock.ExpectQuery("SELECT ENTITY FROM testdb.testschema.foo-bar.baz").
				WillReturnError(sql.ErrNoRows)
			resp, err := http.Get("http://localhost:17866/datasets/foo-bar.baz/entities")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}

			if resp.StatusCode != 500 {
				t.Fatalf("expected 500, got %d", resp.StatusCode)
			}
		})
		// ideally, it should return 400, but it returns 500 because we dont check what the underlying query error actually is caused by
		t.Run("should return 500 if table not found", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.notfound").
				WillReturnError(sql.ErrNoRows)

			resp, err := http.Get("http://localhost:17866/datasets/foo.bar.notfound/entities")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}
			if resp.StatusCode != 500 {
				t.Fatalf("expected 500, got %d", resp.StatusCode)
			}
			// bodyBytes, err := io.ReadAll(resp.Body)
			// Expect(err).NotTo(HaveOccurred())
			// Expect(string(bodyBytes)).To(Equal("{\"message\":\"Failed to query snowflake\"}\n"))
		})
	})
	t.Run("when getting entities with configured (explicit) dataset names", func(t *testing.T) {
		t.Run("should return 200 if table found", func(t *testing.T) {
			setup()

			t.Cleanup(cleanup)
			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{
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
			testLayer.UpdateConfiguration(cfg)
			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}).
					AddRow(`{"id": "1", "props": {"foo": "bar"}, "refs": {}}`).
					AddRow(`{"id": "2", "props": {"foo": "bar2"}, "refs":{}}`),
				)

			resp, err := http.Get("http://localhost:17866/datasets/cucumber/entities")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			// GinkgoLogr.Info(string(bodyBytes))
			if string(bodyBytes) != `[
{"id":"@context","namespaces":{}},
{"id":"1","refs":{},"props":{"foo":"bar"}},
{"id":"2","refs":{},"props":{"foo":"bar2"}},
{"id":"@continuation","token":""}]
` {
				t.Fatalf("unexpected response body: %s", string(bodyBytes))
			}
		})
		t.Run("should return a continuation token when since column is configured", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{
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
			testLayer.UpdateConfiguration(cfg)

			mock.ExpectQuery("SELECT MAX\\(ts\\) FROM foo.bar.baz").
				WillReturnRows(sqlmock.NewRows([]string{"MAX"}).AddRow(165565655567))

			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz WHERE ts <= 165565655567").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}).
					AddRow(`{"id": "1", "props": {"foo": "bar"}, "refs": {}}`).
					AddRow(`{"id": "2", "props": {"foo": "bar2"}, "refs":{}}`),
				)

			resp, err := http.Get("http://localhost:17866/datasets/cucumber/entities")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			// GinkgoLogr.Info(string(bodyBytes))
			expected := `[
{"id":"@context","namespaces":{}},
{"id":"1","refs":{},"props":{"foo":"bar"}},
{"id":"2","refs":{},"props":{"foo":"bar2"}},
{"id":"@continuation","token":"MTY1NTY1NjU1NTY3"}]
`
			if string(bodyBytes) != expected {
				t.Fatalf("unexpected response body: %s. wanted: %s", string(bodyBytes), expected)
			}
		})
		t.Run("should apply since tokens in where clause", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{
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
			testLayer.UpdateConfiguration(cfg)

			mock.ExpectQuery("SELECT MAX\\(ts\\) FROM foo.bar.baz WHERE ts > 165565655567").
				WillReturnRows(sqlmock.NewRows([]string{"MAX"}).AddRow(165565655568))

			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz WHERE ts > 165565655567 and ts <= 165565655568").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}).
					AddRow(`{"id": "3", "props": {}, "refs": {}}`),
				)

			resp, err := http.Get("http://localhost:17866/datasets/cucumber/entities?from=MTY1NTY1NjU1NTY3")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			// GinkgoLogr.Info(string(bodyBytes))
			if string(bodyBytes) != `[
{"id":"@context","namespaces":{}},
{"id":"3","refs":{},"props":{}},
{"id":"@continuation","token":"MTY1NTY1NjU1NTY4"}]
` {
				t.Fatalf("unexpected response body: %s", string(bodyBytes))
			}
		})
		t.Run("should apply string since tokens in where clause", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{
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
			testLayer.UpdateConfiguration(cfg)

			mock.ExpectQuery("SELECT MAX\\(ts\\) FROM foo.bar.baz").
				WillReturnRows(sqlmock.NewRows([]string{"MAX"}).AddRow("2024-09-01T11:00:00+02:00"))

			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz WHERE ts <= '2024\\-09\\-01T11:00:00\\+02:00'").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}).
					AddRow(`{"id": "3", "props": {}, "refs": {}}`),
				)

			resp, err := http.Get("http://localhost:17866/datasets/cucumber/changes")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			// GinkgoLogr.Info(string(bodyBytes))
			if string(bodyBytes) != `[
{"id":"@context","namespaces":{}},
{"id":"3","refs":{},"props":{}},
{"id":"@continuation","token":"JzIwMjQtMDktMDFUMTE6MDA6MDArMDI6MDAn"}]
` {
				t.Fatalf("unexpected response body: %s", string(bodyBytes))
			}

			tDB.(*testDB).ExpectConn()
			mock.ExpectQuery("SELECT MAX\\(ts\\) FROM foo.bar.baz").
				WillReturnRows(sqlmock.NewRows([]string{"MAX"}).AddRow("2024-09-01T11:00:01+02:00"))

			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz WHERE ts > '2024\\-09\\-01T11:00:00\\+02:00' " +
				"and ts <= '2024\\-09\\-01T11:00:01\\+02:00'").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}).
					AddRow(`{"id": "3", "props": {}, "refs": {}}`),
				)
			resp, err = http.Get("http://localhost:17866/datasets/cucumber/changes?since=JzIwMjQtMDktMDFUMTE6MDA6MDArMDI6MDAn")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			bodyBytes, err = io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			// GinkgoLogr.Info(string(bodyBytes))
			if string(bodyBytes) != `[
{"id":"@context","namespaces":{}},
{"id":"3","refs":{},"props":{}},
{"id":"@continuation","token":"JzIwMjQtMDktMDFUMTE6MDA6MDErMDI6MDAn"}]
` {
				t.Fatalf("unexpected response body: %s", string(bodyBytes))
			}
		})
		t.Run("should apply since tokens in where clause and return it again when nothing found", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{
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
			testLayer.UpdateConfiguration(cfg)

			mock.ExpectQuery("SELECT MAX\\(ts\\) FROM foo.bar.baz WHERE ts > 165565655567").
				WillReturnRows(sqlmock.NewRows([]string{"MAX"}))

			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz WHERE ts > 165565655567 and ts <= 165565655567").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}),
				)

			resp, err := http.Get("http://localhost:17866/datasets/cucumber/changes?since=MTY1NTY1NjU1NTY3")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			t.Log(string(bodyBytes))
			if string(bodyBytes) != `[
{"id":"@context","namespaces":{}},
{"id":"@continuation","token":"MTY1NTY1NjU1NTY3"}]
` {
				t.Fatalf("unexpected response body: %s", string(bodyBytes))
			}
		})

		t.Run("should apply string since tokens in where clause and return it again when nothing found", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{
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
			testLayer.UpdateConfiguration(cfg)

			mock.ExpectQuery("SELECT MAX\\(ts\\) FROM foo.bar.baz").
				WillReturnRows(sqlmock.NewRows([]string{"MAX"}))

			mock.ExpectQuery("SELECT ENTITY FROM foo.bar.baz WHERE ts > '2024\\-09\\-01T11:00:00\\+02:00' " +
				"and ts <= '2024\\-09\\-01T11:00:00\\+02:00'").
				WillReturnRows(sqlmock.
					NewRows([]string{"ENTITY"}),
				)
			resp, err := http.Get("http://localhost:17866/datasets/cucumber/changes?since=JzIwMjQtMDktMDFUMTE6MDA6MDArMDI6MDAn")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			// GinkgoLogr.Info(string(bodyBytes))
			if string(bodyBytes) != `[
{"id":"@context","namespaces":{}},
{"id":"@continuation","token":"JzIwMjQtMDktMDFUMTE6MDA6MDArMDI6MDAn"}]
` {
				t.Fatalf("unexpected response body: %s", string(bodyBytes))
			}
		})
	})
	t.Run("when getting rows with mappings", func(t *testing.T) {
		t.Run("should return entities with selected mapped props and refs", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{
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
			testLayer.UpdateConfiguration(cfg)
			mock.ExpectQuery("SELECT id, name, color, origin, amt, for_sale FROM foo.bar.banana").
				WillReturnRows(sqlmock.
					NewRows([]string{"id", "name", "color", "origin", "amt", "for_sale"}).
					AddRow("ns65:1", "Dole", "green", "Colombia", 546554, true).
					AddRow("ns65:2", "Chiquita", "yellow", "Costa Rica", 157556, false),
				)
			resp, err := http.Get("http://localhost:17866/datasets/banana/entities")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			// GinkgoLogr.Info(string(bodyBytes))
			nsm := egdm.NewNamespaceContext()
			parser := egdm.NewEntityParser(nsm).WithExpandURIs()
			coll, err := parser.LoadEntityCollection(strings.NewReader(string(bodyBytes)))
			if err != nil {
				t.Fatalf("failed to parse response body: %v", err)
			}
			if len(coll.Entities) != 2 {
				t.Fatalf("expected 2 entities, got %d", len(coll.Entities))
			}
			e := coll.Entities[0]
			if e.ID != "http://banana/test/id/1" {
				t.Fatalf("expected id to be http://banana/test/id/1, got %s", e.ID)
			}
			if len(e.Properties) != 4 {
				t.Fatalf("expected 4 properties, got %d", len(e.Properties))
			}
			if e.Properties["http://banana/test/Name"] != "Dole" {
				t.Fatalf("expected name to be Dole, got %s", e.Properties["http://banana/test/Name"])
			}
			if e.Properties["http://banana/test/Color"] != "green" {
				t.Fatalf("expected color to be green, got %s", e.Properties["http://banana/test/Color"])
			}
			if e.Properties["http://banana/test/Amount"] != float64(546554) {
				t.Fatalf("expected amount to be 546554, got %v", e.Properties["http://banana/test/Amount"])
			}
			if e.Properties["http://banana/test/ForSale"] != true {
				t.Fatalf("expected for_sale to be true, got %v", e.Properties["http://banana/test/ForSale"])
			}
			if len(e.References) != 1 {
				t.Fatalf("expected 1 reference, got %d", len(e.References))
			}
			if e.References["http://banana/test/From"] != "http://banana/test/origin/Colombia" {
				t.Fatalf("expected from to be http://banana/test/origin/Colombia, got %s", e.References["http://banana/test/From"])
			}
			e = coll.Entities[1]
			if e.ID != "http://banana/test/id/2" {
				t.Fatalf("expected id to be http://banana/test/id/2, got %s", e.ID)
			}
			if len(e.Properties) != 4 {
				t.Fatalf("expected 4 properties, got %d", len(e.Properties))
			}
			if e.Properties["http://banana/test/Name"] != "Chiquita" {
				t.Fatalf("expected name to be Chiquita, got %s", e.Properties["http://banana/test/Name"])
			}
			if e.Properties["http://banana/test/Color"] != "yellow" {
				t.Fatalf("expected color to be yellow, got %s", e.Properties["http://banana/test/Color"])
			}
			if e.Properties["http://banana/test/Amount"] != float64(157556) {
				t.Fatalf("expected amount to be 157556, got %v", e.Properties["http://banana/test/Amount"])
			}
			if e.Properties["http://banana/test/ForSale"] != false {
				t.Fatalf("expected for_sale to be false, got %v", e.Properties["http://banana/test/ForSale"])
			}
			if len(e.References) != 1 {
				t.Fatalf("expected 1 reference, got %d", len(e.References))
			}
			if e.References["http://banana/test/From"] != "http://banana/test/origin/Costa_Rica" {
				t.Fatalf("expected from to be http://banana/test/origin/Costa_Rica, got %s", e.References["http://banana/test/From"])
			}
		})
		t.Run("should return entities with all fields as props and selected refs", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{
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
			testLayer.UpdateConfiguration(cfg)

			mock.ExpectQuery("SELECT \\* FROM foo.bar.banana").
				WillReturnRows(sqlmock.
					NewRows([]string{"id", "name", "color", "origin", "amt", "for_sale"}).
					AddRow("ns65:1", "Dole", "green", "Colombia", 546554, true).
					AddRow("ns65:2", "Chiquita", "yellow", "Costa Rica", 157556, false),
				)
			resp, err := http.Get("http://localhost:17866/datasets/banana/entities")
			if err != nil {
				t.Fatalf("failed to get entities: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}

			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			// GinkgoLogr.Info(string(bodyBytes))
			nsm := egdm.NewNamespaceContext()
			parser := egdm.NewEntityParser(nsm).WithExpandURIs()
			coll, err := parser.LoadEntityCollection(strings.NewReader(string(bodyBytes)))
			if err != nil {
				t.Fatalf("failed to parse response body: %v", err)
			}
			if len(coll.Entities) != 2 {
				t.Fatalf("expected 2 entities, got %d", len(coll.Entities))
			}
			e := coll.Entities[0]
			if e.ID != "http://banana/test/id/1" {
				t.Fatalf("expected id to be http://banana/test/id/1, got %s", e.ID)
			}
			if len(e.Properties) != 6 {
				t.Fatalf("expected 6 properties, got %d", len(e.Properties))
			}
			if e.Properties["http://banana/test/name"] != "Dole" {
				t.Fatalf("expected name to be Dole, got %s", e.Properties["http://banana/test/name"])
			}
			if e.Properties["http://banana/test/color"] != "green" {
				t.Fatalf("expected color to be green, got %s", e.Properties["http://banana/test/color"])
			}
			if e.Properties["http://banana/test/amt"] != float64(546554) {
				t.Fatalf("expected amount to be 546554, got %v", e.Properties["http://banana/test/amt"])
			}
			if e.Properties["http://banana/test/for_sale"] != true {
				t.Fatalf("expected for_sale to be true, got %v", e.Properties["http://banana/test/for_sale"])
			}
			if len(e.References) != 1 {
				t.Fatalf("expected 1 reference, got %d", len(e.References))
			}
			if e.References["http://banana/test/From"] != "http://banana/test/origin/Colombia" {
				t.Fatalf("expected from to be http://banana/test/origin/Colombia, got %s", e.References["http://banana/test/From"])
			}
			e = coll.Entities[1]
			if e.ID != "http://banana/test/id/2" {
				t.Fatalf("expected id to be http://banana/test/id/2, got %s", e.ID)
			}
			if len(e.Properties) != 6 {
				t.Fatalf("expected 6 properties, got %d", len(e.Properties))
			}
			if e.Properties["http://banana/test/name"] != "Chiquita" {
				t.Fatalf("expected name to be Chiquita, got %s", e.Properties["http://banana/test/name"])
			}
			if e.Properties["http://banana/test/color"] != "yellow" {
				t.Fatalf("expected color to be yellow, got %s", e.Properties["http://banana/test/color"])
			}
			if e.Properties["http://banana/test/amt"] != float64(157556) {
				t.Fatalf("expected amount to be 157556, got %v", e.Properties["http://banana/test/amt"])
			}
			if e.Properties["http://banana/test/for_sale"] != false {
				t.Fatalf("expected for_sale to be false, got %v", e.Properties["http://banana/test/for_sale"])
			}
			if len(e.References) != 1 {
				t.Fatalf("expected 1 reference, got %d", len(e.References))
			}
			if e.References["http://banana/test/From"] != "http://banana/test/origin/Costa_Rica" {
				t.Fatalf("expected from to be http://banana/test/origin/Costa_Rica, got %s", e.References["http://banana/test/From"])
			}
		})
	})
	t.Run("when posting entities in incremental mode", func(t *testing.T) {
		t.Run("PUT gzipped entity files in a stage and load specified files", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			f, err := os.CreateTemp("", "zip")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			testLayer.db.(*testDB).NewTmpFile = func(ds string) (*os.File, func(), error) {
				return f, func() {}, err
			}
			defer os.Remove(f.Name())

			// not checking for actual sql, this is regex and it does like all syntax as is
			mock.ExpectExec(`CREATE STAGE IF NOT EXISTS TESTDB.TESTSCHEMA.S_POTATOE`).WillReturnResult(sqlmock.NewResult(1, 1))

			//// new conn
			//mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON'").WillReturnResult(sqlmock.NewResult(1, 1))
			//mock.ExpectExec("USE SECONDARY ROLES ALL").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectQuery(fmt.Sprintf(`PUT file://%v`, f.Name())).
				WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			//// new conn
			//mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON'").WillReturnResult(sqlmock.NewResult(1, 1))
			//mock.ExpectExec("USE SECONDARY ROLES ALL").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectBegin()
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS TESTDB.TESTSCHEMA.POTATOE \\( id varchar, recorded integer," +
				" deleted boolean, dataset varchar, entity variant \\);").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectQuery("COPY INTO TESTDB.TESTSCHEMA.POTATOE\\(id, recorded, deleted, dataset, entity\\) FROM \\( " +
				"SELECT \\$1:id::varchar, \\d+::integer, coalesce\\(\\$1:deleted::boolean, false\\), 'testdb.testschema.potatoe'::varchar, " +
				"\\$1::variant as entity FROM @TESTDB.TESTSCHEMA.S_POTATOE" +
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
			if err != nil {
				t.Fatalf("failed to post entities: %v", err)
			}
			if res.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", res.StatusCode)
			}
			f2, err := os.Open(f.Name())
			if err != nil {
				t.Fatalf("failed to open temp file: %v", err)
			}
			r, err := gzip.NewReader(f2)
			if err != nil {
				t.Fatalf("failed to create gzip reader: %v", err)
			}
			bytes, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read gzip file: %v", err)
			}
			// println(string(bytes))
			if string(bytes) != `{"id":"http://snowflake/foo/1","recorded":1456456456,"refs":{"http://snowflake/foo/baz":"http://snowflake/bar/hello"},"props":{"http://snowflake/foo/foo":"bar"}}
{"id":"http://snowflake/foo/2","recorded":1456456457,"refs":{"http://snowflake/foo/baz":["http://snowflake/bar/hi","http://snowflake/bar/bye"]},"props":{"http://snowflake/foo/foo":"bar2"}}
` {
				t.Fatalf("unexpected gzip file: %s", string(bytes))
			}
		})
		t.Run("PUT gzipped mapped files in a stage and load specified files", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			f, err := os.CreateTemp("", "zip")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			testLayer.db.(*testDB).NewTmpFile = func(ds string) (*os.File, func(), error) {
				return f, func() {}, err
			}
			defer os.Remove(f.Name())

			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{{
				DatasetName: "potatoes",
				SourceConfig: map[string]any{
					TableName: "potatoe",
					Schema:    "SFS2",
					Database:  "SFDB2",
				},
				IncomingMappingConfig: &common_datalayer.IncomingMappingConfig{
					MapNamed: false,
					PropertyMappings: []*common_datalayer.EntityToItemPropertyMapping{{
						EntityProperty: "foo",
						Property:       "foo",
						Datatype:       "varchar",
					}, {
						EntityProperty: "ok",
						Property:       "ok",
						Datatype:       "boolean",
					}, {
						EntityProperty: "num",
						Property:       "num",
						Datatype:       "integer",
					}, {
						EntityProperty: "baz",
						Property:       "baz",
						Datatype:       "varchar",
						IsReference:    true,
					}},
					BaseURI: "http://potatoe/test/",
				},
			}}
			testLayer.UpdateConfiguration(cfg)

			// not checking for actual sql, this is regex and it does like all syntax as is
			mock.ExpectExec(`CREATE STAGE IF NOT EXISTS SFDB2.SFS2.S_POTATOE copy`).
				WillReturnResult(sqlmock.NewResult(1, 1))

			//// new conn
			//mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON'").WillReturnResult(sqlmock.NewResult(1, 1))
			//mock.ExpectExec("USE SECONDARY ROLES ALL").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectQuery(fmt.Sprintf(`PUT file://%v`, f.Name())).
				WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			//// new conn
			//mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON'").WillReturnResult(sqlmock.NewResult(1, 1))
			//mock.ExpectExec("USE SECONDARY ROLES ALL").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectBegin()
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS SFDB2.SFS2.POTATOE \\( id varchar, recorded integer," +
				" deleted boolean, dataset varchar, foo varchar, ok boolean, num integer, baz varchar \\);").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectQuery("COPY INTO SFDB2.SFS2.POTATOE\\(id, recorded, deleted, dataset, foo, ok, num, baz\\) FROM \\( " +
				"SELECT \\$1:id::varchar, \\d+::integer, coalesce\\(\\$1:deleted::boolean, false\\), 'potatoes'::varchar, " +
				"\\$1:props:\"foo\"::varchar as foo, " +
				"\\$1:props:\"ok\"::boolean as ok, " +
				"\\$1:props:\"num\"::integer as num, " +
				"\\$1:refs:\"baz\"::varchar as baz " +
				"FROM @SFDB2.SFS2.S_POTATOE\\) FILE_FORMAT = \\(TYPE='json' COMPRESSION=GZIP\\) FILES = \\('zip.*'\\);",
			).
				WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))
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
			if err != nil {
				t.Fatalf("failed to post entities: %v", err)
			}
			if res.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", res.StatusCode)
			}
			f2, err := os.Open(f.Name())
			if err != nil {
				t.Fatalf("failed to open temp file: %v", err)
			}
			r, err := gzip.NewReader(f2)
			if err != nil {
				t.Fatalf("failed to create gzip reader: %v", err)
			}
			bytes, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read gzip file: %v", err)
			}
			// println(string(bytes))
			expected := `{"id":"http://snowflake/foo/1","refs":{"http://snowflake/foo/baz":"http://snowflake/bar/hello"},"props":{"http://snowflake/foo/foo":"bar","http://snowflake/foo/num":12,"http://snowflake/foo/ok":true}}
{"id":"http://snowflake/foo/2","refs":{"http://snowflake/foo/baz":["http://snowflake/bar/hi","http://snowflake/bar/bye"]},"props":{"http://snowflake/foo/foo":"bar2"}}
{"id":"http://snowflake/foo/3","recorded":126456789123,"deleted":true,"refs":{"http://snowflake/foo/baz":"http://snowflake/bar/hello"},"props":{"http://snowflake/foo/foo":"bar3","http://snowflake/foo/num":12,"http://snowflake/foo/ok":true}}
`

			if string(bytes) != expected {
				t.Fatalf("unexpected gzip file: \n\n%s. wanted \n\n%s", string(bytes), expected)
			}
		})
		t.Run("PUT entity files in a stage and load specified files into LATEST_ACTIVE", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			f, err := os.CreateTemp("", "zip")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			testLayer.db.(*testDB).NewTmpFile = func(ds string) (*os.File, func(), error) {
				return f, func() {}, err
			}
			defer os.Remove(f.Name())

			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{{
				DatasetName: "potatoe",
				SourceConfig: map[string]any{
					TableName:   "potatoe",
					Schema:      "TESTSCHEMA",
					Database:    "TESTDB",
					LatestTable: true,
				},
			}}
			testLayer.UpdateConfiguration(cfg)
			// not checking for actual sql, this is regex and it does like all syntax as is
			mock.ExpectExec(`CREATE STAGE IF NOT EXISTS TESTDB.TESTSCHEMA.S_POTATOE`).WillReturnResult(sqlmock.NewResult(1, 1))

			//// new conn
			//mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON'").WillReturnResult(sqlmock.NewResult(1, 1))
			//mock.ExpectExec("USE SECONDARY ROLES ALL").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectQuery(fmt.Sprintf(`PUT file://%v`, f.Name())).
				WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			//// new conn
			//mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON'").WillReturnResult(sqlmock.NewResult(1, 1))
			//mock.ExpectExec("USE SECONDARY ROLES ALL").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectBegin()
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS TESTDB.TESTSCHEMA.POTATOE \\( id varchar, recorded integer," +
				" deleted boolean, dataset varchar, entity variant \\);").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectExec("CREATE TABLE IF NOT EXISTS TESTDB.TESTSCHEMA.POTATOE_LATEST \\( id varchar, " +
				"recorded integer, deleted boolean, dataset varchar, entity variant \\)").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectQuery("COPY INTO TESTDB.TESTSCHEMA.POTATOE\\(id, recorded, deleted, dataset, entity\\) FROM \\( " +
				"SELECT \\$1:id::varchar, \\d+::integer, coalesce\\(\\$1:deleted::boolean, false\\), 'potatoe'::varchar, " +
				"\\$1::variant as entity FROM @TESTDB.TESTSCHEMA.S_POTATOE" +
				"\\) FILE_FORMAT = \\(TYPE='json' COMPRESSION=GZIP\\) FILES = \\('zip.*'\\);",
			).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			mock.ExpectQuery("MERGE INTO TESTDB.TESTSCHEMA.POTATOE_LATEST AS latest " +
				"USING \\( SELECT \\$1:id::varchar as id, " +
				"\\d+::integer as recorded, " +
				"coalesce\\(\\$1:deleted::boolean, false\\) as deleted, " +
				"'potatoe'::varchar as dataset, " +
				"\\$1::variant as entity " +
				"FROM @TESTDB.TESTSCHEMA.S_POTATOE \\(PATTERN \\=\\> '.*\\(zip.*\\)'\\) \\) AS src " +
				"ON latest.id = src.id WHEN MATCHED THEN UPDATE SET " +
				"latest.recorded = src.recorded, latest.deleted = src.deleted, latest.dataset = src.dataset, " +
				"latest.entity = src.entity " +
				"WHEN NOT MATCHED THEN INSERT \\(id, recorded, deleted, dataset, entity\\) " +
				"VALUES \\(src.id, src.recorded, src.deleted, src.dataset, src.entity\\);",
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
			if err != nil {
				t.Fatalf("failed to post entities: %v", err)
			}
			if res.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", res.StatusCode)
			}
			f2, err := os.Open(f.Name())
			if err != nil {
				t.Fatalf("failed to open temp file: %v", err)
			}
			r, err := gzip.NewReader(f2)
			if err != nil {
				t.Fatalf("failed to create gzip reader: %v", err)
			}
			bytes, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read gzip file: %v", err)
			}
			// println(string(bytes))
			if string(bytes) != `{"id":"http://snowflake/foo/1","recorded":1456456456,"refs":{"http://snowflake/foo/baz":"http://snowflake/bar/hello"},"props":{"http://snowflake/foo/foo":"bar"}}
{"id":"http://snowflake/foo/2","recorded":1456456457,"refs":{"http://snowflake/foo/baz":["http://snowflake/bar/hi","http://snowflake/bar/bye"]},"props":{"http://snowflake/foo/foo":"bar2"}}
` {
				t.Fatalf("unexpected gzip file: %s", string(bytes))
			}
		})
		t.Run("PUT mapped files in a stage and load specified files into LATEST_ACTIVE", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			f, err := os.CreateTemp("", "zip")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			testLayer.db.(*testDB).NewTmpFile = func(ds string) (*os.File, func(), error) {
				return f, func() {}, err
			}
			defer os.Remove(f.Name())

			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{{
				DatasetName: "potatoes",
				SourceConfig: map[string]any{
					TableName:   "potatoe",
					Schema:      "SFS2",
					Database:    "SFDB2",
					LatestTable: true,
				},
				IncomingMappingConfig: &common_datalayer.IncomingMappingConfig{
					MapNamed: false,
					PropertyMappings: []*common_datalayer.EntityToItemPropertyMapping{{
						EntityProperty: "foo",
						Property:       "foo",
						Datatype:       "varchar",
					}, {
						EntityProperty: "ok",
						Property:       "ok",
						Datatype:       "boolean",
					}, {
						EntityProperty: "num",
						Property:       "num",
						Datatype:       "integer",
					}, {
						EntityProperty: "baz",
						Property:       "baz",
						Datatype:       "varchar",
						IsReference:    true,
					}},
					BaseURI: "http://potatoe/test/",
				},
			}}
			testLayer.UpdateConfiguration(cfg)

			// not checking for actual sql, this is regex and it does like all syntax as is
			mock.ExpectExec(`CREATE STAGE IF NOT EXISTS SFDB2.SFS2.S_POTATOE copy`).
				WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectQuery(fmt.Sprintf(`PUT file://%v`, f.Name())).
				WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			mock.ExpectBegin()
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS SFDB2.SFS2.POTATOE \\( id varchar, recorded integer," +
				" deleted boolean, dataset varchar, foo varchar, ok boolean, num integer, baz varchar \\);").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectExec("CREATE TABLE IF NOT EXISTS SFDB2.SFS2.POTATOE_LATEST \\( " +
				"id varchar, recorded integer, deleted boolean, dataset varchar, foo varchar, ok boolean, num integer, baz varchar \\);",
			).WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectQuery("COPY INTO SFDB2.SFS2.POTATOE\\(id, recorded, deleted, dataset, foo, ok, num, baz\\) FROM \\( " +
				"SELECT \\$1:id::varchar, \\d+::integer, coalesce\\(\\$1:deleted::boolean, false\\), 'potatoes'::varchar, " +
				"\\$1:props:\"foo\"::varchar as foo, " +
				"\\$1:props:\"ok\"::boolean as ok, " +
				"\\$1:props:\"num\"::integer as num, " +
				"\\$1:refs:\"baz\"::varchar as baz " +
				"FROM @SFDB2.SFS2.S_POTATOE\\) FILE_FORMAT = \\(TYPE='json' COMPRESSION=GZIP\\) FILES = \\('zip.*'\\);",
			).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			mock.ExpectQuery("MERGE INTO SFDB2.SFS2.POTATOE_LATEST AS latest USING \\( SELECT " +
				"\\$1:id::varchar as id, \\d+::integer as recorded, coalesce\\(\\$1:deleted::boolean, false\\) as deleted, " +
				"'potatoes'::varchar as dataset, \\$1:props:\"foo\"::varchar as foo, \\$1:props:\"ok\"::boolean as ok, " +
				"\\$1:props:\"num\"::integer as num, \\$1:refs:\"baz\"::varchar as baz " +
				"FROM @SFDB2.SFS2.S_POTATOE \\(PATTERN => '.*\\(zip.*\\)'\\) \\) AS src " +
				"ON latest.id = src.id WHEN MATCHED THEN UPDATE SET latest.recorded = src.recorded, " +
				"latest.deleted = src.deleted, latest.dataset = src.dataset, latest.entity = src.entity " +
				"WHEN NOT MATCHED THEN INSERT \\(id, recorded, deleted, dataset, foo, ok, num, baz\\) VALUES \\(" +
				"src.id, src.recorded, src.deleted, src.dataset, src.entity\\);",
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
			if err != nil {
				t.Fatalf("failed to post entities: %v", err)
			}
			if res.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", res.StatusCode)
			}
			f2, err := os.Open(f.Name())
			if err != nil {
				t.Fatalf("failed to open temp file: %v", err)
			}
			r, err := gzip.NewReader(f2)
			if err != nil {
				t.Fatalf("failed to create gzip reader: %v", err)
			}
			bytes, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read gzip file: %v", err)
			}
			println(string(bytes))
			expected := `{"id":"http://snowflake/foo/1","refs":{"http://snowflake/foo/baz":"http://snowflake/bar/hello"},"props":{"http://snowflake/foo/foo":"bar","http://snowflake/foo/num":12,"http://snowflake/foo/ok":true}}
{"id":"http://snowflake/foo/2","refs":{"http://snowflake/foo/baz":["http://snowflake/bar/hi","http://snowflake/bar/bye"]},"props":{"http://snowflake/foo/foo":"bar2"}}
{"id":"http://snowflake/foo/3","recorded":126456789123,"deleted":true,"refs":{"http://snowflake/foo/baz":"http://snowflake/bar/hello"},"props":{"http://snowflake/foo/foo":"bar3","http://snowflake/foo/num":12,"http://snowflake/foo/ok":true}}
`

			if string(bytes) != expected {
				t.Fatalf("unexpected gzip file: \n\n%s. wanted \n\n%s", string(bytes), expected)
			}
		})
		t.Run("fullsync without LATEST_ACTIVE", func(t *testing.T) {
			setup()
			t.Cleanup(cleanup)
			f, err := os.CreateTemp("", "zip")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			testLayer.db.(*testDB).NewTmpFile = func(ds string) (*os.File, func(), error) {
				return f, func() {}, err
			}
			defer os.Remove(f.Name())

			mock.ExpectQuery("SHOW STAGES LIKE '%POTATOE_FSID_%' IN TESTDB.TESTSCHEMA;select .* FROM table\\(RESULT_SCAN\\(LAST_QUERY_ID\\(\\)\\)\\)").
				WillReturnRows(sqlmock.NewRows([]string{}))
			// not checking for actual sql, this is regex and it does like all syntax as is
			mock.ExpectExec(`CREATE STAGE IF NOT EXISTS TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234`).WillReturnResult(sqlmock.NewResult(1, 1))

			//// new conn
			//mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON'").WillReturnResult(sqlmock.NewResult(1, 1))
			//mock.ExpectExec("USE SECONDARY ROLES ALL").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectQuery(fmt.Sprintf(`PUT file://%v`, f.Name())).
				WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			//// new conn
			//mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON'").WillReturnResult(sqlmock.NewResult(1, 1))
			//mock.ExpectExec("USE SECONDARY ROLES ALL").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectBegin()
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234 \\(id varchar, recorded integer," +
				" deleted boolean, dataset varchar, entity variant\\);").WillReturnResult(sqlmock.NewResult(1, 1))

			//COPY INTO TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234(id, recorded, deleted, dataset, entity) FROM (
			//SELECT $1:id::varchar, 1731008161359419314::integer, coalesce($1:deleted::boolean, false), 'testdb.testschema.potatoe'::varchar,
			//$1::variant as entity FROM @TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234
			//) FILE_FORMAT = (TYPE='json' COMPRESSION=GZIP);

			mock.ExpectQuery("COPY INTO TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234\\(id, recorded, deleted, dataset, entity\\) FROM \\( " +
				"SELECT \\$1:id::varchar, \\d+::integer, coalesce\\(\\$1:deleted::boolean, false\\), 'testdb.testschema.potatoe'::varchar, " +
				"\\$1::variant as entity FROM @TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234" +
				"\\) FILE_FORMAT = \\(TYPE='json' COMPRESSION=GZIP\\);",
			).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			mock.ExpectExec("ALTER STAGE TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234 RENAME TO TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234_DONE").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec("ALTER TABLE TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234 SWAP WITH POTATOE").WillReturnError(fmt.Errorf("error"))
			mock.ExpectExec("ALTER TABLE TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234 RENAME TO POTATOE").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()

			postBody := strings.NewReader(`[{"id": "@context", "namespaces": {
"x": "http://snowflake/foo/",
"y": "http://snowflake/bar/",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}},
{"id": "x:1", "recorded": 1456456456, "props": {"x:foo": "bar"}, "refs": {"x:baz": "y:hello"}},
{"id": "x:2", "recorded": 1456456457, "props": {"x:foo": "bar2"}, "refs":{"x:baz": ["y:hi", "y:bye"]}}]
`)
			req, err := http.NewRequest("POST", "http://localhost:17866/datasets/potatoe/entities", postBody)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("universal-data-api-full-sync-start", "true")
			req.Header.Set("universal-data-api-full-sync-end", "true")
			req.Header.Set("universal-data-api-full-sync-id", "1234")

			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("failed to post entities: %v", err)
			}
			if res.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", res.StatusCode)
			}
			f2, err := os.Open(f.Name())
			if err != nil {
				t.Fatalf("failed to open temp file: %v", err)
			}
			r, err := gzip.NewReader(f2)
			if err != nil {
				t.Fatalf("failed to create gzip reader: %v", err)
			}
			bytes, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read gzip file: %v", err)
			}
			// println(string(bytes))
			if string(bytes) != `{"id":"http://snowflake/foo/1","recorded":1456456456,"refs":{"http://snowflake/foo/baz":"http://snowflake/bar/hello"},"props":{"http://snowflake/foo/foo":"bar"}}
{"id":"http://snowflake/foo/2","recorded":1456456457,"refs":{"http://snowflake/foo/baz":["http://snowflake/bar/hi","http://snowflake/bar/bye"]},"props":{"http://snowflake/foo/foo":"bar2"}}
` {
				t.Fatalf("unexpected gzip file: %s", string(bytes))
			}
		})
		t.Run("fullsync with LATEST_ACTIVE", func(t *testing.T) {

			setup()
			t.Cleanup(cleanup)
			f, err := os.CreateTemp("", "zip")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			testLayer.db.(*testDB).NewTmpFile = func(ds string) (*os.File, func(), error) {
				return f, func() {}, err
			}
			defer os.Remove(f.Name())
			cfg.DatasetDefinitions = []*common_datalayer.DatasetDefinition{{
				DatasetName: "potatoe",
				SourceConfig: map[string]any{
					TableName:   "potatoe",
					Schema:      "TESTSCHEMA",
					Database:    "TESTDB",
					LatestTable: true,
				},
			}}
			testLayer.UpdateConfiguration(cfg)
			mock.ExpectQuery("SHOW STAGES LIKE '%POTATOE_FSID_%' IN TESTDB.TESTSCHEMA;select .* FROM table\\(RESULT_SCAN\\(LAST_QUERY_ID\\(\\)\\)\\)").
				WillReturnRows(sqlmock.NewRows([]string{}))
			// not checking for actual sql, this is regex and it does like all syntax as is
			mock.ExpectExec(`CREATE STAGE IF NOT EXISTS TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234`).WillReturnResult(sqlmock.NewResult(1, 1))

			//// new conn
			//mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON'").WillReturnResult(sqlmock.NewResult(1, 1))
			//mock.ExpectExec("USE SECONDARY ROLES ALL").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectQuery(fmt.Sprintf(`PUT file://%v`, f.Name())).
				WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			//// new conn
			//mock.ExpectExec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON'").WillReturnResult(sqlmock.NewResult(1, 1))
			//mock.ExpectExec("USE SECONDARY ROLES ALL").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectBegin()
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234 \\(id varchar, recorded integer," +
				" deleted boolean, dataset varchar, entity variant\\);").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234_LATEST \\(id varchar, recorded integer," +
				" deleted boolean, dataset varchar, entity variant\\);").WillReturnResult(sqlmock.NewResult(1, 1))

			mock.ExpectQuery("COPY INTO TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234\\(id, recorded, deleted, dataset, entity\\) FROM \\( " +
				"SELECT \\$1:id::varchar, \\d+::integer, coalesce\\(\\$1:deleted::boolean, false\\), 'potatoe'::varchar, " +
				"\\$1::variant as entity FROM @TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234" +
				"\\) FILE_FORMAT = \\(TYPE='json' COMPRESSION=GZIP\\);",
			).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			mock.ExpectQuery("MERGE INTO TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234_LATEST AS latest " +
				"USING \\( SELECT \\$1:id::varchar as id, " +
				"\\d+::integer as recorded, " +
				"coalesce\\(\\$1:deleted::boolean, false\\) as deleted, " +
				"'potatoe'::varchar as dataset, " +
				"\\$1::variant as entity " +
				"FROM @TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234 \\) AS src " +
				"ON latest.id = src.id WHEN MATCHED THEN UPDATE SET " +
				"latest.recorded = src.recorded, latest.deleted = src.deleted, latest.dataset = src.dataset, " +
				"latest.entity = src.entity " +
				"WHEN NOT MATCHED THEN INSERT \\(id, recorded, deleted, dataset, entity\\) " +
				"VALUES \\(src.id, src.recorded, src.deleted, src.dataset, src.entity\\);",
			).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("OK"))

			mock.ExpectExec("ALTER STAGE TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234 RENAME TO TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234_DONE").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec("ALTER TABLE TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234 SWAP WITH POTATOE").WillReturnError(fmt.Errorf("error"))
			mock.ExpectExec("ALTER TABLE TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234 RENAME TO POTATOE").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec("ALTER TABLE TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234_LATEST SWAP WITH POTATOE_LATEST").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec("DROP TABLE TESTDB.TESTSCHEMA.S_POTATOE_FSID_1234_LATEST").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()

			postBody := strings.NewReader(`[{"id": "@context", "namespaces": {
"x": "http://snowflake/foo/",
"y": "http://snowflake/bar/",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}},
{"id": "x:1", "recorded": 1456456456, "props": {"x:foo": "bar"}, "refs": {"x:baz": "y:hello"}},
{"id": "x:2", "recorded": 1456456457, "props": {"x:foo": "bar2"}, "refs":{"x:baz": ["y:hi", "y:bye"]}}]
`)
			req, err := http.NewRequest("POST", "http://localhost:17866/datasets/potatoe/entities", postBody)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("universal-data-api-full-sync-start", "true")
			req.Header.Set("universal-data-api-full-sync-end", "true")
			req.Header.Set("universal-data-api-full-sync-id", "1234")

			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("failed to post entities: %v", err)
			}
			if res.StatusCode != 200 {
				t.Fatalf("expected 200, got %d", res.StatusCode)
			}
			f2, err := os.Open(f.Name())
			if err != nil {
				t.Fatalf("failed to open temp file: %v", err)
			}
			r, err := gzip.NewReader(f2)
			if err != nil {
				t.Fatalf("failed to create gzip reader: %v", err)
			}
			bytes, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read gzip file: %v", err)
			}
			// println(string(bytes))
			if string(bytes) != `{"id":"http://snowflake/foo/1","recorded":1456456456,"refs":{"http://snowflake/foo/baz":"http://snowflake/bar/hello"},"props":{"http://snowflake/foo/foo":"bar"}}
{"id":"http://snowflake/foo/2","recorded":1456456457,"refs":{"http://snowflake/foo/baz":["http://snowflake/bar/hi","http://snowflake/bar/bye"]},"props":{"http://snowflake/foo/foo":"bar2"}}
` {
				t.Fatalf("unexpected gzip file: %s", string(bytes))
			}
		})
	})
}
