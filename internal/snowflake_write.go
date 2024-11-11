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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	gsf "github.com/snowflakedb/gosnowflake"
)

func (sf *SfDB) putEntities(ctx context.Context, datasetName string, stage string, entities []*egdm.Entity) ([]string, error) {
	conn := ctx.Value(Connection).(*sql.Conn)
	file, cleanTmpFile, err := sf.NewTmpFile(datasetName)
	if err != nil {
		return nil, err
	}
	defer cleanTmpFile()

	err = WriteAsGzippedNDJson(file, entities, datasetName)
	if err != nil {
		return nil, err
	}
	err = file.Close()
	if err != nil {
		return nil, err
	}

	// then upload to staging
	files := make([]string, 0)
	sf.logger.Debug(fmt.Sprintf("Uploading %s", file.Name()))
	rows, err2 := conn.QueryContext(ctx,
		fmt.Sprintf("PUT file://%s @%s auto_compress=false overwrite=false", file.Name(), stage),
	)
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	if err2 != nil {
		return nil, err2
	}

	files = append(files, filepath.Base(file.Name()))
	return files, nil
}

func (sf *SfDB) tableParts(mapping *common.DatasetDefinition) (string, string, string) {
	dsName := strings.ToUpper(strings.ReplaceAll(mapping.DatasetName, ".", "_"))
	if ds, ok := mapping.SourceConfig[TableName]; ok {
		dsName = strings.ToUpper(ds.(string))
	}
	dbName := strings.ToUpper(sysConfStr(sf.conf, SnowflakeDB))
	if db, ok := mapping.SourceConfig[Database]; ok {
		dbName = strings.ToUpper(db.(string))
	}
	schemaName := strings.ToUpper(sysConfStr(sf.conf, SnowflakeSchema))
	if schema, ok := mapping.SourceConfig[Schema]; ok {
		schemaName = strings.ToUpper(schema.(string))
	}
	return dbName, schemaName, dsName
}

func (sf *SfDB) getFsStage(syncID string, datasetDefinition *common.DatasetDefinition) string {
	dbName, schemaName, dsName := sf.tableParts(datasetDefinition)
	stage := fmt.Sprintf("%s.%s.S_%s", dbName, schemaName, dsName)
	fsSuffix := fmt.Sprintf("_FSID_%s", syncID)
	stage = stage + fsSuffix
	return stage
}

func (sf *SfDB) mkStage(ctx context.Context, syncID string, datasetName string, datasetDefinition *common.DatasetDefinition) (string, error) {
	conn := ctx.Value(Connection).(*sql.Conn)
	dbName, schemaName, dsName := sf.tableParts(datasetDefinition)
	// construct base stage name from dataset name plus either mapping config or app config as fallback
	stage := fmt.Sprintf("%s.%s.S_%s", dbName, schemaName, dsName)

	// if full sync id is provided, append it to stage name. also do some cleanup for previous full sync stages
	if syncID != "" {
		sf.logger.Info("Full sync requested for " + dsName + ", id " + syncID)
		fsSuffix := fmt.Sprintf("_FSID_%s", syncID)
		query := "SHOW STAGES LIKE '%" + dsName + "_FSID_%' IN " + dbName + "." + schemaName
		query = query + ";select \"name\" FROM table(RESULT_SCAN(LAST_QUERY_ID()))"
		// println(query)
		mctx, err := gsf.WithMultiStatement(ctx, 2)
		if err != nil {
			sf.logger.Error("Failed to create multistatement context", "error", err)
			return "", err
		}
		rows, err := conn.QueryContext(mctx, query)

		defer func() {
			if rows != nil {
				closeErr := rows.Close()
				if closeErr != nil {
					sf.logger.Error("Failed to close rows", "error", err)
				}
			}
		}()
		if err != nil {
			sf.logger.Error("Failed to query stages", "error", err)
			return "", err
		}

		var existingFsStage string
		rows.NextResultSet() // skip to 2nd statement result
		if rows.Next() {
			err = rows.Scan(&existingFsStage)
			if err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					sf.logger.Error("Failed to scan rows", "error", err)
					return "", err
				} else {
					sf.logger.Info("No previous full sync stage found for " + dsName)
				}
			}
			sf.logger.Info("Found previous full sync stage " + existingFsStage + ". Dropping it before new full sync")
			stmt := fmt.Sprintf("DROP STAGE %s.%s.%s", dbName, schemaName, existingFsStage)
			_, err = conn.ExecContext(ctx, stmt)
			if err != nil {
				sf.logger.Error("Failed to drop previous full sync stage", "error", err, "statement", stmt)
				return "", err
			}
		} else {
			sf.logger.Info("No previous full sync stage found for " + dsName)
		}
		stage = stage + fsSuffix
	}

	// now create stage
	q := fmt.Sprintf(`
	CREATE STAGE IF NOT EXISTS %s
		copy_options = (on_error=ABORT_STATEMENT)
	    file_format = (TYPE='json' STRIP_OUTER_ARRAY = TRUE);
	`, stage)
	sf.logger.Debug(q)
	_, err := conn.ExecContext(ctx, q)
	if err != nil {
		sf.logger.Warn("Failed to create/ensure stage", "query", q)
		return "", err
	}
	return stage, err
}

func (sf *SfDB) loadStage(ctx context.Context, stage string, loadTime int64, datasetDefinition *common.DatasetDefinition) error {
	conn := ctx.Value(Connection).(*sql.Conn)
	loadTableName := stage

	_, _, dsName := sf.tableParts(datasetDefinition)
	tableName := dsName

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	colNames, columns, colExtractions, colAssignments, srcColExtractions := ColMappings(datasetDefinition)
	// println("\n", smt)
	if _, err2 := tx.Exec(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (id varchar, recorded integer, deleted boolean, dataset varchar, %s);`,
		loadTableName, columns)); err2 != nil {
		return err2
	}
	if sf.HasLatestActive(datasetDefinition) {
		if _, err2 := tx.Exec(fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s_LATEST (id varchar, recorded integer, deleted boolean, dataset varchar, %s);`,
			loadTableName, columns)); err2 != nil {
			return err2
		}
	}

	sf.logger.Debug(fmt.Sprintf("Loading fs table %s", loadTableName))
	q := fmt.Sprintf(`
	COPY INTO %s(id, recorded, deleted, dataset, %s)
	    FROM (
	    	SELECT
 			$1:id::varchar,
			%v::integer,
 			coalesce($1:deleted::boolean, false),
			'%s'::varchar,
 			%s
	    	FROM @%s)
	FILE_FORMAT = (TYPE='json' COMPRESSION=GZIP);
	`, loadTableName, colNames, loadTime, datasetDefinition.DatasetName, colExtractions, stage)
	// sf.logger.Debug(q)
	if _, err2 := tx.Query(q); err2 != nil {
		return err2
	}

	if sf.HasLatestActive(datasetDefinition) {
		q = fmt.Sprintf(`
	MERGE INTO %s_LATEST AS latest
	USING (
		SELECT
		$1:id::varchar as id,
		%v::integer as recorded,
		coalesce($1:deleted::boolean, false) as deleted,
		'%s'::varchar as dataset,
		%s
		FROM @%s
	) AS src
	ON latest.id = src.id
	WHEN MATCHED THEN
		UPDATE SET
			latest.recorded = src.recorded,
			latest.deleted = src.deleted,
			latest.dataset = src.dataset,
			%s
	WHEN NOT MATCHED THEN
		INSERT (id, recorded, deleted, dataset, %s)
		VALUES (src.id, src.recorded, src.deleted, src.dataset, %s);
`, loadTableName, loadTime, datasetDefinition.DatasetName, colExtractions,
			stage, colAssignments, colNames, srcColExtractions)

		if _, err := tx.Query(q); err != nil {
			return err
		}
	}
	_, err = tx.Exec(fmt.Sprintf("ALTER STAGE %s RENAME TO %s", stage, stage+"_DONE"))
	if err != nil {
		return err
	}
	sf.logger.Debug(fmt.Sprintf("Done with %s. now swapping with %s", loadTableName, tableName))
	_, err = tx.Exec(fmt.Sprintf("ALTER TABLE %s SWAP WITH %s", loadTableName, tableName))
	if err != nil {
		// if swap fails, this could be the first full sync and tableName does not exist yet. so try rename
		_, err = tx.Exec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", loadTableName, tableName))
		if err != nil {
			return err
		}
	} else {
		// if swap was success, remove load table (which is now the old table)
		_, err = tx.Exec(fmt.Sprintf("DROP TABLE %s", loadTableName))
		if err != nil {
			return err
		}
	}

	if sf.HasLatestActive(datasetDefinition) {
		_, err = tx.Exec(fmt.Sprintf("ALTER TABLE %s_LATEST SWAP WITH %s_LATEST", loadTableName, tableName))
		if err != nil {
			// if swap fails, this could be the first full sync and tableName does not exist yet. so try rename
			_, err = tx.Exec(fmt.Sprintf("ALTER TABLE %s_LATEST RENAME TO %s_LATEST", loadTableName, tableName))
			if err != nil {
				return err
			}
		} else {
			// if swap was success, remove load table (which is now the old table)
			_, err = tx.Exec(fmt.Sprintf("DROP TABLE %s_LATEST", loadTableName))
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (sf *SfDB) loadFilesInStage(ctx context.Context, files []string, stage string, loadTime int64, datasetDefinition *common.DatasetDefinition) error {
	conn := ctx.Value(Connection).(*sql.Conn)
	dbName, schemaName, dsName := sf.tableParts(datasetDefinition)
	nameSpace := fmt.Sprintf("%s.%s", dbName, schemaName)
	tableName := dsName

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	colNames, columns, colExtractions, colAssignments, srcColExtractions := ColMappings(datasetDefinition)
	if _, err := tx.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.%s ( id varchar, recorded integer, deleted boolean, dataset varchar, %s );
	`, nameSpace, tableName, columns)); err != nil {
		return err
	}

	if sf.HasLatestActive(datasetDefinition) {
		if _, err := tx.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.%s_LATEST ( id varchar, recorded integer, deleted boolean, dataset varchar, %s );
	`, nameSpace, tableName, columns)); err != nil {
			return err
		}
	}
	fileString := "'" + strings.Join(files, "', '") + "'"

	sf.logger.Debug(fmt.Sprintf("Loading %s", fileString))
	q := fmt.Sprintf(`
	COPY INTO %s.%s(id, recorded, deleted, dataset, %s)
	    FROM (
	    	SELECT
 			$1:id::varchar,
			%v::integer,
 			coalesce($1:deleted::boolean, false),
			'%s'::varchar,
			%s
	    	FROM @%s)
	FILE_FORMAT = (TYPE='json' COMPRESSION=GZIP)
	FILES = (%s);
	`, nameSpace, tableName, colNames, loadTime, datasetDefinition.DatasetName, colExtractions, stage, fileString)

	if _, err := tx.Query(q); err != nil {
		return err
	}

	if sf.HasLatestActive(datasetDefinition) {
		q = fmt.Sprintf(`
	MERGE INTO %s.%s_LATEST AS latest
	USING (
		SELECT
		$1:id::varchar as id,
		%v::integer as recorded,
		coalesce($1:deleted::boolean, false) as deleted,
		'%s'::varchar as dataset,
		%s
		FROM @%s (PATTERN => '.*(%s)')
	) AS src
	ON latest.id = src.id
	WHEN MATCHED THEN
		UPDATE SET
			latest.recorded = src.recorded,
			latest.deleted = src.deleted,
			latest.dataset = src.dataset,
			%s
	WHEN NOT MATCHED THEN
		INSERT (id, recorded, deleted, dataset, %s)
		VALUES (src.id, src.recorded, src.deleted, src.dataset, %s);
`, nameSpace, tableName, loadTime, datasetDefinition.DatasetName, colExtractions,
			stage, strings.Join(files, "|"), colAssignments, colNames, srcColExtractions)
		if _, err := tx.Query(q); err != nil {
			return err
		}
	}
	return tx.Commit()
}
