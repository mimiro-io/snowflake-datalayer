package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	common_datalayer "github.com/mimiro-io/common-datalayer"
	"github.com/mimiro-io/datahub-snowflake-datalayer/internal/api"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	gsf "github.com/snowflakedb/gosnowflake"
)

func (sf *Snowflake) putEntities(dataset string, stage string, entities []*egdm.Entity) ([]string, error) {
	return withRefresh(sf, func() ([]string, error) {
		// we will handle snowflake in 2 steps, first write each batch as a zipped ndjson file
		file, cleanTmpFile, err := sf.NewTmpFile(dataset)
		if err != nil {
			return nil, err
		}
		defer cleanTmpFile()

		err = api.WriteAsGzippedNDJson(file, entities, dataset)
		if err != nil {
			return nil, err
		}
		err = file.Close()
		if err != nil {
			return nil, err
		}

		// then upload to staging
		files := make([]string, 0)
		sf.log.Debug().Msgf("Uploading %s", file.Name())
		ctx := context.Background()
		_, err = WithConn(p, ctx, func(conn *sql.Conn) (any, error) {
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

			return nil, nil
		})
		if err != nil {
			return files, err
		}
		files = append(files, filepath.Base(file.Name()))
		return files, nil
	})
}

func (sf *Snowflake) getStage(fsId string, dataset string) string {
	dsName := strings.ToUpper(strings.ReplaceAll(dataset, ".", "_"))
	stage := fmt.Sprintf("%s.%s.S_%s", strings.ToUpper(sf.cfg.SnowflakeDB), strings.ToUpper(sf.cfg.SnowflakeSchema), dsName)
	fsSuffix := fmt.Sprintf("_FSID_%s", fsId)
	stage = stage + fsSuffix
	return stage
}

func (sf *Snowflake) mkStage(fsId, dataset string, mapping *common_datalayer.DatasetDefinition) (string, error) {
	return withRefresh(sf, func() (string, error) {
		ctx := context.Background()
		return WithConn(p, ctx, func(conn *sql.Conn) (string, error) {
			dbName, schemaName, dsName := sf.tableParts(mapping)
			// construct base stage name from dataset name plus either mapping config or app config as fallback
			stage := fmt.Sprintf("%s.%s.S_%s", dbName, schemaName, dsName)

			// if full sync id is provided, append it to stage name. also do some cleanup for previous full sync stages
			if fsId != "" {
				sf.log.Info().Msg("Full sync requested for " + dsName + ", id " + fsId)
				fsSuffix := fmt.Sprintf("_FSID_%s", fsId)
				query := "SHOW STAGES LIKE '%" + dsName + "_FSID_%' IN " + dbName + "." + schemaName
				query = query + ";select \"name\" FROM table(RESULT_SCAN(LAST_QUERY_ID()))"
				// println(query)
				mctx, err := gsf.WithMultiStatement(ctx, 2)
				if err != nil {
					sf.log.Error().Err(err).Msg("Failed to create multistatement context")
					return "", err
				}
				rows, err := conn.QueryContext(mctx, query)

				defer func() {
					if rows != nil {
						closeErr := rows.Close()
						if closeErr != nil {
							sf.log.Error().Err(closeErr).Msg("Failed to close rows")
						}
					}
				}()
				if err != nil {
					sf.log.Error().Err(err).Msg("Failed to query stages")
					return "", err
				}

				var existingFsStage string
				rows.NextResultSet() // skip to 2nd statement result
				if rows.Next() {
					err = rows.Scan(&existingFsStage)
					if err != nil {
						if !errors.Is(err, sql.ErrNoRows) {
							sf.log.Error().Err(err).Msg("Failed to scan row")
							return "", err
						} else {
							sf.log.Info().Msg("No previous full sync stage found for " + dsName)
						}
					}
					sf.log.Info().
						Msg("Found previous full sync stage " + existingFsStage + ". Dropping it before new full sync")
					stmt := fmt.Sprintf("DROP STAGE %s.%s.%s", dbName, schemaName, existingFsStage)
					_, err = conn.ExecContext(ctx, stmt)
					if err != nil {
						sf.log.Error().Err(err).Str("statement", stmt).Msg("Failed to drop previous full sync stage")
						return "", err
					}
				} else {
					sf.log.Info().Msg("No previous full sync stage found for " + dsName)
				}
				stage = stage + fsSuffix
			}

			// now create stage
			q := fmt.Sprintf(`
	CREATE STAGE IF NOT EXISTS %s
		copy_options = (on_error=ABORT_STATEMENT)
	    file_format = (TYPE='json' STRIP_OUTER_ARRAY = TRUE);
	`, stage)
			sf.log.Trace().Msg(q)
			_, err := conn.ExecContext(ctx, q)
			if err != nil {
				sf.log.Warn().Msg("Failed to create/ensure stage")
				return "", err
			}
			return stage, err
		})
	})
}

func (sf *Snowflake) Load(files []string, batchTimestamp int64, mapping *common_datalayer.DatasetDefinition) error {
	_, err := withRefresh(sf, func() (any, error) {
		return nil, func() error {
			ctx := context.Background()
			_, err := WithConn(p, ctx, func(conn *sql.Conn) (any, error) {
				dbName, schemaName, dsName := sf.tableParts(mapping)
				nameSpace := fmt.Sprintf("%s.%s", dbName, schemaName)
				stage := fmt.Sprintf("%s.S_", nameSpace) + dsName
				tableName := dsName

				tx, err := conn.BeginTx(ctx, nil)
				if err != nil {
					return nil, err
				}
				defer func() {
					_ = tx.Rollback()
				}()

				colNames, columns, colExtractions := api.ColMappings(mapping)
				if _, err := tx.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.%s (
  		id varchar,
		recorded integer,
  		deleted boolean,
  		dataset varchar,
		%s
	);
	`, nameSpace, tableName, columns)); err != nil {
					return nil, err
				}

				fileString := "'" + strings.Join(files, "', '") + "'"

				sf.log.Trace().Msgf("Loading %s", fileString)
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
	`, nameSpace, tableName, colNames, batchTimestamp, mapping.DatasetName, colExtractions, stage, fileString)
				sf.log.Trace().Msg(q)
				if _, err := tx.Query(q); err != nil {
					return nil, err
				}
				sf.log.Trace().Msgf("Done with %s", files)
				return nil, tx.Commit()
			})
			return err
		}() // end of func
	})
	return err
}

func (sf *Snowflake) LoadStage(stage string, batchTimestamp int64, mapping *common_datalayer.DatasetDefinition) error {
	_, err := withRefresh(sf, func() (any, error) {
		ctx := context.Background()
		return WithConn(p, ctx, func(conn *sql.Conn) (any, error) {
			return nil, func() error {
				loadTableName := stage

				_, _, dsName := sf.tableParts(mapping)
				tableName := dsName

				tx, err := conn.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer func() {
					_ = tx.Rollback()
				}()
				colNames, columns, colExtractions := api.ColMappings(mapping)
				smt := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
  		id varchar,
		recorded integer,
  		deleted boolean,
  		dataset varchar,
  		%s);
	`, loadTableName, columns)

				// println("\n", smt)
				if _, err2 := tx.Exec(smt); err2 != nil {
					return err2
				}

				sf.log.Trace().Msgf("Loading fs table %s", loadTableName)
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
	`, loadTableName, colNames, batchTimestamp, mapping.DatasetName, colExtractions, stage)
				sf.log.Trace().Msg(q)
				if _, err2 := tx.Query(q); err2 != nil {
					return err2
				}

				_, err = tx.Exec(fmt.Sprintf("ALTER STAGE %s RENAME TO %s", stage, stage+"_DONE"))
				if err != nil {
					return err
				}
				sf.log.Trace().Msgf("Done with %s. now swapping with %s", loadTableName, tableName)
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
				return tx.Commit()
			}() // end of func
		})
	})
	return err
}
