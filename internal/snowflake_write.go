package internal

import (
	"compress/gzip"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mimiro-io/internal-go-util/pkg/uda"

	"github.com/bfontaine/jsons"
	"github.com/rs/zerolog"
	gsf "github.com/snowflakedb/gosnowflake"
)

type pool struct {
	db *sql.DB
}

var p *pool

type Snowflake struct {
	cfg        *Config
	log        zerolog.Logger
	NewTmpFile func(dataset string) (*os.File, error, func()) // file, error, function to cleanup file
	lock       sync.Mutex
}

func NewSnowflake(cfg *Config) (*Snowflake, error) {
	connectionString := "%s:%s@%s"
	if cfg.PrivateCert != "" || cfg.SnowflakePrivateKey != "" {
		data, err := base64.StdEncoding.DecodeString(cfg.SnowflakePrivateKey)
		if err != nil || len(data) == 0 {
			data, err = base64.StdEncoding.DecodeString(cfg.PrivateCert)
			if err != nil {
				return nil, err
			}
		}

		parsedKey8, err := x509.ParsePKCS8PrivateKey(data)
		if err != nil {
			return nil, err
		}
		parsedKey := parsedKey8.(*rsa.PrivateKey)
		// privateKey, err := jwt.ParseRSAPrivateKeyFromPEMWithPassword(data, cfg.CertPassword)
		config := &gsf.Config{
			Account:       cfg.SnowflakeAccount,
			User:          cfg.SnowflakeUser,
			Database:      cfg.SnowflakeDB,
			Schema:        cfg.SnowflakeSchema,
			Warehouse:     cfg.SnowflakeWarehouse,
			Region:        "eu-west-1",
			Authenticator: gsf.AuthTypeJwt,
			PrivateKey:    parsedKey,
		}
		s, err := gsf.DSN(config)
		if err != nil {
			return nil, err
		}
		connectionString = s
	} else {
		if cfg.SnowflakeURI != "" {
			connectionString = fmt.Sprintf(connectionString, cfg.SnowflakeUser, cfg.SnowflakePassword, cfg.SnowflakeURI)
		} else {
			uri := fmt.Sprintf("%s/%s/%s", cfg.SnowflakeAccount, cfg.SnowflakeDB, cfg.SnowflakeSchema)
			connectionString = fmt.Sprintf(connectionString, cfg.SnowflakeUser, cfg.SnowflakePassword, uri)
		}
	}

	if p == nil || p.db == nil {
		LOG.Info().Msg("opening db")
		db, err := sql.Open("snowflake", connectionString)
		// snowflake tokens time out, after 4 hours with default session settings.
		// if we do not evict idle connections, we will get errors after 4 hours
		db.SetConnMaxIdleTime(30 * time.Second)
		db.SetConnMaxLifetime(1 * time.Hour)

		if err != nil {
			return nil, err
		}

		p = &pool{
			db: db,
		}
	}
	LOG.Info().Msg(fmt.Sprintf("start or refresh happening. database connection stats: %+v", p.db.Stats()))
	_, err := p.db.Exec("ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON';")
	if err != nil {
		return nil, err
	}
	return &Snowflake{
		cfg:        cfg,
		log:        LOG.With().Str("logger", "snowflake").Logger(),
		NewTmpFile: newTmpFileWriter,
	}, nil
}

// EnsureStageAndPut returns a list of already uploaded files to be loaded into snowflake, Load must be called subsequently to finnish the process
func (sf *Snowflake) EnsureStageAndPut(ctx context.Context, dataset string, entityContext *uda.Context, entities []*Entity) ([]string, error) {
	// by starting with the sql before creating any files, then if no session is valid, we should prevent creating files we won't use
	// as it fails early
	stage, err := sf.mkStage("", dataset)
	if err != nil {
		return nil, err
	}

	files, err2 := sf.putEntities(dataset, stage, entities, entityContext)
	if err2 != nil {
		return nil, err2
	}

	return files, nil
}

func newTmpFileWriter(dataset string) (*os.File, error, func()) {
	file, err := os.CreateTemp("", dataset)
	if err != nil {
		return nil, err, nil
	}
	finally := func() { os.Remove(file.Name()) }
	return file, nil, finally
}

func (sf *Snowflake) putEntities(dataset string, stage string, entities []*Entity, entityContext *uda.Context) ([]string, error) {
	return withRefresh(sf, func() ([]string, error) {
		// we will handle snowflake in 2 steps, first write each batch as a ndjson file
		file, err, cleanTmpFile := sf.NewTmpFile(dataset)
		if err != nil {
			return nil, err
		}
		defer cleanTmpFile()

		err = sf.gzippedNDJson(file, entities, entityContext, dataset)
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
		if _, err := p.db.Query(fmt.Sprintf("PUT file://%s @%s auto_compress=false overwrite=false", file.Name(), stage)); err != nil {
			return files, err
		}
		files = append(files, filepath.Base(file.Name()))
		return files, nil
	})
}

func (sf *Snowflake) gzippedNDJson(file io.Writer, entities []*Entity, entityContext *uda.Context, dataset string) error {
	zipWriter := gzip.NewWriter(file)
	j := jsons.NewWriter(zipWriter)
	for _, entity := range entities {
		entity.ID = uda.ToURI(entityContext, entity.ID)
		entity.Dataset = dataset

		// do references
		newRefs := make(map[string]any)
		for refKey, refValue := range entity.References {
			// we need to do both key and value replacing
			key := uda.ToURI(entityContext, refKey)
			switch values := refValue.(type) {
			case []any:
				var newValues []string
				for _, val := range values {
					newValues = append(newValues, uda.ToURI(entityContext, val.(string)))
				}
				newRefs[key] = newValues
			case []string:
				var newValues []string
				for _, val := range values {
					newValues = append(newValues, uda.ToURI(entityContext, val))
				}
				newRefs[key] = newValues
			default:
				newRefs[key] = uda.ToURI(entityContext, refValue.(string))
			}
		}
		entity.References = newRefs

		// do preferences
		newProps := make(map[string]any)
		for refKey, refValue := range entity.Properties {
			key := uda.ToURI(entityContext, refKey)
			newProps[key] = refValue
		}
		entity.Properties = newProps

		err := j.Add(entity)
		if err != nil {
			return err
		}
	}

	// flush and close
	return zipWriter.Close()
}

func (sf *Snowflake) getStage(fsId string, dataset string) string {
	dsName := strings.ToUpper(strings.ReplaceAll(dataset, ".", "_"))
	stage := fmt.Sprintf("%s.%s.S_%s", strings.ToUpper(sf.cfg.SnowflakeDB), strings.ToUpper(sf.cfg.SnowflakeSchema), dsName)
	fsSuffix := fmt.Sprintf("_FSID_%s", fsId)
	stage = stage + fsSuffix
	return stage
}

func (sf *Snowflake) mkStage(fsId, dataset string) (string, error) {
	return withRefresh(sf, func() (string, error) {
		dsName := strings.ToUpper(strings.ReplaceAll(dataset, ".", "_"))
		stage := fmt.Sprintf("%s.%s.S_%s", strings.ToUpper(sf.cfg.SnowflakeDB), strings.ToUpper(sf.cfg.SnowflakeSchema), dsName)

		if fsId != "" {
			sf.log.Info().Msg("Full sync requested for " + dsName + ", id " + fsId)
			fsSuffix := fmt.Sprintf("_FSID_%s", fsId)
			query := "SHOW STAGES LIKE '%" + dsName + "_FSID_%' IN " + sf.cfg.SnowflakeDB + "." + sf.cfg.SnowflakeSchema
			query = query + ";select \"name\" FROM table(RESULT_SCAN(LAST_QUERY_ID()))"
			// println(query)
			ctx, err := gsf.WithMultiStatement(context.Background(), 2)
			if err != nil {
				sf.log.Error().Err(err).Msg("Failed to create multistatement context")
				return "", err
			}
			rows, err := p.db.QueryContext(ctx, query)
			defer rows.Close()
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
				sf.log.Info().Msg("Found previous full sync stage " + existingFsStage + ". Dropping it before new full sync")
				stmt := fmt.Sprintf("DROP STAGE %s.%s.%s", sf.cfg.SnowflakeDB, sf.cfg.SnowflakeSchema, existingFsStage)
				_, err = p.db.Exec(stmt)
				if err != nil {
					sf.log.Error().Err(err).Str("statement", stmt).Msg("Failed to drop previous full sync stage")
					return "", err
				}
			} else {
				sf.log.Info().Msg("No previous full sync stage found for " + dsName)
			}
			stage = stage + fsSuffix
		}

		q := fmt.Sprintf(`
	CREATE STAGE IF NOT EXISTS %s
		copy_options = (on_error=ABORT_STATEMENT)
	    file_format = (TYPE='json' STRIP_OUTER_ARRAY = TRUE);
	`, stage)
		sf.log.Trace().Msg(q)
		_, err := p.db.Exec(q)
		if err != nil {
			sf.log.Warn().Msg("Failed to create/ensure stage")
			return "", err
		}
		return stage, err
	})
}

func withRefresh[T any](sf *Snowflake, f func() (T, error)) (T, error) {
	sf.lock.Lock()
	defer sf.lock.Unlock()
	r, callErr := f()
	if callErr != nil {
		if strings.Contains(callErr.Error(), "390114") {
			sf.log.Info().Msg("Refreshing snowflake connection")
			s, err := NewSnowflake(sf.cfg)
			if err != nil {
				sf.log.Error().Err(err).Msg("Failed to reconnect to snowflake")
				return r, err

			}
			sf = s
			sf.log.Info().Msg("Reconnected to snowflake")
			return f()
		}
		sf.log.Debug().Msg("Not a refresh error")
	}
	return r, callErr
}

func (sf *Snowflake) Load(dataset string, files []string, batchTimestamp int64) error {
	_, err := withRefresh(sf, func() (any, error) {

		return nil, func() error {
			nameSpace := fmt.Sprintf("%s.%s", strings.ToUpper(sf.cfg.SnowflakeDB), strings.ToUpper(sf.cfg.SnowflakeSchema))
			stage := fmt.Sprintf("%s.S_", nameSpace) + strings.ToUpper(strings.ReplaceAll(dataset, ".", "_"))
			tableName := strings.ToUpper(strings.ReplaceAll(dataset, ".", "_"))

			tx, err := p.db.Begin()
			if err != nil {
				return err
			}
			defer func() {
				_ = tx.Rollback()
			}()

			if _, err := tx.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.%s (
  		id varchar,
		recorded integer,
  		deleted boolean,
  		dataset varchar,
  		entity variant
	);
	`, nameSpace, tableName)); err != nil {
				return err
			}

			fileString := "'" + strings.Join(files, "', '") + "'"

			sf.log.Trace().Msgf("Loading %s", fileString)
			q := fmt.Sprintf(`
	COPY INTO %s.%s(id, recorded, deleted, dataset, entity)
	    FROM (
	    	SELECT
 			$1:id::varchar,
			%v::integer,
 			$1:deleted::boolean,
			'%s'::varchar,
 			$1::variant
	    	FROM @%s)
	FILE_FORMAT = (TYPE='json' COMPRESSION=GZIP)
	FILES = (%s);
	`, nameSpace, tableName, batchTimestamp, dataset, stage, fileString)
			sf.log.Trace().Msg(q)
			if _, err := tx.Query(q); err != nil {
				return err
			}
			sf.log.Trace().Msgf("Done with %s", files)
			return tx.Commit()
		}() // end of func
	})
	return err
}

func (sf *Snowflake) LoadStage(dataset string, stage string, batchTimestamp int64) error {
	_, err := withRefresh(sf, func() (any, error) {
		return nil, func() error {
			tableName := strings.ToUpper(strings.ReplaceAll(dataset, ".", "_"))
			tableName = fmt.Sprintf("%s.%s.%s", strings.ToUpper(sf.cfg.SnowflakeDB), strings.ToUpper(sf.cfg.SnowflakeSchema), tableName)
			loadTableName := stage

			tx, err := p.db.Begin()
			if err != nil {
				return err
			}
			defer func() {
				_ = tx.Rollback()
			}()
			smt := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
  		id varchar,
		recorded integer,
  		deleted boolean,
  		dataset varchar,
  		entity variant);
	`, loadTableName)

			// println("\n", smt)
			if _, err := tx.Exec(smt); err != nil {
				return err
			}

			sf.log.Trace().Msgf("Loading fs table %s", loadTableName)
			q := fmt.Sprintf(`
	COPY INTO %s(id, recorded, deleted, dataset, entity)
	    FROM (
	    	SELECT
 			$1:id::varchar,
			%v::integer,
 			$1:deleted::boolean,
			'%s'::varchar,
 			$1::variant
	    	FROM @%s)
	FILE_FORMAT = (TYPE='json' COMPRESSION=GZIP);
	`, loadTableName, batchTimestamp, dataset, stage)
			sf.log.Trace().Msg(q)
			if _, err := tx.Query(q); err != nil {
				return err
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
	return err
}
