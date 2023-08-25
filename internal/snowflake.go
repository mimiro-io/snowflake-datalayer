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
	"os"
	"path/filepath"
	"strings"

	"github.com/bfontaine/jsons"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/rs/zerolog"
	gsf "github.com/snowflakedb/gosnowflake"
)

type pool struct {
	db *sql.DB
}

var p *pool

type Snowflake struct {
	cfg Config
	log zerolog.Logger
}

func NewSnowflake(cfg Config) (*Snowflake, error) {
	// let's see what we have
	connectionString := "%s:%s@%s"
	if cfg.PrivateCert != "" {
		data, err := base64.StdEncoding.DecodeString(cfg.PrivateCert)
		if err != nil {
			return nil, err
		}
		//decrypted, err := pemutil.DecryptPKCS8PrivateKey(data, []byte(cfg.CertPassword))
		//cert, _ := pem.Decode(data)
		parsedKey, err := x509.ParsePKCS8PrivateKey(data)
		if err != nil {
			return nil, err
		}

		//privateKey, err := jwt.ParseRSAPrivateKeyFromPEMWithPassword(data, cfg.CertPassword)
		config := &gsf.Config{
			Account:       cfg.SnowflakeAccount,
			User:          cfg.SnowflakeUser,
			Database:      cfg.SnowflakeDb,
			Schema:        cfg.SnowflakeSchema,
			Warehouse:     cfg.SnowflakeWarehouse,
			Region:        "eu-west-1",
			Authenticator: gsf.AuthTypeJwt,
			PrivateKey:    parsedKey.(*rsa.PrivateKey),
		}
		s, err := gsf.DSN(config)
		if err != nil {
			return nil, err
		}
		connectionString = s
	} else {
		if cfg.SnowflakeUri != "" {
			connectionString = fmt.Sprintf(connectionString, cfg.SnowflakeUser, cfg.SnowflakePassword, cfg.SnowflakeUri)
		} else {
			uri := fmt.Sprintf("%s/%s/%s", cfg.SnowflakeAccount, cfg.SnowflakeDb, cfg.SnowflakeSchema)
			connectionString = fmt.Sprintf(connectionString, cfg.SnowflakeUser, cfg.SnowflakePassword, uri)
		}
	}

	db, err := sql.Open("snowflake", connectionString)
	if err != nil {
		return nil, err
	}

	p = &pool{
		db: db,
	}
	return &Snowflake{
		cfg: cfg,
		log: LOG.With().Str("logger", "snowflake").Logger(),
	}, nil
}

// Put returns a list of already uploaded files to be loaded into snowflake, Load must be called subsequently to finnish the process
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

func (sf *Snowflake) putEntities(dataset string, stage string, entities []*Entity, entityContext *uda.Context) ([]string, error) {
	// we will handle snowflake in 2 steps, first write each batch as a ndjson file
	file, err := os.CreateTemp("", dataset)
	if err != nil {
		return nil, err
	}
	defer os.Remove(file.Name())

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
}

func (sf *Snowflake) gzippedNDJson(file *os.File, entities []*Entity, entityContext *uda.Context, dataset string) error {
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
	stage := fmt.Sprintf("%s.%s.S_%s",
		strings.ToUpper(sf.cfg.SnowflakeDb),
		strings.ToUpper(sf.cfg.SnowflakeSchema),
		dsName)
	fsSuffix := fmt.Sprintf("_FSID_%s", fsId)
	stage = stage + fsSuffix
	return stage
}

func (sf *Snowflake) mkStage(fsId, dataset string) (string, error) {
	dsName := strings.ToUpper(strings.ReplaceAll(dataset, ".", "_"))
	stage := fmt.Sprintf("%s.%s.S_%s",
		strings.ToUpper(sf.cfg.SnowflakeDb),
		strings.ToUpper(sf.cfg.SnowflakeSchema),
		dsName)

	if fsId != "" {
		fsSuffix := fmt.Sprintf("_FSID_%s", fsId)
		query := "SHOW STAGES LIKE '%" + dsName + "_FSID_%' IN " + sf.cfg.SnowflakeDb + "." + sf.cfg.SnowflakeSchema
		query = query + ";select \"name\" FROM table(RESULT_SCAN(LAST_QUERY_ID()))"
		//println(query)
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
			_, err = p.db.Exec(fmt.Sprintf("DROP STAGE %s.%s.%s", sf.cfg.SnowflakeDb, sf.cfg.SnowflakeSchema, existingFsStage))
			if err != nil {
				return "", err
			}
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
		return "", err
	}
	return stage, err
}

func (sf *Snowflake) Load(dataset string, files []string, batchTimestamp int64) error {
	nameSpace := fmt.Sprintf("%s.%s", strings.ToUpper(sf.cfg.SnowflakeDb), strings.ToUpper(sf.cfg.SnowflakeSchema))
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
}

func (sf *Snowflake) LoadStage(dataset string, stage string, batchTimestamp int64) error {
	tableName := strings.ToUpper(strings.ReplaceAll(dataset, ".", "_"))
	tableName = fmt.Sprintf("%s.%s.%s", strings.ToUpper(sf.cfg.SnowflakeDb), strings.ToUpper(sf.cfg.SnowflakeSchema), tableName)
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

	//println("\n", smt)
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
}