package internal

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
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
func (sf *Snowflake) Put(ctx context.Context, dataset string, entityContext *uda.Context, entities []*Entity) ([]string, error) {
	// by starting with the sql before creating any files, then if no session is valid, we should prevent creating files we won't use
	// as it fails early
	stage := fmt.Sprintf("%s.%s.S_", strings.ToUpper(sf.cfg.SnowflakeDb), strings.ToUpper(sf.cfg.SnowflakeSchema)) + strings.ToUpper(strings.ReplaceAll(dataset, ".", "_")) //+ "_" + randSeq(10)
	q := fmt.Sprintf(`
	CREATE STAGE IF NOT EXISTS %s
	    copy_options = (on_error='skip_file')
	    file_format = (TYPE='json' STRIP_OUTER_ARRAY = TRUE);
	`, stage)
	sf.log.Trace().Msg(q)
	_, err := p.db.Exec(q)
	if err != nil {
		return nil, err
	}

	// we will handle snowflake in 2 steps, first write each batch as a ndjson file
	file, err := os.CreateTemp("", dataset)
	if err != nil {
		return nil, err
	}
	defer os.Remove(file.Name())

	pipeReader, pipeWriter := io.Pipe()
	j := jsons.NewWriter(pipeWriter)
	defer func() {
		pipeReader.Close()
		pipeWriter.Close()
	}()

	go func() {

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

			j.Add(entity)
		}
		pipeWriter.Close()
	}()
	// then upload to staging
	files := make([]string, 0)
	sf.log.Debug().Msgf("Uploading %s", file.Name())
	streamCtx := gsf.WithFileStream(ctx, pipeReader)
	if _, err := p.db.QueryContext(streamCtx, fmt.Sprintf("PUT file://%s @%s auto_compress=false overwrite=false", file.Name(), stage)); err != nil {
		return files, err
	}
	files = append(files, filepath.Base(file.Name()))

	return files, nil
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
	FILE_FORMAT = (TYPE='json') 
	FILES = (%s);
	`, nameSpace, tableName, batchTimestamp, dataset, stage, fileString)
	sf.log.Trace().Msg(q)
	if _, err := tx.Query(q); err != nil {
		return err
	}
	sf.log.Trace().Msgf("Done with %s", files)
	return tx.Commit()
}