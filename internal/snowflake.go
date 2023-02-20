package internal

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/bfontaine/jsons"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/rs/zerolog"
	sf "github.com/snowflakedb/gosnowflake"
	"math/rand"
	"os"
	"strings"
)

type pool struct {
	db *sql.DB
}

var p *pool

type Snowflake struct {
	cfg Config
	log zerolog.Logger
}

func NewSnowflake(cfg Config, _ statsd.ClientInterface) (*Snowflake, error) {
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
		config := &sf.Config{
			Account:       cfg.SnowflakeAccount,
			Database:      cfg.SnowflakeDb,
			Schema:        cfg.SnowflakeSchema,
			Warehouse:     cfg.SnowflakeWarehouse,
			User:          cfg.SnowflakeUser,
			Region:        "eu-west-1",
			Authenticator: sf.AuthTypeJwt,
			PrivateKey:    parsedKey.(*rsa.PrivateKey),
		}
		s, err := sf.DSN(config)
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

func (sf *Snowflake) Put(ctx context.Context, dataset string, entityContext *uda.Context, entities []*Entity) error {
	// we will handle snowflake in 2 steps, first write each batch as a ndjson file
	file, err := os.CreateTemp("", dataset)
	if err != nil {
		return err
	}

	LOG.Debug().Msg(file.Name())

	j := jsons.NewFileWriter(file.Name())
	if err := j.Open(); err != nil {
		return err
	}
	defer func() {
		_ = j.Close()
	}()

	for _, entity := range entities {
		entity.ID = uda.ToURI(entityContext, entity.ID)

		// do references
		newRefs := make(map[string]any)
		for refKey, refValue := range entity.References {
			// we need to do both key and value replacing
			key := uda.ToURI(entityContext, refKey)
			if values, ok := refValue.([]any); ok {
				var newValues []string
				for _, val := range values {
					newValues = append(newValues, uda.ToURI(entityContext, val.(string)))
				}
				newRefs[key] = newValues
			} else {
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

		sf.log.Trace().Any("entity", entity).Msg(entity.ID)
		j.Add(entity)
	}

	// then upload to staging
	stage := strings.ReplaceAll(dataset, ".", "_") + "_" + randSeq(10)
	tableName := strings.ReplaceAll("datahub."+dataset, ".", "_")
	_, err = p.db.Exec(fmt.Sprintf(`
	CREATE STAGE %s
	    copy_options = (on_error='skip_file')
	    file_format = (TYPE='json' STRIP_OUTER_ARRAY = TRUE);
	`, stage))
	if err != nil {
		return err
	}
	if _, err := p.db.Query(fmt.Sprintf("PUT file:///%s @%s auto_compress=true overwrite=false", file.Name(), stage)); err != nil {
		return err
	}
	if _, err := p.db.Exec(fmt.Sprintf(`
	CREATE OR REPLACE TABLE %s (
  		id varchar,
  		recorded integer,
  		deleted boolean,
  		entity variant
	);
	`, tableName)); err != nil {
		return err
	}

	if _, err := p.db.Exec(fmt.Sprintf(`
	COPY INTO %s(id, recorded, deleted, entity)
	    FROM (
	    	select
 			$1:id::varchar,
 			$1:recorded::integer,
 			$1:deleted::boolean,
 			$1::variant
	    	FROM @%s)
		file_format = (TYPE='json');
`, tableName, stage)); err != nil {
		return err
	}

	return nil
}

var letters = []rune("0123456789abcdefghijklmnopqrstuvwxyz")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
