package internal

import (
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	common_datalayer "github.com/mimiro-io/common-datalayer"

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

func (sf *Snowflake) tableParts(mapping *common_datalayer.DatasetDefinition) (string, string, string) {
	dsName := strings.ToUpper(strings.ReplaceAll(mapping.DatasetName, ".", "_"))
	if ds, ok := mapping.SourceConfig[TableName]; ok {
		dsName = strings.ToUpper(ds.(string))
	}
	dbName := strings.ToUpper(sf.cfg.SnowflakeDB)
	if db, ok := mapping.SourceConfig[Database]; ok {
		dbName = strings.ToUpper(db.(string))
	}
	schemaName := strings.ToUpper(sf.cfg.SnowflakeSchema)
	if schema, ok := mapping.SourceConfig[Schema]; ok {
		schemaName = strings.ToUpper(schema.(string))
	}
	return dbName, schemaName, dsName
}

func (sf *Snowflake) colMappings(mapping *common_datalayer.DatasetDefinition) (string, string, string) {
	columns := ", entity"
	columnTypes := ", entity variant"
	colExtractions := ", $1::variant"
	if mapping.IncomingMappingConfig != nil && mapping.IncomingMappingConfig.PropertyMappings != nil {
		columns = ""
		columnTypes = ""
		colExtractions = ""
		for _, col := range mapping.IncomingMappingConfig.PropertyMappings {
			srcMap := "props"
			if col.IsReference {
				srcMap = "refs"
			}
			columns = fmt.Sprintf("%s, %s", columns, col.Property)
			if col.IsRecorded {
				columnTypes = fmt.Sprintf("%s, %s INTEGER", columnTypes, col.Property)
				colExtractions = fmt.Sprintf(`%s, $1:recorded::integer`, colExtractions)
			} else if col.IsDeleted {
				columnTypes = fmt.Sprintf("%s, %s BOOLEAN", columnTypes, col.Property)
				colExtractions = fmt.Sprintf(`%s, $1:deleted::boolean`, colExtractions)
			} else if col.IsIdentity {
				columnTypes = fmt.Sprintf("%s, %s %s", columnTypes, col.Property, col.Datatype)
				colExtractions = fmt.Sprintf(`%s, $1:id::%s`, colExtractions, col.Datatype)
			} else {
				columnTypes = fmt.Sprintf("%s, %s %s", columnTypes, col.Property, col.Datatype)
				colExtractions = fmt.Sprintf(`%s, $1:%s:"%s"::%s`, colExtractions, srcMap, col.EntityProperty, col.Datatype)
			}
		}
	}
	return columns[2:], columnTypes[2:], colExtractions[2:]
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
