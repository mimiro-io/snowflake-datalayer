package layer

import (
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"time"

	common "github.com/mimiro-io/common-datalayer"
	gsf "github.com/snowflakedb/gosnowflake"
)

type SfDB struct {
	db      *sql.DB
	logger  common.Logger
	metrics common.Metrics
}

func newSfDB(conf *common.Config, logger common.Logger, metrics common.Metrics) (*SfDB, error) {
	connectionString := "%s:%s@%s"
	data, err := base64.StdEncoding.DecodeString(sysConfStr(conf, SnowflakePrivateKey))
	if err != nil {
		return nil, err
	}

	parsedKey8, err := x509.ParsePKCS8PrivateKey(data)
	if err != nil {
		return nil, err
	}
	parsedKey := parsedKey8.(*rsa.PrivateKey)
	config := &gsf.Config{
		Account:       sysConfStr(conf, SnowflakeAccount),
		User:          sysConfStr(conf, SnowflakeUser),
		Database:      sysConfStr(conf, SnowflakeDB),
		Schema:        sysConfStr(conf, SnowflakeSchema),
		Warehouse:     sysConfStr(conf, SnowflakeWarehouse),
		Region:        "eu-west-1",
		Authenticator: gsf.AuthTypeJwt,
		PrivateKey:    parsedKey,
	}
	s, err := gsf.DSN(config)
	if err != nil {
		return nil, err
	}
	connectionString = s

	logger.Info("opening db")
	db, err := sql.Open("snowflake", connectionString)
	if err != nil {
		return nil, err
	}
	// snowflake tokens time out, after 4 hours with default session settings.
	// if we do not evict idle connections, we will get errors after 4 hours
	db.SetConnMaxIdleTime(30 * time.Second)
	db.SetConnMaxLifetime(1 * time.Hour)

	return &SfDB{
		db:      db,
		logger:  logger,
		metrics: metrics,
	}, nil
}
