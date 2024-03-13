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
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"os"
	"time"

	common "github.com/mimiro-io/common-datalayer"
	gsf "github.com/snowflakedb/gosnowflake"
)

type SfDB struct {
	db         *sql.DB
	conf       *common.Config
	logger     common.Logger
	metrics    common.Metrics
	NewTmpFile func(dataset string) (*os.File, func(), error) // file, error, function to cleanup file
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
	// println(connectionString)
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
		db:         db,
		conf:       conf,
		logger:     logger,
		metrics:    metrics,
		NewTmpFile: NewTmpFileWriter,
	}, nil
}

func (sf *SfDB) close() error {
	sf.logger.Warn("Closing db driver")
	return sf.db.Close()
}

func (sf *SfDB) newConnection(ctx context.Context) (*sql.Conn, error) {
	return sf.db.Conn(ctx)
}
