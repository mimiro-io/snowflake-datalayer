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
	"time"

	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

type db interface {
	newConnection(ctx context.Context) (*sql.Conn, error)
	putEntities(ctx context.Context, datasetName string, stage string, entities []*egdm.Entity) ([]string, error)
	mkStage(ctx context.Context, syncID string, datasetName string, datasetDefinition *common.DatasetDefinition) (string, error)
	getFsStage(syncId string, datasetDefinition *common.DatasetDefinition) string
	loadStage(ctx context.Context, stage string, loadTime int64, datasetDefinition *common.DatasetDefinition) error
	loadFilesInStage(ctx context.Context, files []string, stage string, loadTime int64, datasetDefinition *common.DatasetDefinition) error
	createQuery(ctx context.Context, datasetDefinition *common.DatasetDefinition) (query, error)
	close() error
}

type Dataset struct {
	logger            common.Logger
	db                db
	datasetDefinition *common.DatasetDefinition
	sourceConfig      map[string]any
	name              string
}

// MetaData implements common.Dataset.
func (ds *Dataset) MetaData() map[string]any {
	return ds.sourceConfig
}

// Name implements common.Dataset.
func (ds *Dataset) Name() string {
	return ds.name
}

func (ds *Dataset) dbCtx(ctx context.Context) (context.Context, func(), error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	ctx = context.WithValue(ctx, Recorded, time.Now().UnixNano())
	conn, err := ds.db.newConnection(ctx)
	if err != nil {
		defer cancel()
		return nil, nil, err
	}

	_, err = conn.ExecContext(ctx, "ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON';")
	if err != nil {
		defer cancel()
		return nil, nil, err
	}
	// activate secondary roles
	_, err = conn.ExecContext(ctx, "USE SECONDARY ROLES ALL;")
	if err != nil {
		defer cancel()
		return nil, nil, err
	}

	ctx = context.WithValue(ctx, Connection, conn)
	return ctx, func() {
		if ctx.Value(Connection) != nil {
			cancel()
			ctxConn := ctx.Value(Connection).(*sql.Conn)
			err2 := ctxConn.Close()
			if err2 != nil {
				ds.logger.Error("Failed to close connection", "error", err2)
				return
			}
		} else {
			ds.logger.Error("No connection to close")
		}
	}, nil
}
