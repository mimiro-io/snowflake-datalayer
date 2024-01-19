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
	load(ctx context.Context, files []string, stage string, loadTime int64, datasetDefinition *common.DatasetDefinition) error
}

type Dataset struct {
	logger            common.Logger
	db                db
	datasetDefinition *common.DatasetDefinition
	sourceConfig      map[string]any
	name              string
}

// Changes implements common.Dataset.
func (ds *Dataset) Changes(since string, take int, latestOnly bool) (common.EntityIterator, common.LayerError) {
	panic("unimplemented")
}

// Entities implements common.Dataset.
func (ds *Dataset) Entities(since string, take int) (common.EntityIterator, common.LayerError) {
	panic("unimplemented")
}

// MetaData implements common.Dataset.
func (ds *Dataset) MetaData() map[string]any {
	panic("unimplemented")
}

// Name implements common.Dataset.
func (ds *Dataset) Name() string {
	return ds.name
}

func (ds *Dataset) dbCtx(ctx context.Context) (context.Context, func(), error) {
	ctx = context.WithValue(ctx, Recorded, time.Now().UnixNano())
	conn, err := ds.db.newConnection(ctx)
	if err != nil {
		return nil, nil, err
	}

	_, err = conn.ExecContext(ctx, "ALTER SESSION SET GO_QUERY_RESULT_FORMAT = 'JSON';")
	if err != nil {
		return nil, nil, err
	}
	// activate secondary roles
	_, err = conn.ExecContext(ctx, "USE SECONDARY ROLES ALL;")
	if err != nil {
		return nil, nil, err
	}

	ctx = context.WithValue(ctx, Connection, conn)
	return ctx, func() {
		if ctx.Value(Connection) != nil {
			ctx.Value(Connection).(*sql.Conn).Close()
		} else {
			ds.logger.Error("No connection to close")
		}
	}, nil
}
