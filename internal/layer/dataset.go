package layer

import (
	"context"

	common_datalayer "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

type db interface {
	putEntities(datasetName string, stage string, entities []*egdm.Entity) ([]string, error)
}

type Dataset struct {
	mapper       *common_datalayer.Mapper
	sourceConfig map[string]any
	name         string
	db           db
}

// Changes implements common_datalayer.Dataset.
func (ds *Dataset) Changes(since string, take int, latestOnly bool) (common_datalayer.EntityIterator, common_datalayer.LayerError) {
	panic("unimplemented")
}

// Entities implements common_datalayer.Dataset.
func (ds *Dataset) Entities(since string, take int) (common_datalayer.EntityIterator, common_datalayer.LayerError) {
	panic("unimplemented")
}

// Incremental implements common_datalayer.Dataset.
func (ds *Dataset) Incremental(ctx context.Context) (common_datalayer.DatasetWriter, common_datalayer.LayerError) {
	panic("unimplemented")
}

// MetaData implements common_datalayer.Dataset.
func (ds *Dataset) MetaData() map[string]any {
	panic("unimplemented")
}

// Name implements common_datalayer.Dataset.
func (ds *Dataset) Name() string {
	panic("unimplemented")
}
