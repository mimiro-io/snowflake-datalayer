package layer

import (
	"context"

	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

// Incremental implements common.Dataset.
func (ds *Dataset) Incremental(ctx context.Context) (common.DatasetWriter, common.LayerError) {
	ctx, release, err := ds.dbCtx(ctx)
	if err != nil {
		return nil, common.Err(err, common.LayerErrorInternal)
	}

	stage, err2 := ds.db.mkStage(ctx, "", ds.name, ds.datasetDefinition)
	if err2 != nil {
		return nil, common.Err(err2, common.LayerErrorInternal)
	}

	// TODO: make configurable in navtive system config?
	var batchSize int64 = 50000
	return &batchWriter{
		ctx:       ctx,
		dataset:   ds,
		release:   release,
		stage:     stage,
		batchSize: batchSize,
	}, nil
}

type batchWriter struct {
	ctx       context.Context
	dataset   *Dataset
	release   func()
	stage     string
	entities  []*egdm.Entity
	read      int64
	batchSize int64
	files     []string
}

// Close implements common_datalayer.DatasetWriter.
func (w *batchWriter) Close() common.LayerError {
	if w.read > 0 {
		newFiles, err := w.dataset.db.putEntities(w.ctx, w.dataset.name, w.stage, w.entities)
		if err != nil {
			return common.Err(err, common.LayerErrorInternal)
		}
		w.files = append(w.files, newFiles...)
	}

	if len(w.files) > 0 {
		err := w.dataset.db.load(w.ctx, w.files, w.stage, w.ctx.Value(Recorded).(int64), w.dataset.datasetDefinition)
		if err != nil {
			return common.Err(err, common.LayerErrorInternal)
		}
	}
	return nil
}

// Write implements common_datalayer.DatasetWriter.
func (w *batchWriter) Write(entity *egdm.Entity) common.LayerError {
	w.entities = append(w.entities, entity)
	w.read++
	if w.read == w.batchSize {

		newFiles, err := w.dataset.db.putEntities(w.ctx, w.dataset.name, w.stage, w.entities)
		if err != nil {
			return common.Err(err, common.LayerErrorInternal)
		}
		w.files = append(w.files, newFiles...)

		w.read = 0
		w.entities = make([]*egdm.Entity, 0)
	}
	return nil
}
