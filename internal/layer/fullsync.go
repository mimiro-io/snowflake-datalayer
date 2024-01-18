package layer

import (
	"context"
	"time"

	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

func (ds *Dataset) FullSync(ctx context.Context, batchInfo common.BatchInfo) (common.DatasetWriter, common.LayerError) {
	ctx = context.WithValue(ctx, Recorded, time.Now().UnixNano())
	if batchInfo.IsStartBatch {
		// mkStage
	} else {
		// getStage
	}

	// TODO: make configurable in navtive system config?
	var batchSize int64 = 50000
	writer, err := ds.newWriter(ctx, batchInfo, batchSize)
	if err != nil {
		return nil, err
	}
	return writer, nil

	// now let libraty call Write() on writer for each entity and emit batches
}

func (ds *Dataset) newWriter(ctx context.Context, batchInfo common.BatchInfo, batchSize int64) (*datasetWriter, common.LayerError) {
	writer := &datasetWriter{
		dataset:   ds,
		ctx:       ctx,
		batchInfo: batchInfo,
		batchSize: batchSize,
	}
	return writer, nil
}

type datasetWriter struct {
	dataset   *Dataset
	ctx       context.Context
	batchInfo common.BatchInfo
	entities  []*egdm.Entity
	read      int64
	batchSize int64
	stage     string
}

// Close implements common_datalayer.DatasetWriter.
func (w *datasetWriter) Close() common.LayerError {
	// empty the buffer
	if w.read > 0 {
		_, err := w.dataset.db.putEntities(w.dataset.name, w.stage, w.entities)
		if err != nil {
			return common.Err(err, common.LayerErrorInternal)
		}
	}

	if w.batchInfo.IsLastBatch {
		w.dataset.db.endFullSync(w.ctx)
	}
	return nil
}

// Write implements common_datalayer.DatasetWriter.
func (w *datasetWriter) Write(entity *egdm.Entity) common.LayerError {
	w.entities = append(w.entities, entity)
	w.read++
	if w.read == w.batchSize {
		_, err := ds.sf.putEntities(w.dataset.name, w.stage, w.entities)
		if err != nil {
			return err
		}
		w.read = 0
		w.entities = make([]*egdm.Entity, 0)
	}

	panic("unimplemented")
}
