package layer

import (
	"context"
	"strings"

	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

func (ds *Dataset) FullSync(ctx context.Context, batchInfo common.BatchInfo) (common.DatasetWriter, common.LayerError) {
	ctx, release, err := ds.dbCtx(ctx)
	if err != nil {
		return nil, common.Err(err, common.LayerErrorInternal)
	}

	fsID := batchInfo.SyncId
	if fsID != "" {
		fsID = strings.ReplaceAll(fsID, "-", "_")
	}
	var stage string
	if batchInfo.IsStartBatch {
		// mkStage
		var err2 error
		stage, err2 = ds.db.mkStage(ctx, fsID, ds.name, ds.datasetDefinition)
		if err2 != nil {
			ds.logger.Error("Failed to create stage", "error", err2, "stage", stage)
			return nil, common.Err(err2, common.LayerErrorInternal)
		}
		ds.logger.Info("Created stage", "stage", stage)
	} else {
		// getStage
		stage = ds.db.getFsStage(fsID, ds.datasetDefinition)
	}

	// TODO: make configurable in navtive system config?
	var batchSize int64 = 50000
	writer := &datasetWriter{
		dataset:   ds,
		ctx:       ctx,
		batchInfo: batchInfo,
		batchSize: batchSize,
		stage:     stage,
		release:   release,
	}
	return writer, nil

	// now let library call Write() on writer for each entity and emit batches
}

type datasetWriter struct {
	ctx       context.Context
	dataset   *Dataset
	release   func()
	stage     string
	batchInfo common.BatchInfo
	entities  []*egdm.Entity
	read      int64
	batchSize int64
}

// Close implements common_datalayer.DatasetWriter.
func (w *datasetWriter) Close() common.LayerError {
	defer w.release()
	// empty the buffer
	if w.read > 0 {
		_, err := w.dataset.db.putEntities(w.ctx, w.dataset.name, w.stage, w.entities)
		if err != nil {
			return common.Err(err, common.LayerErrorInternal)
		}
	}

	if w.batchInfo.IsLastBatch {
		w.dataset.logger.Info("Loading fullsync stage", "stage", w.stage)
		err := w.dataset.db.loadStage(w.ctx, w.stage, w.ctx.Value(Recorded).(int64), w.dataset.datasetDefinition)
		if err != nil {
			return common.Err(err, common.LayerErrorInternal)
		}

	}
	return nil
}

// Write implements common_datalayer.DatasetWriter.
func (w *datasetWriter) Write(entity *egdm.Entity) common.LayerError {
	w.entities = append(w.entities, entity)
	w.read++
	if w.read == w.batchSize {
		_, err := w.dataset.db.putEntities(w.ctx, w.dataset.name, w.stage, w.entities)
		if err != nil {
			return common.Err(err, common.LayerErrorInternal)
		}
		w.read = 0
		w.entities = make([]*egdm.Entity, 0)
	}
	return nil
}
