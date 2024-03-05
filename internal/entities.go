package layer

import (
	"context"

	common "github.com/mimiro-io/common-datalayer"
)

// Entities implements common.Dataset.
// TODO: should the common library pass in a context? to make it consistent with the other methods?
// TODO: since param should be called 'from' here? to make it consistent with DH. its not a since token but a paging continuation
func (ds *Dataset) Entities(from string, limit int) (common.EntityIterator, common.LayerError) {
	ctx, release, err := ds.dbCtx(context.Background())
	if err != nil {
		return nil, common.Err(err, common.LayerErrorInternal)
	}
	q, err := ds.db.createQuery(ctx, ds.datasetDefinition)
	if err != nil {
		return nil, common.Err(err, common.LayerErrorInternal)
	}

	// due to nature of since queries (no real changes, just ordered entities),
	// we can use the since logic here for entities pagination as well.
	sinceColumn, sinceActive := ds.sourceConfig[SinceColumn]
	if sinceActive {
		_, err := q.withSince(sinceColumn.(string), from)
		if err != nil {
			return nil, common.Err(err, common.LayerErrorInternal)
		}
	}

	if limit > 0 {
		_, err := q.withLimit(limit)
		if err != nil {
			return nil, common.Err(err, common.LayerErrorInternal)
		}
	}

	return q.run(ctx, release)
}
