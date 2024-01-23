package layer

import (
	"context"

	common "github.com/mimiro-io/common-datalayer"
)

// Entities implements common.Dataset.
// TODO: should the common library pass in a context? to make it consistent with the other methods?
// TODO: since param should be called 'from' here? to make it consistent with DH. its not a since token but a paging continuation
func (ds *Dataset) Entities(since string, take int) (common.EntityIterator, common.LayerError) {
	ctx, release, err := ds.dbCtx(context.Background())
	if err != nil {
		return nil, common.Err(err, common.LayerErrorInternal)
	}

	q, err := ds.db.createQuery(ctx, ds.datasetDefinition)
	if err != nil {
		return nil, common.Err(err, common.LayerErrorInternal)
	}

	sinceColumn, sinceActive := ds.sourceConfig[SinceColumn]
	if sinceActive {
		_, err := q.withSince(sinceColumn.(string), since)
		if err != nil {
			return nil, common.Err(err, common.LayerErrorInternal)
		}
	}

	if take > 0 {
		_, err := q.withLimit(take)
		if err != nil {
			return nil, common.Err(err, common.LayerErrorInternal)
		}
	}

	return q.run(ctx, release)
}
