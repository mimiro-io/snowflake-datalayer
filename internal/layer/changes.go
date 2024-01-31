package layer

import common "github.com/mimiro-io/common-datalayer"

// Changes implements common.Dataset.
//
// Currently, this layer does not implement proper change detection, but it is possible
// to page through all (current) entities in the dataset if a sinceColumn is configured.
//
// making this paging available as changes endpoint allows for incremental consumption
// with the limitation that there will never be deletion changes. this is a tradeoff
//
// To mitigate, use incremental in conjunction with regular fullsyncs (the fullsync protocol
// requires the consumer to deal with deletions).
func (ds *Dataset) Changes(since string, take int, latestOnly bool) (common.EntityIterator, common.LayerError) {
	return ds.Entities(since, take)
}
