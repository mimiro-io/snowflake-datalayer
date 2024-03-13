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
