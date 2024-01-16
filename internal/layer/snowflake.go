package layer

import (
	"context"

	common "github.com/mimiro-io/common-datalayer"
)

type SnowflakeDataLayer struct {
	datasets map[string]*Dataset
}

// Dataset implements common_datalayer.DataLayerService.
func (sdl *SnowflakeDataLayer) Dataset(dataset string) (common.Dataset, common.LayerError) {
	ds, found := sdl.datasets[dataset]
	if found {
		return ds, nil
	}
	return nil, common.Errorf(common.LayerErrorBadParameter, "dataset %s not found", dataset)
}

// DatasetDescriptions implements common_datalayer.DataLayerService.
func (sdl *SnowflakeDataLayer) DatasetDescriptions() []*common.DatasetDescription {
	var datasetDescriptions []*common.DatasetDescription
	for key := range sdl.datasets {
		datasetDescriptions = append(datasetDescriptions, &common.DatasetDescription{Name: key})
	}
	return datasetDescriptions
}

// Stop implements common_datalayer.DataLayerService.
func (*SnowflakeDataLayer) Stop(ctx context.Context) error {
	return nil
}

func NewSnowflakeDataLayer(conf *common.Config, logger common.Logger, metrics common.Metrics) (common.DataLayerService, error) {
	l := &SnowflakeDataLayer{
		datasets: map[string]*Dataset{},
	}
	return l, nil
}
