package layer

import (
	"context"
	"fmt"
	"strings"

	common "github.com/mimiro-io/common-datalayer"
)

type SnowflakeDataLayer struct {
	datasets map[string]*Dataset
	logger   common.Logger
	metrics  common.Metrics
	config   *common.Config
	db       db
}

// Dataset implements common_datalayer.DataLayerService.
func (dl *SnowflakeDataLayer) Dataset(dataset string) (common.Dataset, common.LayerError) {
	memErr := dl.assertMemory()
	if memErr != nil {
		return nil, memErr
	}
	// try explicit mappings first
	ds, found := dl.datasets[dataset]
	if found {
		return ds, nil
	}

	// construct implicit mapping if not found
	dl.logger.Debug("Failed to get mapping for dataset " + dataset + ". Trying implicit mapping.")
	ds = &Dataset{name: dataset, db: dl.db, logger: dl.logger}

	// in read mode, we expect the dataset name to contain db and schema in the form db.schema.table
	readMapping, err := implicitMapping(dataset)
	if err == nil {
		ds.sourceConfig = readMapping.SourceConfig
		ds.datasetDefinition = readMapping
		dl.logger.Debug(fmt.Sprintf("infered implicit target table[r]: %+v", ds.sourceConfig))
		return ds, nil
	}

	// in write mode. we only allow writing to the configured db and schema, so the name is just the table name
	// implicitWriteName is used to construct the full name in write mode
	impWriteName := dataset
	impWriteName = strings.ReplaceAll(dataset, ".", "_")
	impWriteName = fmt.Sprintf("%s.%s.%s", dl.config.NativeSystemConfig[SnowflakeDB], dl.config.NativeSystemConfig[SnowflakeSchema], impWriteName)
	writeMapping, err := implicitMapping(impWriteName)
	if err == nil {
		ds.sourceConfig = writeMapping.SourceConfig
		ds.datasetDefinition = writeMapping
		dl.logger.Debug(fmt.Sprintf("infered implicit target table[w]: %+v", ds.sourceConfig))
		return ds, nil
	}
	// return error if implicit mapping cannot be constructed
	return nil, common.Errorf(common.LayerErrorBadParameter, "dataset %s not found and cannot infer implicit target table from name, %w", dataset, err)
}

// DatasetDescriptions implements common_datalayer.DataLayerService.
func (dl *SnowflakeDataLayer) DatasetDescriptions() []*common.DatasetDescription {
	var datasetDescriptions []*common.DatasetDescription
	for key := range dl.datasets {
		datasetDescriptions = append(datasetDescriptions, &common.DatasetDescription{Name: key})
	}
	return datasetDescriptions
}

// Stop implements common_datalayer.DataLayerService.
func (*SnowflakeDataLayer) Stop(ctx context.Context) error {
	return nil
}

func NewSnowflakeDataLayer(conf *common.Config, logger common.Logger, metrics common.Metrics) (common.DataLayerService, error) {
	err := validateConfig(conf)
	if err != nil {
		return nil, err
	}

	sfdb, err := newSfDB(conf, logger, metrics)
	if err != nil {
		return nil, err
	}

	l := &SnowflakeDataLayer{
		datasets: map[string]*Dataset{},
		logger:   logger,
		metrics:  metrics,
		config:   conf,
		db:       sfdb,
	}
	err = l.UpdateConfiguration(conf)
	if err != nil {
		return nil, err
	}
	return l, nil
}
