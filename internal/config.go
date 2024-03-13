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

import (
	"fmt"
	"os"
	"strings"

	common "github.com/mimiro-io/common-datalayer"
)

type CtxKey int

const (
	Connection CtxKey = iota
	Recorded
	Closed
)

var (
	ErrNoImplicitDataset = common.Errorf(common.LayerErrorBadParameter, "no implicit mapping for dataset")
	ErrQuery             = common.Errorf(common.LayerErrorInternal, "failed to query snowflake")
	ErrHeadroom          = common.Errorf(common.LayerErrorInternal, "MemoryGuard: headroom too low, rejecting request")
)

const (
	// dataset mapping config
	TableName   = "table_name"
	Schema      = "schema"
	Database    = "database"
	RawColumn   = "raw_column"
	SinceColumn = "since_column"

	// native system config
	MemoryHeadroom      = "memory_headroom"
	SnowflakeDB         = "snowflake_db"
	SnowflakeSchema     = "snowflake_schema"
	SnowflakeUser       = "snowflake_user"
	SnowflakeAccount    = "snowflake_account"
	SnowflakeWarehouse  = "snowflake_warehouse"
	SnowflakePrivateKey = "snowflake_private_key"
)

func sysConfStr(conf *common.Config, key string) string {
	v, ok := conf.NativeSystemConfig[key].(string)

	if !ok {
		// should never happen, since we assume all config is validated already.
		// if it does, improve the validateConfig function
		panic(fmt.Sprintf("expected string value for %s, got %T", key, conf.NativeSystemConfig[key]))
	}

	return v
}

// TODO: use BuildNativeEnvOverrides from common-datalayer
func EnvOverrides(config *common.Config) error {
	if v, ok := os.LookupEnv("MEMORY_HEADROOM"); ok {
		config.NativeSystemConfig[MemoryHeadroom] = v
	}
	if v, ok := os.LookupEnv("SNOWFLAKE_DB"); ok {
		config.NativeSystemConfig[SnowflakeDB] = v
	}
	if v, ok := os.LookupEnv("SNOWFLAKE_SCHEMA"); ok {
		config.NativeSystemConfig[SnowflakeSchema] = v
	}
	if v, ok := os.LookupEnv("SNOWFLAKE_USER"); ok {
		config.NativeSystemConfig[SnowflakeUser] = v
	}
	if v, ok := os.LookupEnv("SNOWFLAKE_ACCOUNT"); ok {
		config.NativeSystemConfig[SnowflakeAccount] = v
	}
	if v, ok := os.LookupEnv("SNOWFLAKE_WAREHOUSE"); ok {
		config.NativeSystemConfig[SnowflakeWarehouse] = v
	}
	if v, ok := os.LookupEnv("SNOWFLAKE_PRIVATE_KEY"); ok {
		config.NativeSystemConfig[SnowflakePrivateKey] = v
	}
	return nil
}

func validateConfig(conf *common.Config) error {
	if conf.LayerServiceConfig == nil {
		return fmt.Errorf("missing required layer_config block")
	}
	if conf.NativeSystemConfig == nil {
		return fmt.Errorf("missing required system_config block")
	}
	type p struct {
		v any
		n string
	}
	reqVal := func(vals ...p) error {
		for _, v := range vals {
			if v.v == nil || v.v == "" {
				return fmt.Errorf("missing required config value %v", v.n)
			}
		}
		return nil
	}
	return reqVal(
		p{conf.LayerServiceConfig.ServiceName, "service_name"},
		p{conf.LayerServiceConfig.Port, "port"},
		p{conf.NativeSystemConfig[SnowflakeDB], SnowflakeDB},
		p{conf.NativeSystemConfig[SnowflakeSchema], SnowflakeSchema},
		p{conf.NativeSystemConfig[SnowflakeUser], SnowflakeUser},
		p{conf.NativeSystemConfig[SnowflakeAccount], SnowflakeAccount},
		p{conf.NativeSystemConfig[SnowflakeWarehouse], SnowflakeWarehouse},
		p{conf.NativeSystemConfig[SnowflakePrivateKey], SnowflakePrivateKey},
	)
}

// UpdateConfiguration implements common_datalayer.DataLayerService.
// we only dynamically update the mapping config.
// the rest of the config is static and loaderd in NewSnowflakeDataLayer
func (dl *SnowflakeDataLayer) UpdateConfiguration(config *common.Config) common.LayerError {
	existingDatasets := map[string]bool{}
	// update existing datasets
	for k, v := range dl.datasets {
		for _, dsd := range config.DatasetDefinitions {
			if k == dsd.DatasetName {
				existingDatasets[k] = true
				v.sourceConfig = dsd.SourceConfig
				v.datasetDefinition = dsd
			}
		}
	}
	// remove deleted datasets
	for k := range dl.datasets {
		if _, found := existingDatasets[k]; !found {
			delete(dl.datasets, k)
		}
	}

	// add new datasets
	for _, dsd := range config.DatasetDefinitions {
		if _, found := existingDatasets[dsd.DatasetName]; !found {
			dl.datasets[dsd.DatasetName] = &Dataset{
				logger:            dl.logger,
				name:              dsd.DatasetName,
				sourceConfig:      dsd.SourceConfig,
				db:                dl.db,
				datasetDefinition: dsd,
			}
		}
	}

	return nil
}

// if there is no read config for the given dataset name, make an attempt
// to interpret the dataset name string as table spec.
func implicitMapping(name string) (*common.DatasetDefinition, error) {
	tokens := strings.Split(name, ".")
	if len(tokens) == 3 {
		return &common.DatasetDefinition{
			DatasetName: name,
			SourceConfig: map[string]any{
				TableName: tokens[2],
				Schema:    tokens[1],
				Database:  tokens[0],
				RawColumn: "ENTITY",
			},

			IncomingMappingConfig: &common.IncomingMappingConfig{},
			OutgoingMappingConfig: &common.OutgoingMappingConfig{},
		}, nil
	}
	return nil, fmt.Errorf("%w %s. expected database.schema.table format", ErrNoImplicitDataset, name)
}
