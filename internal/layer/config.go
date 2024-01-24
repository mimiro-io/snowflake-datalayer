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
	Role        = "role"
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

// TODO: this should be easier? e.g. pass a list of required and optional native params to the library
// and have env reading and validation setup automatically
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

// TODO: provide library function which takes a list of required native params?
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

	// TODO: re-apply EnvOverrides? normally native conf should not be both places
	// but to be sure that the ENV value always is set it would need to be re applied?

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
