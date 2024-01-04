package internal

import (
	"context"
	"fmt"
	"io"
	"strings"

	common_datalayer "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"github.com/rs/zerolog"
)

type Dataset struct {
	cfg *Config
	log zerolog.Logger
	sf  *Snowflake
	// m    statsd.ClientInterface
}

func NewDataset(cfg *Config, sf *Snowflake) *Dataset {
	return &Dataset{
		cfg: cfg,
		log: LOG.With().Str("logger", "dataset").Logger(),
		sf:  sf,
		// m:   m,
	}
}

func (ds *Dataset) WriteFs(ctx context.Context, info dsInfo, reader io.Reader) error {
	var stage string
	mappings, err := ds.GetDatasetMapping(info, true)
	if err != nil {
		return err
	}
	if info.fsStart && info.fsID != "" {
		var err error
		stage, err = ds.sf.mkStage(info.fsID, info.name, mappings)
		if err != nil {
			ds.log.Error().Err(err).Str("stage", stage).Msg("Failed to create stage, and is not a refresh issue")
			return err
		}
		ds.log.Info().Str("stage", stage).Msg("Created stage")
	} else {
		stage = ds.sf.getStage(info.fsID, info.name)
	}
	var batchSize int64 = 50000
	if s, ok := ctx.Value("batchSize").(int64); ok {
		batchSize = s
	}

	var read int64 = 0
	entities := make([]*egdm.Entity, 0)
	nsm := egdm.NewNamespaceContext()
	esp := egdm.NewEntityParser(nsm).WithExpandURIs()
	err = esp.Parse(reader, func(entity *egdm.Entity) error {
		entities = append(entities, entity)
		read++
		if read == batchSize {
			_, err := ds.sf.putEntities(info.name, stage, entities)
			if err != nil {
				return err
			}
			read = 0
			entities = make([]*egdm.Entity, 0)
		}
		return nil
	}, nil)
	if err != nil {
		return err
	}
	if read > 0 {
		_, err := ds.sf.putEntities(info.name, stage, entities)
		if err != nil {
			return err
		}
	}
	if info.fsEnd {
		ds.log.Info().Str("stage", stage).Msg("Loading fullsync stage")
		err := ds.sf.LoadStage(stage, ctx.Value("recorded").(int64), mappings)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ds *Dataset) Write(ctx context.Context, info dsInfo, reader io.Reader) error {
	dataset := info.name
	var batchSize int64 = 50000
	var read int64 = 0
	entities := make([]*egdm.Entity, 0)
	files := make([]string, 0)

	mappings, err := ds.GetDatasetMapping(info, true)
	if err != nil {
		return err
	}
	nsm := egdm.NewNamespaceContext()
	esp := egdm.NewEntityParser(nsm).WithExpandURIs()
	err = esp.Parse(reader, func(entity *egdm.Entity) error {
		entities = append(entities, entity)
		read++
		if read == batchSize {
			stage, err := ds.sf.mkStage("", dataset, mappings)
			if err != nil {
				return err
			}
			newFiles, err := ds.sf.putEntities(dataset, stage, entities)
			if err != nil {
				return err
			}
			files = append(files, newFiles...)
			read = 0
			entities = make([]*egdm.Entity, 0)
		}
		return nil
	}, nil)
	if err != nil {
		return err
	}
	if read > 0 {
		// mkStage is idempotent, so we can call it again to be sure it exists
		stage, err2 := ds.sf.mkStage("", dataset, mappings)
		if err2 != nil {
			return err2
		}
		newFiles, err2 := ds.sf.putEntities(dataset, stage, entities)
		if err2 != nil {
			return err2
		}
		files = append(files, newFiles...)
	}
	if len(files) > 0 {
		err2 := ds.sf.Load(files, ctx.Value("recorded").(int64), mappings)
		if err2 != nil {
			return err2
		}
	}

	return nil
}

func (ds *Dataset) ReadAll(ctx context.Context, writer io.Writer, dsInfo dsInfo) error {
	mapping, err := ds.GetDatasetMapping(dsInfo, false)
	if err != nil {
		return err
	}
	return ds.sf.ReadAll(ctx, writer, dsInfo, mapping)
}

func (ds *Dataset) GetDatasetMapping(dsInfo dsInfo, write bool) (*common_datalayer.DatasetDefinition, error) {
	mapping, err := ds.cfg.Mapping(dsInfo.name)
	if err != nil {
		LOG.Debug().Msg("Failed to get mapping for dataset " + dsInfo.name + ". Trying implicit mapping.")

		// in read mode, we expect the dataset name to contain db and schema in the form db.schema.table
		impName := dsInfo.name
		if write {
			// in write mode. we only allow writing to the configured db and schema, so the name is just the table name
			// implicitWriteName is used to construct the full name in write mode
			impName = strings.ReplaceAll(dsInfo.name, ".", "_")
			impName = fmt.Sprintf("%s.%s.%s", ds.cfg.SnowflakeDB, ds.cfg.SnowflakeSchema, impName)
		}
		var err2 error
		mapping, err2 = implicitMapping(impName)
		if err2 != nil {
			LOG.Error().Err(err2).Msg("Failed to get implicit mapping for dataset " + dsInfo.name + ".")
			return nil, fmt.Errorf("no table mapping: %w, %w", err, err2)
		}
	}
	return mapping, nil
}

const (
	TableName   = "table_name"
	Schema      = "schema"
	Database    = "database"
	Role        = "role"
	RawColumn   = "raw_column"
	SinceColumn = "since_column"
	// DefaultType = "default_type"
)

// if there is no read config for the given dataset name, make an attempt
// to interpret the dataset name string as table spec.
func implicitMapping(name string) (*common_datalayer.DatasetDefinition, error) {
	tokens := strings.Split(name, ".")
	if len(tokens) == 3 {
		return &common_datalayer.DatasetDefinition{
			DatasetName: name,
			SourceConfig: map[string]any{
				TableName: tokens[2],
				Schema:    tokens[1],
				Database:  tokens[0],
				RawColumn: "ENTITY",
			},

			IncomingMappingConfig: nil,
			OutgoingMappingConfig: &common_datalayer.OutgoingMappingConfig{},
		}, nil
	}
	return nil, fmt.Errorf("%w %s. expected database.schema.table format", ErrNoImplicitDataset, name)
}
