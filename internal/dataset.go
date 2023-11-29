package internal

import (
	"context"
	"fmt"
	common_datalayer "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"github.com/rs/zerolog"
	"io"
	"strings"
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
	if info.fsStart && info.fsID != "" {
		var err error
		stage, err = ds.sf.mkStage(info.fsID, info.name)
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
	err := esp.Parse(reader, func(entity *egdm.Entity) error {
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
		err := ds.sf.LoadStage(info.name, stage, ctx.Value("recorded").(int64))
		if err != nil {

			return err
		}
	}
	return nil
}

func (ds *Dataset) Write(ctx context.Context, dataset string, reader io.Reader) error {
	var batchSize int64 = 50000
	var read int64 = 0
	entities := make([]*egdm.Entity, 0)
	files := make([]string, 0)

	nsm := egdm.NewNamespaceContext()
	esp := egdm.NewEntityParser(nsm).WithExpandURIs()
	err := esp.Parse(reader, func(entity *egdm.Entity) error {
		entities = append(entities, entity)
		read++
		if read == batchSize {
			var err error
			files, err = ds.safeEnsureStageAndPut(dataset, entities, files)
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
		files, err = ds.safeEnsureStageAndPut(dataset, entities, files)
		if err != nil {
			return err
		}
		read = 0
		entities = make([]*egdm.Entity, 0)
	}
	if len(files) > 0 {
		err2 := ds.sf.Load(dataset, files, ctx.Value("recorded").(int64))
		if err2 != nil {
			return err2
		}
	}

	return nil
}

func (ds *Dataset) safeEnsureStageAndPut(dataset string, entities []*egdm.Entity, files []string) ([]string, error) {
	stage, err := ds.sf.mkStage("", dataset)
	if err != nil {
		return nil, err
	}

	newFiles, err2 := ds.sf.putEntities(dataset, stage, entities)
	if err2 != nil {
		return nil, err2
	}
	files = append(files, newFiles...)
	return files, nil
}

func (ds *Dataset) ReadAll(ctx context.Context, writer io.Writer, dsInfo dsInfo) error {
	mapping, err := ds.cfg.Mapping(dsInfo.name)
	if err != nil {
		LOG.Info().Msg("Failed to get mapping for dataset " + dsInfo.name + ". Trying implicit mapping.")
		var err2 error
		mapping, err2 = implicitMapping(dsInfo.name)
		if err2 != nil {
			LOG.Error().Err(err2).Msg("Failed to get implicit mapping for dataset " + dsInfo.name + ".")
			return fmt.Errorf("no table mapping: %w, %w", err, err2)
		}
	}
	if err = ds.sf.ReadAll(ctx, writer, dsInfo, mapping); err != nil {
		return err
	}
	return nil
}

const (
	TableName   = "table_name"
	Schema      = "schema"
	Database    = "database"
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
