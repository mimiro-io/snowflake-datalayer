package internal

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	common_datalayer "github.com/mimiro-io/common-datalayer"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Dataset struct {
	cfg  *Config
	log  zerolog.Logger
	sf   *Snowflake
	lock sync.Mutex
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
			refreshed, err2 := ds.tryRefresh(err)
			if err2 != nil {
				return err2
			}
			if refreshed {
				stage, err = ds.sf.mkStage(info.fsID, info.name)
				if err != nil {
					ds.log.Error().Err(err).Str("stage", stage).Msg("Failed to create stage, even after login refresh")
					return err
				}
			} else {
				ds.log.Error().Err(err).Str("stage", stage).Msg("Failed to create stage, tis is not a refresh issue")
				return err
			}
		}
		ds.log.Info().Str("stage", stage).Msg("Created stage")
	} else {
		stage = ds.sf.getStage(info.fsID, info.name)
	}
	var batchSize int64 = 50000
	if s, ok := ctx.Value("batchSize").(int64); ok {
		batchSize = s
	}

	isFirst := true
	var read int64 = 0
	entities := make([]*Entity, 0)
	esp := NewEntityStreamParser()

	var entityContext *uda.Context
	err := esp.ParseStream(reader, func(entity *Entity) error {
		if isFirst {
			isFirst = false
			entityContext = AsContext(entity)
		} else {
			if entity.ID != "@continuation" {
				entities = append(entities, entity)
				read++
			}
			if read == batchSize {
				err := ds.safePut(info.name, stage, entityContext, entities)
				if err != nil {
					return err
				}
				read = 0
				entities = make([]*Entity, 0)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if read > 0 {
		err = ds.safePut(info.name, stage, entityContext, entities)
		if err != nil {
			return err
		}
	}
	if info.fsEnd {
		ds.log.Info().Str("stage", stage).Msg("Loading fullsync stage")
		err := ds.sf.LoadStage(info.name, stage, ctx.Value("recorded").(int64))
		if err != nil {
			refreshed, err2 := ds.tryRefresh(err)
			if err2 != nil {
				return err2
			}
			if refreshed {
				return ds.sf.LoadStage(info.name, stage, ctx.Value("recorded").(int64))
			} else {
				return err
			}
		}
	}
	return nil
}

func (ds *Dataset) Write(ctx context.Context, dataset string, reader io.Reader) error {
	var batchSize int64 = 50000

	isFirst := true
	var read int64 = 0
	entities := make([]*Entity, 0)
	esp := NewEntityStreamParser()
	files := make([]string, 0)

	var entityContext *uda.Context
	err := esp.ParseStream(reader, func(entity *Entity) error {
		if isFirst {
			isFirst = false
			entityContext = AsContext(entity)
		} else {
			if entity.ID != "@continuation" {
				entities = append(entities, entity)
				read++
			}
			if read == batchSize {
				var err error
				read, entities, files, err = ds.safeEnsureStageAndPut(ctx, dataset, entityContext, entities, files)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if read > 0 {
		read, entities, files, err = ds.safeEnsureStageAndPut(ctx, dataset, entityContext, entities, files)
		if err != nil {
			return err
		}
	}
	if len(files) > 0 {

		err := ds.sf.Load(dataset, files, ctx.Value("recorded").(int64))
		if err != nil {
			refreshed, err2 := ds.tryRefresh(err)
			if err2 != nil {
				return err2
			}
			if refreshed {
				return ds.sf.Load(dataset, files, ctx.Value("recorded").(int64))
			} else {
				return err
			}
		}
	}

	return nil
}

func (ds *Dataset) safePut(dataset string, stage string, entityContext *uda.Context, entities []*Entity) error {
	if _, err := ds.sf.putEntities(dataset, stage, entities, entityContext); err != nil {
		refreshed, err2 := ds.tryRefresh(err)
		if err2 != nil {
			// failed to reset snowflake driver
			return err2
		}
		if refreshed {
			if _, err3 := ds.sf.putEntities(dataset, stage, entities, entityContext); err3 != nil {
				return err3 // give up at this point
			}
		} else {
			return err
		}
	}
	return nil
}

func (ds *Dataset) safeEnsureStageAndPut(ctx context.Context, dataset string, entityContext *uda.Context, entities []*Entity, files []string) (int64, []*Entity, []string, error) {
	if f, err := ds.sf.EnsureStageAndPut(ctx, dataset, entityContext, entities); err != nil {
		refreshed, err2 := ds.tryRefresh(err)
		if err2 != nil {
			// failed to reset snowflake driver
			return 0, nil, nil, err2
		}
		if refreshed {
			if f2, err3 := ds.sf.EnsureStageAndPut(ctx, dataset, entityContext, entities); err3 != nil {
				return 0, nil, nil, err3 // give up at this point
			} else {
				files = append(files, f2...)
			}
		} else {
			return 0, nil, nil, err
		}
	} else {
		files = append(files, f...)
	}
	entities = make([]*Entity, 0)
	return 0, entities, files, nil
}

func (ds *Dataset) tryRefresh(err error) (bool, error) {
	ds.lock.Lock()
	defer ds.lock.Unlock()
	if strings.Contains(err.Error(), "390114") {
		ds.log.Info().Msg("Refreshing snowflake connection")
		s, err := NewSnowflake(ds.cfg)
		if err != nil {
			ds.log.Error().Err(err).Msg("Failed to reconnect to snowflake")
			return false, err

		}
		ds.sf = s
		log.Info().Msg("Reconnected to snowflake")
		return true, nil
	}
	return false, nil
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
		refreshed, err2 := ds.tryRefresh(err)
		if err2 != nil {
			return err2
		}
		if refreshed {
			return ds.sf.ReadAll(ctx, writer, dsInfo, mapping)
		} else {
			return err
		}
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