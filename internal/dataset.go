package internal

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Dataset struct {
	cfg  Config
	log  zerolog.Logger
	sf   *Snowflake
	lock sync.Mutex
	//m    statsd.ClientInterface
}

func NewDataset(cfg Config, sf *Snowflake) *Dataset {
	return &Dataset{
		cfg: cfg,
		log: LOG.With().Str("logger", "dataset").Logger(),
		sf:  sf,
		//m:   m,
	}
}

func (ds *Dataset) WriteFs(ctx context.Context, info dsInfo, reader io.Reader) error {
	var stage string
	if info.fsStart && info.fsId != "" {
		var err error
		stage, err = ds.sf.mkStage(info.fsId, info.name)
		if err != nil {
			refreshed, err2 := ds.tryRefresh(err)
			if err2 != nil {
				return err2
			}
			if refreshed {
				stage, err = ds.sf.mkStage(info.fsId, info.name)
			} else {
				ds.log.Error().Str("stage", stage).Msg("Failed to create stage, even after login refresh")
				return err
			}
		}
		ds.log.Info().Str("stage", stage).Msg("Created stage")
	} else {
		stage = ds.sf.getStage(info.fsId, info.name)
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
				var err error
				err = ds.safePut(info.name, stage, entityContext, entities)
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
	if s, ok := ctx.Value("batchSize").(int64); ok {
		batchSize = s
	}

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
				if err3 != nil {
					return err3 // give up at this point
				}
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
			if f, err3 := ds.sf.EnsureStageAndPut(ctx, dataset, entityContext, entities); err3 != nil {
				if err3 != nil {
					return 0, nil, nil, err3 // give up at this point
				}
			} else {
				files = append(files, f...)
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