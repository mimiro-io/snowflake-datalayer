package internal

import (
	"context"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/rs/zerolog"
	"io"
	"strings"
	"sync"
)

type Dataset struct {
	cfg  Config
	log  zerolog.Logger
	sf   *Snowflake
	lock sync.Mutex
	m    statsd.ClientInterface
}

func NewDataset(cfg Config, sf *Snowflake, m statsd.ClientInterface) *Dataset {
	return &Dataset{
		cfg: cfg,
		log: LOG.With().Str("logger", "dataset").Logger(),
		sf:  sf,
		m:   m,
	}
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
				read = 0
				if f, err := ds.sf.Put(ctx, dataset, entityContext, entities); err != nil {
					refreshed, err2 := ds.tryRefresh(err)
					if err2 != nil {
						// failed to reset snowflake driver
						return err2
					}
					if refreshed {
						if f, err3 := ds.sf.Put(ctx, dataset, entityContext, entities); err != nil {
							if err3 != nil {
								return err3 // give up at this point
							}
						} else {
							files = append(files, f...)
						}

					} else {
						return err
					}
				} else {
					files = append(files, f...)
				}
				entities = make([]*Entity, 0)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if read > 0 {
		if f, err := ds.sf.Put(ctx, dataset, entityContext, entities); err != nil {
			refreshed, err2 := ds.tryRefresh(err)
			if err2 != nil {
				// failed to reset snowflake driver
				return err2
			}
			if refreshed {
				if f, err3 := ds.sf.Put(ctx, dataset, entityContext, entities); err != nil {
					if err3 != nil {
						return err3 // give up at this point
					}
				} else {
					files = append(files, f...)
				}

			} else {
				return err
			}
		} else {
			files = append(files, f...)
		}
	}
	if len(files) > 0 {
		err := ds.sf.Load(dataset, files)
		if err != nil {
			refreshed, err2 := ds.tryRefresh(err)
			if err2 != nil {
				return err2
			}
			if refreshed {
				return ds.sf.Load(dataset, files)
			} else {
				return err
			}
		}
	}

	return nil
}

func (ds *Dataset) tryRefresh(err error) (bool, error) {
	ds.lock.Lock()
	defer ds.lock.Unlock()
	if strings.Contains(err.Error(), "390114") {
		s, err := NewSnowflake(ds.cfg, ds.m)
		if err != nil {
			return false, err

		}
		ds.sf = s
		return true, nil
	}
	return false, nil
}
