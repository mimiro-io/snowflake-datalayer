package internal

import (
	"context"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/mimiro-io/internal-go-util/pkg/uda"
	"github.com/rs/zerolog"
	"io"
)

type Dataset struct {
	cfg Config
	log zerolog.Logger
	sf  *Snowflake
}

func NewDataset(cfg Config, sf *Snowflake, _ statsd.ClientInterface) *Dataset {
	return &Dataset{
		cfg: cfg,
		log: LOG.With().Str("logger", "dataset").Logger(),
		sf:  sf,
	}
}

func (ds *Dataset) Write(ctx context.Context, dataset string, reader io.Reader) error {
	var batchSize int64 = 10000
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
				read = 0
				if err := ds.sf.Put(ctx, dataset, entityContext, entities); err != nil {
					return err
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
		return ds.sf.Put(ctx, dataset, entityContext, entities)
	}
	return nil
}
