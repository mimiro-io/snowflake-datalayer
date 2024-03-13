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
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"

	gsf "github.com/snowflakedb/gosnowflake"

	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

type query interface {
	withSince(sinceColumn, sinceToken string) (query, error)
	withLimit(limit int) (query, error)
	run(ctx context.Context, releaseConn func()) (common.EntityIterator, common.LayerError)
}

type sfQuery struct {
	datasetDefinition *common.DatasetDefinition
	logger            common.Logger
	ctx               context.Context
	token             string
	queryString       string
}

func (sf *SfDB) createQuery(ctx context.Context, datasetDefinition *common.DatasetDefinition) (query, error) {
	columns := ""
	if colval, ok := datasetDefinition.SourceConfig[RawColumn]; ok {
		columns = colval.(string)
	} else if datasetDefinition.OutgoingMappingConfig != nil && datasetDefinition.OutgoingMappingConfig.MapAll {
		columns = "*"
	} else {
		columns = ColumnDDL(datasetDefinition.OutgoingMappingConfig)
	}
	return &sfQuery{
		datasetDefinition: datasetDefinition,
		queryString: fmt.Sprintf("SELECT %s FROM %s.%s.%s",
			columns,
			datasetDefinition.SourceConfig[Database],
			datasetDefinition.SourceConfig[Schema],
			datasetDefinition.SourceConfig[TableName]),
		logger: sf.logger,
		ctx:    ctx,
		token:  "",
	}, nil
}

// withSince implements query.
func (q *sfQuery) withSince(sinceColumn, sinceToken string) (query, error) {
	newSince := ""
	conn := q.ctx.Value(Connection).(*sql.Conn)

	// if a since is given, build a between where clause
	sinceVal, err := base64.URLEncoding.DecodeString(sinceToken)
	if err != nil {
		q.logger.Error("Failed to decode since token", "error", err)
		return nil, fmt.Errorf("failed to decode since token %s", sinceToken)
	}

	var res any
	maxQ := fmt.Sprintf("SELECT MAX(%s) FROM %s.%s.%s", sinceColumn,
		q.datasetDefinition.SourceConfig[Database],
		q.datasetDefinition.SourceConfig[Schema],
		q.datasetDefinition.SourceConfig[TableName])

	if sinceToken != "" {
		maxQ = fmt.Sprintf("%s WHERE %s > %s", maxQ, sinceColumn, sinceVal)
	}
	q.logger.Debug(maxQ)
	row := conn.QueryRowContext(q.ctx, maxQ)
	if row.Err() != nil {
		q.logger.Error("Failed to read new since value", "error", row.Err())
		return nil, row.Err()
	}
	row.Scan(&res)

	if res == nil {
		res = string(sinceVal)
	}
	newSince = fmt.Sprintf("%v", res)
	q.token = base64.URLEncoding.EncodeToString([]byte(newSince))

	if sinceToken != "" {
		q.queryString = fmt.Sprintf("%s WHERE %s > %s and %s <= %s",
			q.queryString, sinceColumn, sinceVal, sinceColumn, newSince)
	} else {
		// without since, just cap query
		q.queryString = fmt.Sprintf("%s WHERE %s <= %s",
			q.queryString, sinceColumn, newSince)
	}
	return q, nil
}

// withLimit implements query.
func (q *sfQuery) withLimit(limit int) (query, error) {
	q.queryString = fmt.Sprintf("%s LIMIT %v", q.queryString, limit)
	return q, nil
}

// run implements query.
func (q *sfQuery) run(ctx context.Context, releaseConn func()) (common.EntityIterator, common.LayerError) {
	conn := q.ctx.Value(Connection).(*sql.Conn)
	q.logger.Debug(q.queryString)
	qctx := gsf.WithStreamDownloader(ctx)
	rows, err := conn.QueryContext(qctx, q.queryString)
	if err != nil {
		q.logger.Error("failed to query snowflake", "error", err)
		releaseConn()
		return nil, common.Err(err, common.LayerErrorInternal)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		q.logger.Error("failed to get query result column types", "error", err)
		releaseConn()
		return nil, common.Err(err, common.LayerErrorInternal)
	}

	mapper := common.NewMapper(q.logger, nil, q.datasetDefinition.OutgoingMappingConfig)

	return &entIter{
		logger:  q.logger,
		mapping: q.datasetDefinition,
		release: func() {
			if rows != nil {
				rows.Close()
			}
			releaseConn()
		},
		token:    q.token,
		rows:     rows,
		colTypes: colTypes,
		mapper:   mapper,
		rowBuf:   make([]any, len(colTypes)),
	}, nil
}

type entIter struct {
	logger   common.Logger
	mapping  *common.DatasetDefinition
	release  func()
	token    string
	rows     *sql.Rows
	mapper   *common.Mapper
	colTypes []*sql.ColumnType
	rowBuf   []any
}

// Close implements common_datalayer.EntityIterator.
func (i *entIter) Close() common.LayerError {
	i.release()
	return nil
}

// Context implements common_datalayer.EntityIterator.
func (i *entIter) Context() *egdm.Context {
	return nil
}

// Next implements common_datalayer.EntityIterator.
func (i *entIter) Next() (*egdm.Entity, common.LayerError) {
	if i.rows.Next() {
		//
		for x := range i.colTypes {
			i.rowBuf[x] = new(any)
		}

		err := i.rows.Scan(i.rowBuf...)
		if err != nil {
			i.logger.Error("failed to scan row", "error", err)
			return nil, common.Err(err, common.LayerErrorInternal)
		}
		var jsonEntity string
		if i.mapping.SourceConfig[RawColumn] != nil {
			jsonEntity = (*i.rowBuf[0].(*any)).(string)
			entity := egdm.NewEntity()
			json.Unmarshal([]byte(jsonEntity), entity)
			return entity, nil
		} else {
			entity := egdm.NewEntity()
			ri := rowItem(i.rowBuf, i.colTypes)
			err = i.mapper.MapItemToEntity(ri, entity)
			if err != nil {
				i.logger.Error("failed to map row", "error", err, "row", fmt.Sprintf("%+v", ri))
				return nil, common.Err(err, common.LayerErrorInternal)
			}
			return entity, nil

		}
	} else {
		// exhausted or failed
		if i.rows.Err() != nil {
			i.logger.Error("failed to read rows", "error", i.rows.Err())
			return nil, common.Err(i.rows.Err(), common.LayerErrorInternal)
		}
		return nil, nil
	}
}

// Token implements common_datalayer.EntityIterator.
func (i *entIter) Token() (*egdm.Continuation, common.LayerError) {
	c := egdm.NewContinuation()
	c.Token = i.token
	return c, nil
}

type rItem struct {
	line []any
	cols []string
}

func (r rItem) GetValue(name string) any {
	for i, col := range r.cols {
		if col == name {
			val := *r.line[i].(*any)
			return val
		}
	}
	return nil
}

func (r rItem) GetPropertyNames() []string {
	return r.cols
}

func (r rItem) SetValue(name string, value any) { panic("implement me") }
func (r rItem) NativeItem() any                 { return r.line }

func rowItem(line []any, types []*sql.ColumnType) common.Item {
	colNames := make([]string, len(types))
	for i, t := range types {
		colNames[i] = t.Name()
	}

	return rItem{line: line, cols: colNames}
}
