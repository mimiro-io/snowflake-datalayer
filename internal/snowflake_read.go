package internal

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	common_datalayer "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	gsf "github.com/snowflakedb/gosnowflake"
)

func (sf *Snowflake) ReadAll(ctx context.Context, writer io.Writer, info dsInfo, mapping *common_datalayer.DatasetDefinition) error {
	_, err := withRefresh(sf, func() (any, error) {
		return WithConn(p, ctx, func(conn *sql.Conn) (any, error) {
			var err error
			var rows *sql.Rows
			var query string
			columns := ""
			sinceColumn, sinceActive := mapping.SourceConfig[SinceColumn]

			if colval, ok := mapping.SourceConfig[RawColumn]; ok {
				columns = colval.(string)
			} else if mapping.OutgoingMappingConfig.MapAll {
				columns = "*"
			} else {
				columns = cols(mapping.OutgoingMappingConfig)
			}

			// if a since is given, build a between where clause
			sinceVal, err := base64.URLEncoding.DecodeString(info.since)
			if err != nil {
				sf.log.Error().Err(err).Msg("Failed to decode since token")
				return nil, err
			}

			token := info.since
			newSince := ""
			if sinceActive {

				var res any
				maxQ := fmt.Sprintf("SELECT MAX(%s) FROM %s.%s.%s", sinceColumn, mapping.SourceConfig[Database], mapping.SourceConfig[Schema], mapping.SourceConfig[TableName])

				if info.since != "" {
					maxQ = fmt.Sprintf("%s WHERE %s > %s", maxQ, sinceColumn, sinceVal)
				}
				sf.log.Debug().Msg(maxQ)
				row := conn.QueryRowContext(ctx, maxQ)
				if row.Err() != nil {
					sf.log.Error().Err(row.Err()).Msg("Failed to read new since value")
					return nil, row.Err()
				}
				row.Scan(&res)

				if res == nil {
					res = string(sinceVal)
				}
				newSince = fmt.Sprintf("%v", res)
				token = base64.URLEncoding.EncodeToString([]byte(newSince))
			}

			query = fmt.Sprintf("SELECT %s FROM %s.%s.%s", columns, mapping.SourceConfig[Database], mapping.SourceConfig[Schema], mapping.SourceConfig[TableName])

			if sinceActive {
				if info.since != "" {
					query = fmt.Sprintf("%s WHERE %s > %s and %s <= %s", query, sinceColumn, sinceVal, sinceColumn, newSince)
				} else {
					// without since, just cap query
					query = fmt.Sprintf("%s WHERE %s <= %s", query, sinceColumn, newSince)
				}
			}

			sf.log.Debug().Msg(query)
			qctx := gsf.WithStreamDownloader(ctx)
			rows, err = conn.QueryContext(qctx, query)
			if err != nil {
				sf.log.Error().Err(err).Msg("Failed to query snowflake")
				return nil, err
			}
			defer rows.Close()

			var headerWritten bool

			colTypes, err := rows.ColumnTypes()
			if err != nil {
				sf.log.Error().Err(err).Msg("Failed to access query result column types")
				return nil, err
			}
			rowLine := make([]any, len(colTypes))

			mapper := common_datalayer.NewMapper(nil, nil, mapping.OutgoingMappingConfig)

			for rows.Next() {
				for i := range colTypes {
					rowLine[i] = new(any)
				}

				err = rows.Scan(rowLine...)
				if err != nil {
					sf.log.Error().Err(err).Msg("Failed to scan row")
					return nil, err
				}
				var jsonEntity string
				if mapping.SourceConfig[RawColumn] != nil {
					jsonEntity = (*rowLine[0].(*any)).(string)
				} else {
					entity := egdm.NewEntity()
					err = mapper.MapItemToEntity(rowItem(rowLine, colTypes), entity)
					if err != nil {
						sf.log.Error().Err(err).Msg("Failed to map row")
						return nil, err
					}
					jsonBytes, err2 := json.Marshal(entity)
					if err2 != nil {
						return nil, err2
					}
					jsonEntity = string(jsonBytes)
				}

				// pushed writing of header as far back as possible, so that errors before this point still can influnce http status code
				if !headerWritten {
					err2 := sf.writeHeader(writer, mapping)
					if err2 != nil {
						return nil, err2
					}

					headerWritten = true
				}

				_, err2 := fmt.Fprint(writer, ",\n"+jsonEntity)
				if err2 != nil {
					sf.log.Error().Err(err2).Msg("Failed to write entity to http writer")
					return nil, err2
				}
			}
			if err = rows.Err(); err != nil {
				sf.log.Error().Err(err).Msg("Failed to read rows")
				return nil, err
			}

			// when there was an empty result, we still need to write the header
			if !headerWritten {
				err := sf.writeHeader(writer, mapping)
				if err != nil {
					return nil, err
				}
			}
			// append continuation token if sinceActive
			if sinceActive {
				_, err = fmt.Fprintf(writer, `, {"id": "@continuation", "token": "%s"}`, token)
			}

			// close batch
			_, err = fmt.Fprintln(writer, "]")
			if err != nil {
				return nil, err
			}
			return nil, nil
		})
	})
	return err
}

func (sf *Snowflake) writeHeader(writer io.Writer, mapping *common_datalayer.DatasetDefinition) error {
	_, err := fmt.Fprintf(writer, `[{"id": "@context", "namespaces": {
"_": "http://snowflake/%s/%s/%s/",
"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
}}`, mapping.SourceConfig[Database], mapping.SourceConfig[Schema], mapping.SourceConfig[TableName])
	if err != nil {
		sf.log.Error().Err(err).Msg("Failed to write context")
		return err
	}
	return nil
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
func (r rItem) NativeItem() any                 { panic("implement me") }

func rowItem(line []any, types []*sql.ColumnType) common_datalayer.Item {
	colNames := make([]string, len(types))
	for i, t := range types {
		colNames[i] = t.Name()
	}

	return rItem{line: line, cols: colNames}
}

func cols(config *common_datalayer.OutgoingMappingConfig) string {
	res := ""
	for _, mapping := range config.PropertyMappings {
		if len(res) > 0 {
			res = res + ", "
		}
		res = res + mapping.Property
	}
	return res
}
