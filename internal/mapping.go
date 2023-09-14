package internal

import "encoding/json"

type EntityPropertyMapping struct {
	EntityProperty  string `json:"entity_property"`
	Property        string `json:"property"`
	Datatype        string `json:"datatype"`
	IsReference     bool   `json:"is_reference"`
	UrlValuePattern string `json:"url_value_pattern"`
	IsIdentity      bool   `json:"is_identity"`
}

type SourceConfiguration struct {
	TableName string `json:"table_name"`
	Schema    string `json:"schema"`
	Database  string `json:"database"`
	RawColumn string `json:"raw_column"`
	Query     string `json:"query"`
	MapAll    bool   `json:"map_all"`
}

type DatasetDefinition struct {
	DatasetName         string                  `json:"dataset_name"`
	SourceConfiguration SourceConfiguration     `json:"source_configuration"`
	Mappings            []EntityPropertyMapping `json:"mappings"`
}

type DsMappingConfig struct {
	DatasetDefinitions []DatasetDefinition `json:"dataset_definitions"`
}

func (dsm DatasetDefinition) ToEntity(row map[string]any) (*Entity, error) {
	if dsm.IsRaw() {
		var entity *Entity
		err := json.Unmarshal([]byte(row[dsm.SourceConfiguration.RawColumn].(string)), entity)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (dsm DatasetDefinition) IsRaw() bool {
	return dsm.SourceConfiguration.RawColumn != ""
}