package api

import (
	"fmt"

	common "github.com/mimiro-io/common-datalayer"
)

func ColMappings(mapping *common.DatasetDefinition) (string, string, string) {
	columns := ", entity"
	columnTypes := ", entity variant"
	colExtractions := ", $1::variant"
	if mapping.IncomingMappingConfig != nil && mapping.IncomingMappingConfig.PropertyMappings != nil {
		columns = ""
		columnTypes = ""
		colExtractions = ""
		for _, col := range mapping.IncomingMappingConfig.PropertyMappings {
			srcMap := "props"
			t := col.Datatype
			// if no datatype is specified, we assume string
			if t == "" {
				t = "string"
			}
			if col.IsReference {
				srcMap = "refs"
			}
			columns = fmt.Sprintf("%s, %s", columns, col.Property)
			if col.Custom != nil && col.Custom["expression"] != nil {
				// if a Custom expression is provided, we expect it to be a SQL expression,
				// like "now()::timestamp" or "$1.props:myprop::string"
				columnTypes = fmt.Sprintf("%s, %s %s", columnTypes, col.Property, col.Datatype)
				colExtractions = fmt.Sprintf(`%s, %s`, colExtractions, col.Custom["expression"])
			} else if col.IsRecorded {
				columnTypes = fmt.Sprintf("%s, %s INTEGER", columnTypes, col.Property)
				colExtractions = fmt.Sprintf(`%s, $1:recorded::integer`, colExtractions)
			} else if col.IsDeleted {
				columnTypes = fmt.Sprintf("%s, %s BOOLEAN", columnTypes, col.Property)
				colExtractions = fmt.Sprintf(`%s, $1:deleted::boolean`, colExtractions)
			} else if col.IsIdentity {
				columnTypes = fmt.Sprintf("%s, %s %s", columnTypes, col.Property, t)
				colExtractions = fmt.Sprintf(`%s, $1:id::%s`, colExtractions, t)
			} else {
				columnTypes = fmt.Sprintf("%s, %s %s", columnTypes, col.Property, t)
				colExtractions = fmt.Sprintf(`%s, $1:%s:"%s"::%s`, colExtractions, srcMap, col.EntityProperty, t)
			}
		}
	}
	return columns[2:], columnTypes[2:], colExtractions[2:]
}
