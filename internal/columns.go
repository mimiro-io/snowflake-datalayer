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
	"fmt"

	common "github.com/mimiro-io/common-datalayer"
)

func ColMappings(mapping *common.DatasetDefinition) (string, string, string, string, string) {
	columns := ", entity"
	columnTypes := ", entity variant"
	colExtractions := ", $1::variant as entity"
	colAssignments := ", latest.entity = src.entity"
	srcColExtractions := ", src.entity"

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
				colExtractions = fmt.Sprintf(`%s, %s as %s`, colExtractions, col.Custom["expression"], col.Property)
			} else if col.IsRecorded {
				columnTypes = fmt.Sprintf("%s, %s INTEGER", columnTypes, col.Property)
				colExtractions = fmt.Sprintf(`%s, $1:recorded::integer as recorded`, colExtractions)
			} else if col.IsDeleted {
				columnTypes = fmt.Sprintf("%s, %s BOOLEAN", columnTypes, col.Property)
				colExtractions = fmt.Sprintf(`%s, $1:deleted::boolean as deleted`, colExtractions)
			} else if col.IsIdentity {
				columnTypes = fmt.Sprintf("%s, %s %s", columnTypes, col.Property, t)
				colExtractions = fmt.Sprintf(`%s, $1:id::%s as id`, colExtractions, t)
			} else {
				columnTypes = fmt.Sprintf("%s, %s %s", columnTypes, col.Property, t)
				colExtractions = fmt.Sprintf(`%s, $1:%s:"%s"::%s as %s`, colExtractions, srcMap, col.EntityProperty, t, col.Property)
			}
		}
	}
	return columns[2:], columnTypes[2:], colExtractions[2:], colAssignments[2:], srcColExtractions[2:]
}

func ColumnDDL(config *common.OutgoingMappingConfig) string {
	res := ""
	if config == nil || config.PropertyMappings == nil {
		return "*"
	}
	for _, mapping := range config.PropertyMappings {
		if len(res) > 0 {
			res = res + ", "
		}
		res = res + mapping.Property
	}
	return res
}
