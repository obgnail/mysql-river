package es

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/juju/errors"
	"strings"
)

type Rule struct {
	Schema string   `json:"schema"`
	Table  string   `json:"table"`
	Index  string   `json:"index"`
	Type   string   `json:"type"`
	Parent string   `json:"parent"`
	ID     []string `json:"id"`

	// Default, a MySQL table field name is mapped to Elasticsearch field name.
	// Sometimes, you want to use different name, e.g, the MySQL file name is title,
	// but in Elasticsearch, you want to name it my_title.
	FieldMapping map[string]string `json:"field_mapping"`

	// MySQL table information
	TableInfo *schema.Table

	//only MySQL fields in filter will be synced , default sync all fields
	Filter []string `json:"filter"`

	// Elasticsearch pipeline
	// To pre-process documents before indexing
	Pipeline string `json:"pipeline"`
}

func RuleKey(schema string, table string) string {
	return strings.ToLower(fmt.Sprintf("%s:%s", schema, table))
}

func NewRule(schema string, table string, canal *canal.Canal, skipNoPkTable bool) (*Rule, error) {
	tableInfo, err := canal.GetTable(schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(tableInfo.PKColumns) == 0 && !skipNoPkTable {
		return nil, errors.Errorf("%s.%s must have a PK for a column", schema, table)
	}
	lowerTable := strings.ToLower(table)
	r := &Rule{
		Schema:       schema,
		Table:        table,
		Index:        lowerTable,
		Type:         lowerTable,
		TableInfo:    tableInfo,
		FieldMapping: make(map[string]string),
	}
	return r, nil
}

// CheckFilter checkers whether the field needs to be filtered.
func (r *Rule) CheckFilter(field string) bool {
	if r.Filter == nil {
		return true
	}

	for _, f := range r.Filter {
		if f == field {
			return true
		}
	}
	return false
}
