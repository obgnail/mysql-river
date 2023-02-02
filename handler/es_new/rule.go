package es_new

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/obgnail/mysql-river/handler/es_new/elastic"
)

// If you want to sync MySQL data into elasticsearch, you must set a rule to let use know how to do it.
// The mapping rule may thi: schema + table <-> index + document type.
// schema and table is for MySQL, index and document type is for Elasticsearch.
type Rule struct {
	Schema string `toml:"schema"`
	Table  string `toml:"table"`
	Index  string `toml:"index"`
	Type   string `toml:"type"`
	Parent string `toml:"parent"`

	IDColumns        string `toml:"id_columns"`
	HtmlStripColumns string `toml:"html_strip_columns"`
	JSONColumns      string `toml:"json_columns"`
	// Default, a MySQL table field name is mapped to Elasticsearch field name.
	// Sometimes, you want to use different name, e.g, the MySQL file name is title,
	// but in Elasticsearch, you want to name it my_title.
	FieldMapping map[string]string `toml:"field"`

	// MySQL table information
	TableInfo *schema.Table

	CustomActionMapping []*ActionMapping `toml:"action"`

	ActionMapping map[string]*ActionMapping
}

type ActionMapping struct {
	DBAction       string `toml:"db_action"`
	ESAction       string `toml:"es_action"`
	Script         string `toml:"script"`
	ScriptFile     string `toml:"script_file"`
	ScriptedUpsert bool   `toml:"scripted_upsert"`
}

func newDefaultRule(schema string, table string) *Rule {
	r := new(Rule)

	r.Schema = schema
	r.Table = table
	r.Index = table
	r.Type = table
	r.FieldMapping = make(map[string]string)
	r.ActionMapping = make(map[string]*ActionMapping)

	return r
}

func (r *Rule) prepare() error {
	if r.FieldMapping == nil {
		r.FieldMapping = make(map[string]string)
	}

	if len(r.Index) == 0 {
		r.Index = r.Table
	}

	if len(r.Type) == 0 {
		r.Type = r.Index
	}

	if r.ActionMapping == nil {
		r.ActionMapping = make(map[string]*ActionMapping)
	}

	for _, mapping := range r.CustomActionMapping {
		r.ActionMapping[mapping.DBAction] = mapping
	}

	if _, ok := r.ActionMapping[canal.InsertAction]; !ok {
		r.ActionMapping[canal.InsertAction] = &ActionMapping{
			DBAction: canal.InsertAction,
			ESAction: elastic.ActionIndex,
		}
	}

	if _, ok := r.ActionMapping[canal.UpdateAction]; !ok {
		r.ActionMapping[canal.UpdateAction] = &ActionMapping{
			DBAction: canal.UpdateAction,
			ESAction: elastic.ActionUpdate,
		}
	}

	if _, ok := r.ActionMapping[canal.DeleteAction]; !ok {
		r.ActionMapping[canal.DeleteAction] = &ActionMapping{
			DBAction: canal.DeleteAction,
			ESAction: elastic.ActionDelete,
		}
	}

	return nil
}
