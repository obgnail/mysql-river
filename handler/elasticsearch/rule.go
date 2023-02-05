package elasticsearch

import (
	"bytes"
	"fmt"
	"github.com/obgnail/mysql-river/river"
)

type Rule struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`

	Index  string `json:"index"`
	Parent string `json:"parent"`
	// If id is none, get primary keys in one row and format them into a string
	ID string `json:"id"`

	// Default, a MySQL table field name is mapped to Elasticsearch field name.
	// Sometimes, you want to use different name, e.g, the MySQL file name is title,
	// but in Elasticsearch, you want to name it my_title.
	FieldMapping map[string]string `json:"field_mapping"`

	//only MySQL fields in filter will be synced , default sync all fields
	Filter []string `json:"filter"`

	// Elasticsearch pipeline
	// To pre-process documents before indexing
	Pipeline string `json:"pipeline"`

	ActionMapping []*ActionMapping `json:"action_mapping"`
}

type ActionMapping struct {
	DBAction string `json:"db_action"`
	ESAction string `json:"es_action"`
}

func NewDefaultRule(schema string, table string) *Rule {
	r := &Rule{
		Schema:        schema,
		Table:         table,
		Index:         table,
		Parent:        "",
		ID:            "",
		FieldMapping:  make(map[string]string),
		Filter:        nil,
		Pipeline:      "",
		ActionMapping: nil,
	}
	return r
}

func (r *Rule) getID(event *river.EventData) string {
	var PkField string
	if len(r.ID) != 0 {
		PkField = r.ID
	} else {
		var buf bytes.Buffer
		sep := ""
		for _, value := range event.Primary {
			buf.WriteString(fmt.Sprintf("%s%v", sep, value))
			sep = ":"
		}
		PkField = buf.String()
	}

	dataset := event.After
	if len(dataset) == 0 {
		dataset = event.Before
	}
	for field, value := range dataset {
		if field == PkField {
			return fmt.Sprintf("%v", value)
		}
	}
	return ""
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

func (r *Rule) getAction(event *river.EventData) string {
	var dbAction string
	switch event.EventType {
	case river.EventTypeInsert:
		dbAction = ActionCreate
	case river.EventTypeUpdate:
		dbAction = ActionUpdate
	case river.EventTypeDelete:
		dbAction = ActionDelete
	}
	for _, mapping := range r.ActionMapping {
		if mapping.DBAction == dbAction {
			dbAction = mapping.DBAction
		}
	}
	return dbAction
}

func (r *Rule) getData(event *river.EventData) (data map[string]interface{}) {
	kv := event.After
	if event.EventType == river.EventTypeDelete {
		kv = event.Before
	}

	data = make(map[string]interface{})
	for filed, value := range kv {
		if newField, ok := r.FieldMapping[filed]; ok {
			filed = newField
		}
		if r.CheckFilter(filed) {
			data[filed] = value
		}
	}
	return
}

func (r *Rule) ToReqs(event *river.EventData) (reqs []*BulkRequest) {
	reqs = append(reqs, &BulkRequest{
		Action:   r.getAction(event),
		Index:    r.Index,
		ID:       r.getID(event),
		Parent:   r.Parent,
		Pipeline: r.Pipeline,
		Data:     r.getData(event),
	})
	return
}
