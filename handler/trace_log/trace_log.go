package trace_log

import (
	"fmt"
	"github.com/obgnail/mysql-river/river"
	"reflect"
	"strings"
)

const (
	SqlInsertFormat = "\u001B[32mINSERT INTO\u001B[0m `%s`.`\u001B[30;46m%s\u001B[0m`(%s) \u001B[32mVALUES\u001B[0m (%s);"
	SqlUpdateFormat = "\u001B[33mUPDATE\u001B[0m `%s`.`\u001B[30;46m%s\u001B[0m` \u001B[33mSET\u001B[0m %s \u001B[33mWHERE\u001B[0m %s LIMIT 1;"
	SqlDeleteFormat = "\u001B[31mDELETE FROM\u001B[0m `%s`.`\u001B[30;46m%s\u001B[0m` \u001B[31mWHERE\u001B[0m %s LIMIT 1;"

	SqlNormalInsertFormat = "INSERT INTO `%s`.`%s`(%s) VALUES (%s);"
	SqlNormalUpdateFormat = "UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;"
	SqlNormalDeleteFormat = "DELETE FROM `%s`.`%s` WHERE %s LIMIT 1;"
)

const (
	InvalidFormat = "[InvalidFieldValue]: '%v'"
)

type TraceLogHandler struct {
	showAllField     bool // show all field message in update sql
	showQueryMessage bool // show transition msg in sql
	highlight        bool // sql highlight
	dbs              map[string]struct{}
}

func list2Map(slice []string) map[string]struct{} {
	res := make(map[string]struct{})
	for _, ele := range slice {
		res[strings.ToLower(ele)] = struct{}{}
	}
	return res
}

func New(dbs []string, showAllField, showQueryMessage, highlight bool) *TraceLogHandler {
	return &TraceLogHandler{
		dbs:              list2Map(dbs),
		showAllField:     showAllField,
		showQueryMessage: showQueryMessage,
		highlight:        highlight,
	}
}

func (t *TraceLogHandler) String() string {
	return "trace log"
}

func (t *TraceLogHandler) Show(event *river.EventData) error {
	var data string
	switch event.EventType {
	case river.EventTypeUpdate, river.EventTypeInsert, river.EventTypeDelete:
		data = t.handlerRow(event)
	case river.EventTypeGTID:
		data = fmt.Sprintf("/* GTID: %s */", event.GTIDSet)
	case river.EventTypeXID:
		data = fmt.Sprintf("/* XID: %s */", event.Position())
	case river.EventTypeDDL:
		data = event.SQL
	}
	if len(data) != 0 {
		fmt.Println(data)
	}
	return nil
}

func (t *TraceLogHandler) handlerRow(event *river.EventData) (sql string) {
	if len(t.dbs) != 0 {
		if _, ok := t.dbs[event.Db]; !ok {
			return ""
		}
	}

	switch event.EventType {
	case river.EventTypeUpdate:
		sql = GenUpdateSql(event, t.highlight, t.showAllField)
	case river.EventTypeInsert:
		sql = GenInsertSql(event, t.highlight)
	case river.EventTypeDelete:
		sql = GenDeleteSql(event, t.highlight)
	}
	return
}

func map2list(kv map[string]interface{}) (fields []string, values []string) {
	for filed, value := range kv {
		fields = append(fields, fmt.Sprintf("`%s`", filed))
		values = append(values, buildSqlFieldValue(value))
	}
	return
}

func GenUpdateSql(event *river.EventData, highlight bool, more bool) string {
	var setFields []string
	if !more {
		setFields = buildUpdateSqlSimpleFieldsExp(event.Before, event.After)
	} else {
		setFields = buildSqlFieldsExp(event.After, false)
	}
	whereFields := buildSqlFieldsExp(event.Before, true)

	formatter := SqlNormalUpdateFormat
	if highlight {
		formatter = SqlUpdateFormat
	}
	content := fmt.Sprintf(
		formatter,
		event.Db,
		event.Table,
		strings.Join(setFields, ", "),
		strings.Join(whereFields, " AND "),
	)
	return content
}

func GenInsertSql(event *river.EventData, highlight bool) string {
	fields, values := map2list(event.After)

	formatter := SqlNormalInsertFormat
	if highlight {
		formatter = SqlInsertFormat
	}
	content := fmt.Sprintf(
		formatter,
		event.Db,
		event.Table,
		strings.Join(fields, ", "),
		strings.Join(values, ", "),
	)
	return content
}

func GenDeleteSql(event *river.EventData, highlight bool) string {
	kv := buildSqlFieldsExp(event.Before, true)

	formatter := SqlNormalDeleteFormat
	if highlight {
		formatter = SqlDeleteFormat
	}
	content := fmt.Sprintf(
		formatter,
		event.Db,
		event.Table,
		strings.Join(kv, " AND "),
	)
	return content
}

func buildSqlFieldValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}

	fieldType := reflect.TypeOf(value)
	switch fieldType.Kind() {
	case reflect.String:
		return fmt.Sprintf("'%v'", value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%v", value)
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%v", value)
	case reflect.Bool:
		return fmt.Sprintf("%v", value)
		// text, longtext
	case reflect.Slice:
		s, ok := reflect.ValueOf(value).Interface().([]byte)
		if !ok {
			return fmt.Sprintf(InvalidFormat, value)
		}
		return fmt.Sprintf("'%s'", string(s))
	case reflect.Uintptr, reflect.Complex64, reflect.Complex128, reflect.Array, reflect.Chan,
		reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Struct,
		reflect.UnsafePointer:
		return fmt.Sprintf(InvalidFormat, value)
	case reflect.Invalid:
		return fmt.Sprintf(InvalidFormat, value)
	default:
		return fmt.Sprintf(InvalidFormat, value)
	}
}

func buildSqlFieldsExp(kv map[string]interface{}, inWhere bool) []string {
	var res []string
	for field, value := range kv {
		valueStr := buildSqlFieldValue(value)
		res = append(res, buildEqualExp(field, valueStr, inWhere))
	}
	return res
}

func buildEqualExp(key, value string, inWhere bool) string {
	// if v is NULL, may need to process
	if inWhere && value == "NULL" {
		return fmt.Sprintf("`%s` IS %s", key, value)
	}
	// in Set
	return fmt.Sprintf("`%s`=%s", key, value)
}

func buildUpdateSqlSimpleFieldsExp(before, after map[string]interface{}) []string {
	var res []string
	for field, value := range after {
		afterValue := buildSqlFieldValue(value)
		beforeValue := buildSqlFieldValue(before[field])
		if beforeValue != afterValue {
			res = append(res, buildEqualExp(field, afterValue, false))
		}
	}
	return res
}
