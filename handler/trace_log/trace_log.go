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

type Config struct {
	DBs          []string
	EntireFields bool // show all field message in update sql
	ShowTxMsg    bool // show transition msg in sql
	Highlight    bool // sql highlight
}

type TraceLogHandler struct {
	config *Config
	dbs    map[string]struct{} // map[db]struct{}

	river.NopCloserAlerter
}

var _ river.Handler = (*TraceLogHandler)(nil)

func New(config *Config) *TraceLogHandler {
	return &TraceLogHandler{dbs: list2map(config.DBs), config: config}
}

func (t *TraceLogHandler) String() string {
	return "trace log"
}

func (t *TraceLogHandler) OnEvent(event *river.EventData) error {
	var data string

	switch event.EventType {
	case river.EventTypeUpdate, river.EventTypeInsert, river.EventTypeDelete:
		data = t.handlerRow(event)
	case river.EventTypeGTID:
		if t.config.ShowTxMsg {
			data = fmt.Sprintf("/* GTID: %s */", event.GTIDSet)
		}
	case river.EventTypeXID:
		if t.config.ShowTxMsg {
			data = fmt.Sprintf("/* XID: %s */", event.Position())
		}
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
		sql = GenUpdateSql(event, t.config.Highlight, t.config.EntireFields)
	case river.EventTypeInsert:
		sql = GenInsertSql(event, t.config.Highlight)
	case river.EventTypeDelete:
		sql = GenDeleteSql(event, t.config.Highlight)
	}
	return
}

func GenUpdateSql(event *river.EventData, highlight bool, showAllField bool) string {
	var setFields []string
	if !showAllField {
		setFields = buildSimpleSqlKVExp(event.Before, event.After)
	} else {
		setFields = buildSqlKVExp(event.After, false)
	}
	whereFields := buildSqlKVExp(event.Before, true)

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
	kv := buildSqlKVExp(event.Before, true)

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

func buildSqlKVExp(kv map[string]interface{}, inWhere bool) []string {
	var res []string
	for field, value := range kv {
		valueStr := buildSqlValue(value)
		res = append(res, buildEqualExp(field, valueStr, inWhere))
	}
	return res
}

func buildSimpleSqlKVExp(before, after map[string]interface{}) []string {
	var res []string
	for field, value := range after {
		afterValue := buildSqlValue(value)
		beforeValue := buildSqlValue(before[field])
		if beforeValue != afterValue {
			res = append(res, buildEqualExp(field, afterValue, false))
		}
	}
	return res
}

func buildSqlValue(value interface{}) string {
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

func buildEqualExp(key, value string, inWhere bool) string {
	// if v is NULL, may need to process
	if inWhere && value == "NULL" {
		return fmt.Sprintf("`%s` IS %s", key, value)
	}
	// in Set
	return fmt.Sprintf("`%s`=%s", key, value)
}

func list2map(slice []string) map[string]struct{} {
	res := make(map[string]struct{})
	for _, ele := range slice {
		res[strings.ToLower(ele)] = struct{}{}
	}
	return res
}

func map2list(kv map[string]interface{}) (fields []string, values []string) {
	for filed, value := range kv {
		fields = append(fields, fmt.Sprintf("`%s`", filed))
		values = append(values, buildSqlValue(value))
	}
	return
}
