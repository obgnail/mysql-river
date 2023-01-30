package trace_log

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/obgnail/mysql-river/handler/common"
	"reflect"
	"runtime/debug"
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

type TraceLogHandler struct {
	dbs map[string]struct{}

	showAllField     bool // show all field message in update sql
	showQueryMessage bool // show transition msg in sql

	*canal.DummyEventHandler
}

func NewTraceLogHandler(dbs []string, showAllField, showQueryMessage bool) *TraceLogHandler {
	return &TraceLogHandler{
		dbs:               common.List2Map(dbs),
		showAllField:      showAllField,
		showQueryMessage:  showQueryMessage,
		DummyEventHandler: new(canal.DummyEventHandler),
	}
}

func (t *TraceLogHandler) String() string {
	return "trace log"
}

func (t *TraceLogHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	if t.showQueryMessage {
		fmt.Printf("/* %s */", string(queryEvent.Query))
	}
	return nil
}

func (t *TraceLogHandler) OnXID(nextPos mysql.Position) error {
	if t.showQueryMessage {
		fmt.Println("/* XID */")
	}
	return nil
}

func (t *TraceLogHandler) OnGTID(gtid mysql.GTIDSet) error {
	if t.showQueryMessage {
		fmt.Printf("/* %s */", gtid.String())
	}
	return nil
}

func (t *TraceLogHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()

	var sql string

	if len(t.dbs) != 0 {
		if _, ok := t.dbs[e.Table.Schema]; !ok {
			return nil
		}
	}

	switch e.Action {
	case canal.UpdateAction:
		sql = GenUpdateSql(e, true, t.showAllField)
	case canal.InsertAction:
		sql = GenInsertSql(e, true)
	case canal.DeleteAction:
		sql = GenDeleteSql(e, true)
	}

	fmt.Println(sql)

	return nil
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
			return fmt.Sprintf("---Invalid---: '%v'", value)
		}
		return fmt.Sprintf("'%s'", string(s))
	case reflect.Uintptr, reflect.Complex64, reflect.Complex128, reflect.Array, reflect.Chan,
		reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Struct,
		reflect.UnsafePointer:
		return fmt.Sprintf("---Invalid---: '%v'", value)
	case reflect.Invalid:
		return fmt.Sprintf("---Invalid---: '%v'", value)
	default:
		return fmt.Sprintf("---Invalid---: '%v'", value)
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

func buildSqlFieldsExp(columns []schema.TableColumn, fields []interface{}, inWhere bool) []string {
	res := make([]string, len(fields))
	for idx, field := range fields {
		key := columns[idx].Name
		value := buildSqlFieldValue(field)
		res[idx] = buildEqualExp(key, value, inWhere)
	}
	return res
}

func buildUpdateSqlSimpleFieldsExp(columns []schema.TableColumn, whereFields []interface{}, setFields []interface{}) []string {
	var res []string
	for idx, col := range columns {
		where := buildSqlFieldValue(whereFields[idx])
		set := buildSqlFieldValue(setFields[idx])
		if reflect.DeepEqual(where, set) {
			continue
		}
		res = append(res, buildEqualExp(col.Name, set, false))
	}
	return res
}

func GenUpdateSql(e *canal.RowsEvent, highlight bool, more bool) string {
	var setFields []string
	if !more {
		setFields = buildUpdateSqlSimpleFieldsExp(e.Table.Columns, e.Rows[0], e.Rows[1])
	} else {
		setFields = buildSqlFieldsExp(e.Table.Columns, e.Rows[1], false)
	}
	whereFields := buildSqlFieldsExp(e.Table.Columns, e.Rows[0], true)
	formatter := SqlNormalUpdateFormat
	if highlight {
		formatter = SqlUpdateFormat
	}
	content := fmt.Sprintf(
		formatter,
		e.Table.Schema,
		e.Table.Name,
		strings.Join(setFields, ", "),
		strings.Join(whereFields, " AND "),
	)
	return content
}

func GenInsertSql(e *canal.RowsEvent, highlight bool) string {
	fields := make([]string, len(e.Rows[0]))
	values := make([]string, len(e.Rows[0]))
	for idx, field := range e.Rows[0] {
		fields[idx] = fmt.Sprintf("`%s`", e.Table.Columns[idx].Name)
		values[idx] = buildSqlFieldValue(field)
	}
	formatter := SqlNormalInsertFormat
	if highlight {
		formatter = SqlInsertFormat
	}
	content := fmt.Sprintf(
		formatter,
		e.Table.Schema,
		e.Table.Name,
		strings.Join(fields, ", "),
		strings.Join(values, ", "),
	)
	return content
}

func GenDeleteSql(e *canal.RowsEvent, highlight bool) string {
	fields := buildSqlFieldsExp(e.Table.Columns, e.Rows[0], true)
	formatter := SqlNormalDeleteFormat
	if highlight {
		formatter = SqlDeleteFormat
	}
	content := fmt.Sprintf(
		formatter,
		e.Table.Schema,
		e.Table.Name,
		strings.Join(fields, " AND "),
	)
	return content
}
