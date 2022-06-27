package main

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pingcap/errors"
	"reflect"
	"runtime/debug"
	"strings"
)

const (
	SqlInsertFormat = "INSERT INTO `%s`.`%s`(%s) VALUES (%s);"
	SqlUpdateFormat = "UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;"
	SqlDeleteFormat = "DELETE FROM `%s`.`%s` WHERE %s LIMIT 1;"
)

type RiverHandler struct {
	canal.DummyEventHandler
	*canal.Canal
}

func (r *RiverHandler) String() string {
	return "river handler"
}

func NewRiver(addr string, user string, password string) *RiverHandler {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = addr
	cfg.User = user
	cfg.Password = password
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = ""
	c, err := canal.NewCanal(cfg)
	if err != nil {
		panic(err)
	}
	return &RiverHandler{Canal: c}
}

func (r *RiverHandler) Listen() error {
	coords, err := r.GetMasterPos()
	if err != nil {
		return errors.Trace(err)
	}
	r.SetEventHandler(r)
	if err := r.RunFrom(coords); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *RiverHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()

	var sql string

	switch e.Action {
	case canal.UpdateAction:
		sql = genUpdateSql(e)
	case canal.InsertAction:
		sql = genInsertSql(e)
	case canal.DeleteAction:
		sql = genDeleteSql(e)
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
	case reflect.Uintptr, reflect.Complex64, reflect.Complex128, reflect.Array, reflect.Chan,
		reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice,
		reflect.Struct, reflect.UnsafePointer, reflect.Invalid:
		return ""
	default:
		return ""
	}
}

func buildEqualExp(key, value string) string {
	// if v is NULL, may need to process
	if value == "NULL" {
		return fmt.Sprintf("`%s` IS %s", key, value)
	}
	return fmt.Sprintf("`%s`=%s", key, value)
}

func buildSqlFieldsExp(columns []schema.TableColumn, fields []interface{}) []string {
	res := make([]string, len(fields))
	for idx, field := range fields {
		key := columns[idx].Name
		value := buildSqlFieldValue(field)
		res[idx] = buildEqualExp(key, value)
	}
	return res
}

func genUpdateSql(e *canal.RowsEvent) string {
	beforeFields := buildSqlFieldsExp(e.Table.Columns, e.Rows[0])
	updatedFields := buildSqlFieldsExp(e.Table.Columns, e.Rows[1])
	content := fmt.Sprintf(
		SqlUpdateFormat,
		e.Table.Schema,
		e.Table.Name,
		strings.Join(updatedFields, ", "),
		strings.Join(beforeFields, " AND "),
	)
	return content
}

func genInsertSql(e *canal.RowsEvent) string {
	fields := make([]string, len(e.Rows[0]))
	values := make([]string, len(e.Rows[0]))
	for idx, field := range e.Rows[0] {
		fields[idx] = fmt.Sprintf("`%s`", e.Table.Columns[idx].Name)
		values[idx] = buildSqlFieldValue(field)
	}
	content := fmt.Sprintf(
		SqlInsertFormat,
		e.Table.Schema,
		e.Table.Name,
		strings.Join(fields, ", "),
		strings.Join(values, ", "),
	)
	return content
}

func genDeleteSql(e *canal.RowsEvent) string {
	fields := buildSqlFieldsExp(e.Table.Columns, e.Rows[0])
	content := fmt.Sprintf(
		SqlDeleteFormat,
		e.Table.Schema,
		e.Table.Name,
		strings.Join(fields, " AND "),
	)
	return content
}
