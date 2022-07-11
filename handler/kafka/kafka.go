package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/obgnail/mysql-river/handler/trace_log"
	"runtime/debug"
	"strings"
)

/*
最终输出格式
{
    "log_pos":"786",
    "db":"test",
    "table":"test1",
    "query":"",
    "event_type":"update",
    "before":{
        "id":2,
        "num":1,
        "strs":"wddd",
        "times":"-0001-11-30 00:00:00"
    },
    "after":{
        "id":2,
        "num":1,
        "strs":"wddd",
        "times":"2018-09-14 00:00:00"
    }
}
*/
type FormatData struct {
	ServerID  uint32                 `json:"server_id"`
	LogPos    uint32                 `json:"log_pos"`
	Db        string                 `json:"db"`
	Table     string                 `json:"table"`
	SQL       string                 `json:"sql"`        // 主要用于 DDL event
	EventType string                 `json:"event_type"` // 操作类型 insert、update、delete、ddl
	Primary   []string               `json:"primary"`    // 主键字段；EventType非ddl时有值
	Before    map[string]interface{} `json:"before"`     // 变更前数据, insert 类型的 before 为空
	After     map[string]interface{} `json:"after"`      // 变更后数据, delete 类型的 after 为空
	Timestamp uint32                 `json:"timestamp"`  // 事件时间
}

// 转换json
func FormatEventDataJson(data *FormatData) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return []byte{}, nil
	}
	return b, nil
}

func NewFormatDataFromDDL(nextPos mysql.Position, e *replication.QueryEvent) *FormatData {
	d := &FormatData{
		ServerID:  e.SlaveProxyID,
		LogPos:    nextPos.Pos,
		Db:        string(e.Schema),
		Table:     "",
		SQL:       string(e.Query),
		EventType: "DDL",
		Primary:   []string{},
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Timestamp: e.ExecutionTime,
	}
	return d
}

func NewFormatDataFromRow(e *canal.RowsEvent) *FormatData {
	var sql string
	before := make(map[string]interface{})
	after := make(map[string]interface{})

	switch e.Action {
	case canal.UpdateAction:
		sql = trace_log.GenUpdateSql(e, true)
		before = buildFields(e.Table.Columns, e.Rows[0])
		after = buildFields(e.Table.Columns, e.Rows[1])
	case canal.InsertAction:
		sql = trace_log.GenInsertSql(e)
		after = buildFields(e.Table.Columns, e.Rows[0])
	case canal.DeleteAction:
		sql = trace_log.GenDeleteSql(e)
		before = buildFields(e.Table.Columns, e.Rows[0])
	}

	var primaryKey []string
	for _, colIdx := range e.Table.PKColumns {
		primaryKey = append(primaryKey, e.Table.Columns[colIdx].Name)
	}

	d := &FormatData{
		ServerID:  e.Header.ServerID,
		LogPos:    e.Header.LogPos,
		Db:        e.Table.Schema,
		Table:     e.Table.Name,
		SQL:       sql,
		EventType: e.Action,
		Primary:   primaryKey,
		Before:    before,
		After:     after,
		Timestamp: e.Header.Timestamp,
	}
	return d
}

type KafkaHandler struct {
	databases map[string]struct{}
	tables    map[string]struct{}

	*canal.DummyEventHandler
}

func NewKafkaHandler(databases, tables []string, showAllField, showQueryMessage bool) *KafkaHandler {
	ds := make(map[string]struct{}, len(databases))
	ts := make(map[string]struct{}, len(tables))
	for _, d := range databases {
		ds[strings.ToLower(d)] = struct{}{}
	}
	for _, t := range tables {
		ts[strings.ToLower(t)] = struct{}{}
	}
	return &KafkaHandler{
		databases:         ds,
		tables:            ts,
		DummyEventHandler: new(canal.DummyEventHandler),
	}
}

func (h *KafkaHandler) String() string {
	return "kafka"
}

func (h *KafkaHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	d := NewFormatDataFromDDL(nextPos, queryEvent)
	result, err := FormatEventDataJson(d)

	fmt.Println(result, err)
	return nil
}

func (h *KafkaHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()

	if len(h.databases) != 0 {
		if _, ok := h.databases[e.Table.Schema]; !ok {
			return nil
		}
	}
	if len(h.tables) != 0 {
		if _, ok := h.tables[e.Table.Name]; !ok {
			return nil
		}
	}

	d := NewFormatDataFromRow(e)
	result, err := FormatEventDataJson(d)

	fmt.Println(result, err)
	return nil
}

func buildFields(columns []schema.TableColumn, fields []interface{}) map[string]interface{} {
	res := make(map[string]interface{}, len(fields))
	for idx, field := range fields {
		key := columns[idx].Name
		res[key] = field
	}
	return res
}
