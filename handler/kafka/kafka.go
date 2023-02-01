package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/handler/trace_log"
	"github.com/obgnail/mysql-river/river"
	"runtime/debug"
)

/*
{
	"server_id":"server_id1",
    "log_pos":"786",
    "db":"test",
    "table":"test1",
    "sql":"UPDATE `testdb01`.`user` SET (...);",
    "event_type":"update",
	"gtid": "577b1aef-a03e-11eb-b217-0242ac110003:11",
	"primary": ["uuid"],
    "before":{
        "id":2,
        "num":1,
        "strs":"wddd",
        "time":"2001-11-30 00:00:00"
    },
    "after":{
        "id":2,
        "num":1,
        "strs":"wddd",
        "time":"2018-09-14 00:00:00"
    },
	"timestamp":"1675081632"
}
*/
type FormatData struct {
	ServerID  uint32                 `json:"server_id"`
	LogPos    uint32                 `json:"log_pos"`
	Db        string                 `json:"db"`
	Table     string                 `json:"table"`
	SQL       string                 `json:"sql"`        // 主要用于 DDL event
	EventType string                 `json:"event_type"` // 操作类型 insert、update、delete、ddl、gtid、xid
	GTID      string                 `json:"gtid"`       // 存储gtid
	Primary   []string               `json:"primary"`    // 主键字段；EventType非ddl时有值
	Before    map[string]interface{} `json:"before"`     // 变更前数据, insert 类型的 before 为空
	After     map[string]interface{} `json:"after"`      // 变更后数据, delete 类型的 after 为空
	Timestamp uint32                 `json:"timestamp"`  // 事件时间
}

func ToBytes(data *FormatData) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return []byte{}, nil
	}
	return b, nil
}

func NewRowData(e *canal.RowsEvent, gtid string) *FormatData {
	var sql string
	before := make(map[string]interface{})
	after := make(map[string]interface{})
	switch e.Action {
	case canal.UpdateAction:
		sql = trace_log.GenUpdateSql(e, false, true)
		before = buildFields(e.Table.Columns, e.Rows[0])
		after = buildFields(e.Table.Columns, e.Rows[1])
	case canal.InsertAction:
		sql = trace_log.GenInsertSql(e, false)
		after = buildFields(e.Table.Columns, e.Rows[0])
	case canal.DeleteAction:
		sql = trace_log.GenDeleteSql(e, false)
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
		GTID:      gtid,
		Primary:   primaryKey,
		Before:    before,
		After:     after,
		Timestamp: e.Header.Timestamp,
	}
	return d
}

func NewDDLData(nextPos mysql.Position, e *replication.QueryEvent) *FormatData {
	d := &FormatData{
		ServerID:  e.SlaveProxyID,
		LogPos:    nextPos.Pos,
		Db:        string(e.Schema),
		Table:     "",
		SQL:       string(e.Query),
		EventType: "ddl",
		Primary:   []string{},
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Timestamp: e.ExecutionTime,
	}
	return d
}

func NewGTIDData(gtid mysql.GTIDSet) *FormatData {
	d := &FormatData{EventType: "gtid", GTID: gtid.String()}
	return d
}

func NewXIDData(nextPos mysql.Position) *FormatData {
	d := &FormatData{EventType: "xid", LogPos: nextPos.Pos}
	return d
}

type KafkaHandler struct {
	currentGTID string
	*canal.DummyEventHandler
}

func NewKafkaHandler() *KafkaHandler {
	return &KafkaHandler{DummyEventHandler: new(canal.DummyEventHandler)}
}

func (h *KafkaHandler) String() string {
	return "kafka"
}

func (h *KafkaHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	d := NewDDLData(nextPos, queryEvent)
	result, err := ToBytes(d)
	fmt.Println(string(result), err)
	return nil
}

func (h *KafkaHandler) OnGTID(gtid mysql.GTIDSet) error {
	h.currentGTID = gtid.String()
	d := NewGTIDData(gtid)
	result, err := ToBytes(d)
	fmt.Println(string(result), err)
	return nil
}

func (h *KafkaHandler) OnXID(nextPos mysql.Position) error {
	d := NewXIDData(nextPos)
	result, err := ToBytes(d)
	fmt.Println(string(result), err)
	return nil
}

func (h *KafkaHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()

	d := NewRowData(e, h.currentGTID)
	result, err := ToBytes(d)
	fmt.Println(string(result), err)
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

func RunKafkaRiver(addr, user, password string) error {
	return errors.Trace(river.RunRiver(addr, user, password, NewKafkaHandler()))
}
