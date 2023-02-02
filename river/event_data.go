package river

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
)

const (
	EventTypeUpdate       = canal.UpdateAction
	EventTypeInsert       = canal.InsertAction
	EventTypeDelete       = canal.DeleteAction
	EventTypeDDL          = "ddl"
	EventTypeGTID         = "gtid"
	EventTypeXID          = "xid"
	EventTypeRotate       = "rotate"
	EventTypeTableChanged = "table_change"
)

type EventData struct {
	// insert、update、delete、ddl、gtid、xid、rotate、table_changed
	EventType string                 `json:"event_type"`
	ServerID  uint32                 `json:"server_id"`
	LogName   string                 `json:"log_name"` // 对应mysql.Position
	LogPos    uint32                 `json:"log_pos"`  // 对应mysql.Position
	Db        string                 `json:"db"`
	Table     string                 `json:"table"`
	SQL       string                 `json:"sql"` // 仅当EventType为ddl有值
	GTIDSet   string                 `json:"gtid_set"`
	Primary   []string               `json:"primary"`   // 主键字段；EventType为insert、update、delete时有值
	Before    map[string]interface{} `json:"before"`    // 变更前数据, insert 类型的 before 为空
	After     map[string]interface{} `json:"after"`     // 变更后数据, delete 类型的 after 为空
	Timestamp uint32                 `json:"timestamp"` // 事件时间
}

func (d *EventData) Position() string {
	return fmt.Sprintf("%s:%d", d.LogName, d.LogPos)
}

func (d *EventData) ToBytes() ([]byte, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return []byte{}, nil
	}
	return b, nil
}
