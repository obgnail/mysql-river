package river

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/juju/errors"
	"time"
)

type River struct {
	currentGTID string
	nextLog     string
	nextPos     uint32

	handler EventHandler

	masterInfo *masterInfo // 记录解析到哪了
	canal      *canal.Canal
	syncChan   chan *EventData
	exitChan   chan struct{}
}

var _ canal.EventHandler = (*River)(nil)

func New(host string, port int64, user string, password string,
	masterInfoDir string, saveInterval time.Duration) (*River, error) {
	_canal, err := newCanal(host, port, user, password)
	if err != nil {
		return nil, errors.Trace(err)
	}
	master, err := loadMasterInfo(masterInfoDir, saveInterval)
	if err != nil {
		return nil, errors.Trace(err)
	}
	r := &River{
		masterInfo: master,
		canal:      _canal,
		syncChan:   make(chan *EventData, 4094),
		exitChan:   make(chan struct{}, 1),
	}
	return r, nil
}

func (r *River) SetHandlerFunc(handler EventHandler) {
	r.handler = handler
}

func (r *River) Position() mysql.Position {
	return r.masterInfo.Position()
}

func (r *River) updatePos(nextLog string, nextPos uint32, currentGTID string) {
	if len(nextLog) != 0 && nextPos != 0 {
		r.nextLog = nextLog
		r.nextPos = nextPos
	}
	if len(currentGTID) != 0 {
		r.currentGTID = currentGTID
	}
}

func (r *River) String() string {
	return "river"
}

func (r *River) OnRotate(header *replication.EventHeader, e *replication.RotateEvent) error {
	r.updatePos(string(e.NextLogName), uint32(e.Position), "")
	r.syncChan <- &EventData{
		ServerID:  header.ServerID,
		LogName:   r.nextLog,
		LogPos:    r.nextPos,
		Db:        "",
		SQL:       "",
		Table:     "",
		EventType: EventTypeRotate,
		GTIDSet:   "",
		Primary:   []string{},
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Timestamp: header.Timestamp,
	}
	return nil
}

func (r *River) OnDDL(header *replication.EventHeader, nextPos mysql.Position, e *replication.QueryEvent) error {
	r.updatePos(nextPos.Name, nextPos.Pos, e.GSet.String())
	r.syncChan <- &EventData{
		ServerID:  header.ServerID,
		LogName:   r.nextLog,
		LogPos:    r.nextPos,
		Db:        string(e.Schema),
		SQL:       string(e.Query),
		Table:     "",
		EventType: EventTypeDDL,
		GTIDSet:   r.currentGTID,
		Primary:   []string{},
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Timestamp: header.Timestamp,
	}
	return nil
}

func (r *River) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	r.updatePos(nextPos.Name, nextPos.Pos, "")
	r.syncChan <- &EventData{
		ServerID:  header.ServerID,
		LogName:   r.nextLog,
		LogPos:    r.nextPos,
		Db:        "",
		SQL:       "",
		Table:     "",
		EventType: EventTypeXID,
		GTIDSet:   "",
		Primary:   []string{},
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Timestamp: header.Timestamp,
	}
	return nil
}

func (r *River) OnGTID(header *replication.EventHeader, gtid mysql.GTIDSet) error {
	r.updatePos(r.nextLog, header.LogPos, gtid.String())
	r.syncChan <- &EventData{
		ServerID:  header.ServerID,
		LogName:   r.nextLog,
		LogPos:    r.nextPos,
		Db:        "",
		SQL:       "",
		Table:     "",
		EventType: EventTypeGTID,
		GTIDSet:   r.currentGTID,
		Primary:   []string{},
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Timestamp: header.Timestamp,
	}
	return nil
}

func (r *River) OnRow(e *canal.RowsEvent) error {
	before := make(map[string]interface{})
	after := make(map[string]interface{})
	switch e.Action {
	case canal.UpdateAction:
		before = buildFields(e.Table.Columns, e.Rows[0])
		after = buildFields(e.Table.Columns, e.Rows[1])
	case canal.InsertAction:
		after = buildFields(e.Table.Columns, e.Rows[0])
	case canal.DeleteAction:
		before = buildFields(e.Table.Columns, e.Rows[0])
	}

	var primaryKey []string
	for _, colIdx := range e.Table.PKColumns {
		primaryKey = append(primaryKey, e.Table.Columns[colIdx].Name)
	}
	r.updatePos(r.nextLog, e.Header.LogPos, "")
	r.syncChan <- &EventData{
		ServerID:  e.Header.ServerID,
		LogName:   r.nextLog,
		LogPos:    r.nextPos,
		Db:        e.Table.Schema,
		Table:     e.Table.Name,
		SQL:       "",
		EventType: e.Action,
		GTIDSet:   r.currentGTID,
		Primary:   primaryKey,
		Before:    before,
		After:     after,
		Timestamp: e.Header.Timestamp,
	}
	return nil
}

func (r *River) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	r.updatePos(r.nextLog, header.LogPos, "")
	r.syncChan <- &EventData{
		ServerID:  header.ServerID,
		LogName:   r.nextLog,
		LogPos:    r.nextPos,
		Db:        schema,
		SQL:       "",
		Table:     table,
		EventType: EventTypeTableChanged,
		GTIDSet:   r.currentGTID,
		Primary:   []string{},
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Timestamp: header.Timestamp,
	}
	return nil
}

// 监听binlog日志的变化文件与记录的位置,不使用此函数,因为从master.info恢复时不会触发此函数
func (r *River) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (r *River) rangeEvent(handler EventHandler) {
	ticker := time.NewTicker(defaultSaveInterval)
	defer ticker.Stop()

	binlogName, binlogPas := r.masterInfo.Name, r.masterInfo.Pos
	for {
		needSavePos := false
		select {
		case <-ticker.C:
			needSavePos = true
		case <-r.exitChan:
			return
		case event := <-r.syncChan:
			binlogName, binlogPas = event.LogName, event.LogPos
			if event.EventType == EventTypeRotate || event.EventType == EventTypeDDL {
				needSavePos = true
			}
			if err := handler.Handle(event); err != nil {
				r.exitChan <- struct{}{}
			}
		}

		if needSavePos {
			if err := r.masterInfo.Save(binlogName, binlogPas); err != nil {
				// 无法正常写入,直接退出
				r.exitChan <- struct{}{}
			}
		}
	}
}

type From string

const (
	FromMasterPos From = "masterPos"
	FromInfoFile  From = "master.info"
)

func (r *River) RunFrom(from From) (err error) {
	go r.rangeEvent(r.handler)

	start := r.Position()
	if from == FromMasterPos || len(start.Name) == 0 || start.Pos == 0 {
		if start, err = r.canal.GetMasterPos(); err != nil {
			return errors.Trace(err)
		}
	}

	r.canal.SetEventHandler(r)
	fmt.Println(logo)
	if err := r.canal.RunFrom(start); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func newCanal(host string, port int64, user string, password string) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", host, port)
	cfg.User = user
	cfg.Password = password
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// must use binlog full row image
	if err := c.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}
	return c, nil
}

func buildFields(columns []schema.TableColumn, fields []interface{}) map[string]interface{} {
	res := make(map[string]interface{}, len(fields))
	for idx, field := range fields {
		key := columns[idx].Name
		res[key] = field
	}
	return res
}

const logo = `
 __    __     __  __     ______     ______     __         ______     __     __   __   ______     ______    
/\ "-./  \   /\ \_\ \   /\  ___\   /\  __ \   /\ \       /\  == \   /\ \   /\ \ / /  /\  ___\   /\  == \   
\ \ \-./\ \  \ \____ \  \ \___  \  \ \ \/\_\  \ \ \____  \ \  __<   \ \ \  \ \ \'/   \ \  __\   \ \  __<   
 \ \_\ \ \_\  \/\_____\  \/\_____\  \ \___\_\  \ \_____\  \ \_\ \_\  \ \_\  \ \__|    \ \_____\  \ \_\ \_\ 
  \/_/  \/_/   \/_____/   \/_____/   \/___/_/   \/_____/   \/_/ /_/   \/_/   \/_/      \/_____/   \/_/ /_/ 
                                                                                                          
`
