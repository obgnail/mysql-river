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

const (
	defaultSaveInterval = 3 * time.Second
)

type HandlerFunc func(event *EventData) error

var _ canal.EventHandler = (*BaseHandler)(nil)

type BaseHandler struct {
	currentGTID string
	nextLog     string
	nextPos     uint32

	handler HandlerFunc

	masterInfo *masterInfo // 记录解析到哪了
	canal      *canal.Canal
	syncChan   chan *EventData
	exitChan   chan struct{}
	*canal.DummyEventHandler
}

func New(host string, port int64, user string, password string, masterInfoDir string) (*BaseHandler, error) {
	_canal, err := newCanal(host, port, user, password)
	if err != nil {
		return nil, errors.Trace(err)
	}
	master, err := loadMasterInfo(masterInfoDir)
	if err != nil {
		return nil, errors.Trace(err)
	}
	h := &BaseHandler{
		masterInfo:        master,
		canal:             _canal,
		syncChan:          make(chan *EventData, 4094),
		exitChan:          make(chan struct{}, 1),
		DummyEventHandler: new(canal.DummyEventHandler),
	}
	return h, nil
}

func (h *BaseHandler) SetHandlerFunc(f HandlerFunc) {
	h.handler = f
}

func (h *BaseHandler) updatePos(nextLog string, nextPos uint32, currentGTID string) {
	if len(nextLog) != 0 && nextPos != 0 {
		h.nextLog = nextLog
		h.nextPos = nextPos
	}
	if len(currentGTID) != 0 {
		h.currentGTID = currentGTID
	}
}

func (h *BaseHandler) String() string {
	return "base"
}

func (h *BaseHandler) OnRotate(e *replication.RotateEvent) error {
	h.updatePos(string(e.NextLogName), uint32(e.Position), "")
	h.syncChan <- &EventData{
		ServerID:  0,
		LogName:   h.nextLog,
		LogPos:    h.nextPos,
		Db:        "",
		SQL:       "",
		Table:     "",
		EventType: EventTypeRotate,
		GTIDSet:   "",
		Primary:   []string{},
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Timestamp: 0,
	}
	return nil
}

func (h *BaseHandler) OnDDL(nextPos mysql.Position, e *replication.QueryEvent) error {
	h.updatePos(nextPos.Name, nextPos.Pos, e.GSet.String())
	h.syncChan <- &EventData{
		ServerID:  e.SlaveProxyID,
		LogName:   h.nextLog,
		LogPos:    h.nextPos,
		Db:        string(e.Schema),
		SQL:       string(e.Query),
		Table:     "",
		EventType: EventTypeDDL,
		GTIDSet:   h.currentGTID,
		Primary:   []string{},
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Timestamp: e.ExecutionTime,
	}
	return nil
}

func (h *BaseHandler) OnXID(nextPos mysql.Position) error {
	h.updatePos(nextPos.Name, nextPos.Pos, "")
	h.syncChan <- &EventData{
		ServerID:  0,
		LogName:   h.nextLog,
		LogPos:    h.nextPos,
		Db:        "",
		SQL:       "",
		Table:     "",
		EventType: EventTypeXID,
		GTIDSet:   "",
		Primary:   []string{},
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Timestamp: 0,
	}
	return nil
}

func (h *BaseHandler) OnGTID(gtid mysql.GTIDSet) error {
	h.updatePos("", 0, gtid.String())
	h.syncChan <- &EventData{
		ServerID:  0,
		LogName:   h.nextLog,
		LogPos:    h.nextPos,
		Db:        "",
		SQL:       "",
		Table:     "",
		EventType: EventTypeGTID,
		GTIDSet:   h.currentGTID,
		Primary:   []string{},
		Before:    make(map[string]interface{}),
		After:     make(map[string]interface{}),
		Timestamp: 0,
	}
	return nil
}

func (h *BaseHandler) OnRow(e *canal.RowsEvent) error {
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
	h.updatePos(h.nextLog, e.Header.LogPos, "")
	h.syncChan <- &EventData{
		ServerID:  e.Header.ServerID,
		LogName:   h.nextLog,
		LogPos:    h.nextPos,
		Db:        e.Table.Schema,
		Table:     e.Table.Name,
		SQL:       "",
		EventType: e.Action,
		GTIDSet:   h.currentGTID,
		Primary:   primaryKey,
		Before:    before,
		After:     after,
		Timestamp: e.Header.Timestamp,
	}
	return nil
}

func (h *BaseHandler) Range(f func(event *EventData) error) {
	ticker := time.NewTicker(defaultSaveInterval)
	defer ticker.Stop()
	binlogName, binlogPas := h.masterInfo.Name, h.masterInfo.Pos
	for {
		needSavePos := false
		select {
		case <-ticker.C:
			needSavePos = true
		case <-h.exitChan:
			return
		case event := <-h.syncChan:
			binlogName, binlogPas = event.LogName, event.LogPos
			if event.EventType == EventTypeRotate || event.EventType == EventTypeDDL {
				needSavePos = true
			}
			if err := f(event); err != nil {
				h.exitChan <- struct{}{}
			}
		}

		if needSavePos {
			if err := h.masterInfo.Save(binlogName, binlogPas); err != nil {
				h.exitChan <- struct{}{}
			}
		}
	}
}

type From string

const (
	FromMasterPos From = "masterPos"
	FromInfoFile  From = "master.info"
)

// from:db or file
func (h *BaseHandler) RunFrom(from From) (err error) {
	go h.Range(h.handler)

	start := h.masterInfo.Position()
	if from == FromMasterPos || len(start.Name) == 0 || start.Pos == 0 {
		if start, err = h.canal.GetMasterPos(); err != nil {
			return errors.Trace(err)
		}
	}

	h.canal.SetEventHandler(h)
	fmt.Println(logo)
	if err := h.canal.RunFrom(start); err != nil {
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
