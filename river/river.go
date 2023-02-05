package river

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/juju/errors"
	"time"
)

type River struct {
	config *Config

	handler Handler

	currentGTID string
	nextLog     string
	nextPos     uint32

	masterInfo *masterInfo // 记录解析到哪了
	healthInfo *healthInfo // 记录masterInfo和canal.GetMasterPos()的差距,可对接告警机制
	canal      *canal.Canal

	Error error

	ctx    context.Context
	cancel context.CancelFunc

	syncChan   chan *EventData
	statusChan chan *StatusMsg
}

var _ canal.EventHandler = (*River)(nil)

func New(config *Config) *River {
	return &River{config: config}
}

func (r *River) SetHandler(handler Handler) *River {
	r.handler = handler
	return r
}

func (r *River) PrintConfig(from From) {
	fmt.Println(logo)
	temp := *r.config.MySQLConfig
	temp.Password = ""
	fmt.Printf(`
Handler            :  %+v
From               :  %+v
MySQLConfig        :  %+v
PosAutoSaverConfig :  %+v
HealthCheckerConfig:  %+v
--------------------
`,
		r.handler.String(),
		from,
		temp,
		*r.config.PosAutoSaverConfig,
		*r.config.HealthCheckerConfig,
	)
}

func (r *River) Sync(from From) (err error) {
	r.PrintConfig(from)

	if err = r.prepare(); err != nil {
		return errors.Trace(err)
	}

	startPos := r.GetFilePosition()
	if from == FromDB || len(startPos.Name) == 0 || startPos.Pos == 0 {
		if startPos, err = r.GetDBPosition(); err != nil {
			return errors.Trace(err)
		}
	}

	go r.loopSync(startPos, r.handler.OnEvent)
	go r.loopHealthCheck(r.handler.OnAlert)

	r.canal.SetEventHandler(r)
	if err := r.canal.RunFrom(startPos); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *River) prepare() (err error) {
	db := r.config.MySQLConfig
	saver := r.config.PosAutoSaverConfig
	checker := r.config.HealthCheckerConfig

	r.canal, err = newCanal(db.Host, db.Port, db.User, db.Password)
	if err != nil {
		return errors.Trace(err)
	}
	r.masterInfo, err = loadMasterInfo(saver.SaveDir, saver.SaveInterval)
	if err != nil {
		return errors.Trace(err)
	}
	r.healthInfo = newHealthInfo(checker.CheckInterval, checker.CheckPosThreshold)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.syncChan = make(chan *EventData, 4094)
	r.statusChan = make(chan *StatusMsg, 64)
	return nil
}

func (r *River) GetFilePosition() mysql.Position {
	return r.masterInfo.position()
}

func (r *River) GetDBPosition() (pos mysql.Position, err error) {
	pos, err = r.canal.GetMasterPos()
	if err != nil {
		return pos, errors.Trace(err)
	}
	return pos, nil
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

// OnPosSynced 监听binlog日志的变化文件与记录的位置,不使用此函数,因为从master.info恢复时不会触发此函数
func (r *River) OnPosSynced(*replication.EventHeader, mysql.Position, mysql.GTIDSet, bool) error {
	return nil
}

func (r *River) Close(err error) {
	Logger.Info("closing river")
	r.canal.Close()
	r.masterInfo.Close()
	r.cancel()
	r.Error = err
	r.handler.OnClose(r)
}

// To avoid false alarms, need to sleep for a period of time, then take the result again and compare it again
func (r *River) recheckFilePos(preFilePos *mysql.Position) (pass bool) {
	Logger.Debug("rechecking file position")
	time.Sleep(defaultHealthGracePeriod)
	curFilePos := r.GetFilePosition()
	return curFilePos.Compare(*preFilePos) == 1
}

// healthCheck 检测健康状态
// 当获取 db-pos 失败时, 健康状态为 red
// 当 db-pos 跟 file-pos 相差在阈值内, 健康状态为 green
// 当 db-pos 跟 file-pos 相关在阈值外时, 健康状态为 yellow
// 当 db-pos 跟 上次记录的 db-pos 没有变化时，且file-pos 跟 上次记录的 file-pos 没有变化时，且 db-pos 跟 file-pos 相等时 健康状态为 green
// 当 db-pos 跟 上次记录的 db-pos 没有变化时，且file-pos 跟 上次记录的 file-pos 没有变化时，且 db-pos 大于 file-pos 时 健康状态为 red
// 当 db-pos 跟 上次记录的 db-pos 没有变化时，且file-pos 跟 上次记录的 file-pos 有变化时, 健康状态为 green
// 当 db-pos 跟 上次记录的 db-pos 有变化时, 且 file-pos 跟 上次记录的 file-pos 没有变化时, 健康状态为 red
// 当 db-pos 跟 上次记录的 db-pos 有变化时, 且 file-pos 跟 上次记录的 file-pos 有变化时, 健康状态为 green
// 最终根据各个判读条件得到的状态值，以最差的状态值作为最终健康状态
func (r *River) healthCheck() {
	status := healthStatusGreen
	var reasons []string

	filePos := r.GetFilePosition()
	dbPos, err := r.GetDBPosition()
	if err != nil {
		reason := []string{ReasonGetPosError + err.Error()}
		r.statusChan <- r.healthInfo.newMsg(healthStatusRed, reason, &filePos, &dbPos)
		return
	}
	if r.healthInfo.lastDBPos == nil {
		r.healthInfo.lastDBPos = &dbPos
	}
	if r.healthInfo.lastFilePos == nil {
		r.healthInfo.lastFilePos = &filePos
	}

	startPos := filePos.Pos
	if filePos.Name != dbPos.Name { // 已经更换binlog文件
		startPos = 0
	}
	if startPos+uint32(r.healthInfo.posThreshold) < dbPos.Pos {
		status.Worse(healthStatusYellow)
		reasons = append(reasons, ReasonExceedThreshold)
	}

	if r.healthInfo.dbMakeNoProgress(&dbPos) {
		if r.healthInfo.fileMakeNoProgress(&filePos) && !r.healthInfo.equal(&dbPos, &filePos) &&
			!r.recheckFilePos(&filePos) {
			status.Worse(healthStatusRed)
			reasons = append(reasons, ReasonStopApproaching)
		}
	} else {
		if r.healthInfo.fileMakeNoProgress(&filePos) && !r.recheckFilePos(&filePos) {
			status.Worse(healthStatusRed)
			reasons = append(reasons, ReasonStopSync)
		}
	}
	r.statusChan <- r.healthInfo.newMsg(status, reasons, &filePos, &dbPos)
	Logger.Debugf("health checked: [%s]", status)
}

func (r *River) loopHealthCheck(onAlert func(msg *StatusMsg) error) {
	time.Sleep(5 * time.Second)
	ticker := time.NewTicker(r.healthInfo.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			Logger.Info("health check process had done")
			return
		case <-ticker.C:
			r.healthCheck()
		case msg := <-r.statusChan:
			if needAlert := r.healthInfo.update(msg); needAlert {
				go func() {
					if err := onAlert(msg); err != nil {
						r.Close(err)
					}
				}()
			}
		}
	}
}

func (r *River) loopSync(startPos mysql.Position, onEvent func(event *EventData) error) {
	ticker := time.NewTicker(r.masterInfo.saveInterval)
	defer ticker.Stop()

	logName, logPas := startPos.Name, startPos.Pos
	for {
		needSavePos := false
		select {
		case <-r.ctx.Done():
			Logger.Info("event handle and position auto saver process had done")
			return
		case <-ticker.C:
			needSavePos = true
		case event := <-r.syncChan:
			logName, logPas = event.LogName, event.LogPos
			if event.EventType == EventTypeRotate || event.EventType == EventTypeDDL {
				needSavePos = true
			}
			if err := onEvent(event); err != nil {
				r.Close(err)
			}
		}

		if needSavePos {
			Logger.Debugf("position auto save at: [%s:%d]", logName, logPas)
			if err := r.masterInfo.save(logName, logPas); err != nil {
				r.Close(err) // 无法正常写入,直接退出
			}
		}
	}
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
███╗   ███╗██╗   ██╗███████╗ ██████╗ ██╗         ██████╗ ██╗██╗   ██╗███████╗██████╗ 
████╗ ████║╚██╗ ██╔╝██╔════╝██╔═══██╗██║         ██╔══██╗██║██║   ██║██╔════╝██╔══██╗
██╔████╔██║ ╚████╔╝ ███████╗██║   ██║██║         ██████╔╝██║██║   ██║█████╗  ██████╔╝
██║╚██╔╝██║  ╚██╔╝  ╚════██║██║▄▄ ██║██║         ██╔══██╗██║╚██╗ ██╔╝██╔══╝  ██╔══██╗
██║ ╚═╝ ██║   ██║   ███████║╚██████╔╝███████╗    ██║  ██║██║ ╚████╔╝ ███████╗██║  ██║
╚═╝     ╚═╝   ╚═╝   ╚══════╝ ╚══▀▀═╝ ╚══════╝    ╚═╝  ╚═╝╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝
`
