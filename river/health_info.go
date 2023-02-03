package river

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"time"
)

type HealthStatus string

const (
	healthStatusGreen  HealthStatus = "green"
	healthStatusYellow HealthStatus = "yellow"
	healthStatusRed    HealthStatus = "red"
)

var levelList = []HealthStatus{healthStatusGreen, healthStatusYellow, healthStatusRed}

func (h *HealthStatus) toLevel() int {
	for i, status := range levelList {
		if status == *h {
			return i
		}
	}
	return 0
}

func (h *HealthStatus) ChooseWorse(status HealthStatus) {
	if status == *h {
		return
	}
	if status.toLevel() > h.toLevel() {
		*h = status
	}
}

const (
	ReasonGetPosError     = "failed to get db-pos"                                                     // red
	ReasonExceedThreshold = "The diff between db-pos and file-pos exceeds the threshold"               // yellow
	ReasonStopApproaching = "both of db-pos and file-pos make no progress, but file-pos behind db-pos" // red
	ReasonStopSync        = "db-pos makes progress while file-pos not"                                 // red
)

const (
	defaultHealthCheckInterval = 10
	minHealthCheckInterval     = 1
	defaultPosThreshold        = 10000
	minPosThreshold            = 1000

	defaultHealthGracePeriod = time.Second * 5
)

type StatusMsg struct {
	Status        HealthStatus
	LastStatus    HealthStatus
	Reason        []string // 发生告警时的消息(可能有多条不通过)
	FilePos       *mysql.Position
	DBPos         *mysql.Position
	CheckInterval time.Duration
	PosThreshold  int
}

type healthInfo struct {
	checkInterval time.Duration
	posThreshold  int // byte num
	lastFilePos   *mysql.Position
	lastDBPos     *mysql.Position
	lastStatus    HealthStatus
}

func newHealthInfo(checkInterval int, posThreshold int) *healthInfo {
	if checkInterval < minHealthCheckInterval {
		checkInterval = defaultHealthCheckInterval
	}
	if posThreshold < minPosThreshold {
		posThreshold = defaultPosThreshold
	}
	h := &healthInfo{
		checkInterval: time.Duration(checkInterval) * time.Second,
		posThreshold:  posThreshold,
	}
	return h
}

func (h *healthInfo) Update(msg *StatusMsg, onAlert func(msg *StatusMsg) error) error {
	if h.NeedAlert(msg.Status) {
		return errors.Trace(onAlert(msg))
	}
	h.lastStatus = msg.Status
	h.lastFilePos = msg.FilePos
	h.lastDBPos = msg.DBPos
	return nil
}

func (h *healthInfo) NewMsg(status HealthStatus, reason []string, filePos, dbPos *mysql.Position) *StatusMsg {
	return &StatusMsg{
		Status:        status,
		LastStatus:    h.lastStatus,
		Reason:        reason,
		FilePos:       filePos,
		DBPos:         dbPos,
		CheckInterval: h.checkInterval,
		PosThreshold:  h.posThreshold,
	}
}

// NeedAlert 状态发生改变,并且改变后的状态不是green
func (h *healthInfo) NeedAlert(curStatus HealthStatus) bool {
	return h.lastStatus != curStatus && curStatus != healthStatusGreen
}

func (h *healthInfo) dbMakeNoProgress(dbPos *mysql.Position) bool {
	return dbPos.Name == h.lastDBPos.Name && dbPos.Pos == h.lastDBPos.Pos
}

func (h *healthInfo) fileMakeNoProgress(filePos *mysql.Position) bool {
	return filePos.Name == h.lastFilePos.Name && filePos.Pos == h.lastFilePos.Pos
}

func (h *healthInfo) Equal(dbPos, filePos *mysql.Position) bool {
	return filePos.Name == dbPos.Name && filePos.Pos == dbPos.Pos
}
