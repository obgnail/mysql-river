package river

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"sync"
	"time"
)

type HealthStatus string

const (
	healthStatusGreen  HealthStatus = "green"
	healthStatusYellow HealthStatus = "yellow"
	healthStatusRed    HealthStatus = "red"
)

var levelList = []HealthStatus{healthStatusGreen, healthStatusYellow, healthStatusRed}

func (h *HealthStatus) ToLevel() int {
	for i, status := range levelList {
		if status == *h {
			return i
		}
	}
	return 0
}

func (h *HealthStatus) Worse(status HealthStatus) {
	if status == *h {
		return
	}
	if status.ToLevel() > h.ToLevel() {
		*h = status
	}
}

const (
	ReasonGetPosError     = "failed to get db-pos."                                                           // red
	ReasonExceedThreshold = "The diff between db-pos and file-pos exceeds the threshold."                     // yellow
	ReasonStopApproaching = "both of db-pos and file-pos make no progress, but file-pos still behind db-pos." // red
	ReasonStopSync        = "db-pos makes progress while file-pos not."                                       // red
)

const (
	defaultHealthCheckInterval = 10 * time.Second
	minHealthCheckInterval     = 1 * time.Second
	defaultPosThreshold        = 10000
	minPosThreshold            = 1000

	defaultHealthGracePeriod = 5 * time.Second
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

	sync.RWMutex // protect below
	lastFilePos  *mysql.Position
	lastDBPos    *mysql.Position
	lastStatus   HealthStatus
}

func newHealthInfo(checkInterval time.Duration, posThreshold int) *healthInfo {
	if checkInterval < minHealthCheckInterval {
		checkInterval = defaultHealthCheckInterval
	}
	if posThreshold < minPosThreshold {
		posThreshold = defaultPosThreshold
	}
	h := &healthInfo{
		checkInterval: checkInterval,
		posThreshold:  posThreshold,
	}
	return h
}

func (h *healthInfo) update(msg *StatusMsg) (needAlert bool) {
	h.Lock()
	defer h.Unlock()
	// 状态发生改变,并且改变后的状态不是green
	needAlert = h.lastStatus != msg.Status && msg.Status != healthStatusGreen
	h.lastStatus = msg.Status
	h.lastFilePos = msg.FilePos
	h.lastDBPos = msg.DBPos
	return
}

func (h *healthInfo) newMsg(status HealthStatus, reason []string, filePos, dbPos *mysql.Position) *StatusMsg {
	h.RLock()
	defer h.RUnlock()
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

func (h *healthInfo) dbMakeNoProgress(dbPos *mysql.Position) bool {
	h.RLock()
	defer h.RUnlock()
	return dbPos.Name == h.lastDBPos.Name && dbPos.Pos == h.lastDBPos.Pos
}

func (h *healthInfo) fileMakeNoProgress(filePos *mysql.Position) bool {
	h.RLock()
	defer h.RUnlock()
	return filePos.Name == h.lastFilePos.Name && filePos.Pos == h.lastFilePos.Pos
}

func (h *healthInfo) equal(dbPos, filePos *mysql.Position) bool {
	return filePos.Name == dbPos.Name && filePos.Pos == dbPos.Pos
}
