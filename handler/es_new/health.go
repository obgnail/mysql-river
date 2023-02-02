package es_new

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime/debug"
	"time"
)

type healthStatus string

const (
	healthStatusGreen  healthStatus = "green"
	healthStatusYellow healthStatus = "yellow"
	healthStatusRed    healthStatus = "red"

	defaultHealthFilePath      = "/tmp/health.info"
	defaultHealthCheckInterval = 10
	minHealthCheckInterval     = 1
	defaultPosThreshold        = 10000
	minPosThreshold            = 1000

	defaultHealthGracePeriod = time.Second * 5
)

var (
	healthStatusLevelList = []healthStatus{
		healthStatusGreen,
		healthStatusYellow,
		healthStatusRed,
	}
)

func (h *healthStatus) toLevel() int {
	for i, status := range healthStatusLevelList {
		if status == *h {
			return i
		}
	}
	return 0
}

func (h *healthStatus) Update(status healthStatus) {
	if status.toLevel() > h.toLevel() {
		*h = status
	}
}

type healthInfo struct {
	flavor              string
	healthCheckInterval int
	posThreshold        int
	filePath            string
	prevHealthStatus    healthStatus
	prevMasterPos       *mysql.Position
	prevBinlogPos       *mysql.Position

	prevMasterMysqlGTIDSet *mysql.MysqlGTIDSet
	prevBinlogMysqlGTIDSet *mysql.MysqlGTIDSet

	needSave bool
}

func newHealthInfo(
	dataDir string,
	outputDir string,
	healthCheckInterval int,
	posThreshold int,
) (*healthInfo, error) {
	var h healthInfo
	h.healthCheckInterval = healthCheckInterval
	if healthCheckInterval < minHealthCheckInterval {
		h.healthCheckInterval = defaultHealthCheckInterval
	}
	h.posThreshold = posThreshold
	if posThreshold < minPosThreshold {
		h.posThreshold = defaultPosThreshold
	}

	if outputDir != "" {
		dataDir = outputDir
	}

	h.filePath = path.Join(dataDir, "health.info")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}
	if err := os.RemoveAll(h.filePath); err != nil {
		return nil, errors.Trace(err)
	}
	h.needSave = true
	h.Update(healthStatusGreen)
	return &h, nil
}

func (h *healthInfo) IsChangeStatus(status healthStatus) bool {
	return h.prevHealthStatus != status
}

func (h *healthInfo) Update(status healthStatus) {
	if h.IsChangeStatus(status) {
		h.needSave = true
		log.Printf("[warn] [Health Status] change: %s to %s", h.prevHealthStatus, status)
	}
	h.prevHealthStatus = status
	if h.needSave {
		if err := ioutil.WriteFile(h.filePath, []byte(status), 0644); err != nil {
			log.Printf("[err] canal save health info to file %s err %v", h.filePath, err)
		}
		h.needSave = false
	}
}

func (r *River) healthLoop() {
	var preMessage []string
	time.Sleep(time.Second * 5)
	log.Println("run health loop")

	for {
		status, messages := r.getHealthStatus()
		if IsStringSliceDiff(preMessage, messages) {
			for _, message := range messages {
				log.Printf("health status message: %s", message)
			}
		}
		r.health.Update(status)
		preMessage = messages
		time.Sleep(time.Duration(r.health.healthCheckInterval) * time.Second)
	}
}

func (r *River) getHealthStatus() (healthStatus, []string) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[err] %s\n%s\n", err, debug.Stack())
		}
	}()

	masterPos, _ := r.master.PositionAndGTIDSet()
	return r.getHealthStatusByPosition(masterPos)
}

// getHealthStatusByPosition 获取健康状态
// 当获取 binlog-pos 失败时， 健康状态为 red
// 当 binlog-pos 跟 master-pos 相差在阈值内， 健康状态为 green
// 当 binlog-pos 跟 master-pos 相关在阈值外时， 健康状态为 yellow
// 当 binlog-pos 跟 上次记录的 binlog-pos 没有变化时，且master-pos 跟 上次记录的 master-pos 没有变化时，且 binlog-pos  跟  master-pos 相等时 健康状态为 green
// 当 binlog-pos 跟 上次记录的 binlog-pos 没有变化时，且master-pos 跟 上次记录的 master-pos 没有变化时，且 binlog-pos  大于  master-pos 时 健康状态为 red
// 当 binlog-pos 跟 上次记录的 binlog-pos 没有变化时，且master-pos 跟 上次记录的 master-pos 有变化时， 健康状态为 green
// 当 binlog-pos 跟 上次记录的 binlog-pos 有变化时， 且 master-pos 跟 上次记录的 master-pos 没有变化时， 健康状态为 red
// 当 binlog-pos 跟 上次记录的 binlog-pos 有变化时， 且 master-pos 跟 上次记录的 master-pos 有变化时， 健康状态为 green
// 最终根据各个判读条件得到的状态值，以最差的状态值作为最终健康状态
func (r *River) getHealthStatusByPosition(masterPos mysql.Position) (healthStatus, []string) {
	var messages []string
	status := healthStatusGreen

	binlogPos, err := r.canal.GetMasterPos()
	if err != nil {
		messages = append(messages, fmt.Sprintf("failed to get binlog pos, err %v", err))
		return healthStatusRed, messages
	}
	defer func() {
		r.health.prevBinlogPos = &binlogPos
		r.health.prevMasterPos = &masterPos
	}()

	if r.health.prevBinlogPos == nil {
		r.health.prevBinlogPos = &binlogPos
	}
	if r.health.prevMasterPos == nil {
		r.health.prevMasterPos = &masterPos
	}

	// compare posThreshold
	if masterPos.Name == binlogPos.Name {
		if masterPos.Pos+uint32(r.health.posThreshold) > binlogPos.Pos {
			status.Update(healthStatusGreen)
		} else {
			messages = append(messages,
				fmt.Sprintf(
					"master-pos is too much smaller than binlog-pos, exceeding the threshold %d",
					r.health.posThreshold,
				),
			)
			status.Update(healthStatusYellow)
		}
	} else {
		if binlogPos.Pos > uint32(r.health.posThreshold) {
			messages = append(messages,
				fmt.Sprintf(
					"master-pos is too much smaller than binlog-pos, exceeding the threshold %d",
					r.health.posThreshold,
				),
			)
			status.Update(healthStatusYellow)
		}
	}

	// compare prevPos
	if binlogPos.Name == r.health.prevBinlogPos.Name && binlogPos.Pos == r.health.prevBinlogPos.Pos {
		if masterPos.Name == r.health.prevMasterPos.Name && masterPos.Pos == r.health.prevMasterPos.Pos {
			if masterPos.Name == binlogPos.Name && masterPos.Pos == binlogPos.Pos {
				status.Update(healthStatusGreen)
			} else if binlogPos.Compare(masterPos) == 1 {
				// To avoid false alarms, need to sleep for a period of time, then take the result again and compare it again
				time.Sleep(defaultHealthGracePeriod)
				currentMasterPos, _ := r.master.PositionAndGTIDSet()
				if currentMasterPos.Compare(masterPos) == 1 {
					status.Update(healthStatusGreen)
				} else {
					messages = append(messages,
						fmt.Sprintf(
							"binlog-pos is larger than master-pos, but master-pos has not changed, sync stopped working,"+
								"binlogPos(%s), masterPos(%s), prevBinlogPos(%s), prevMasterPos(%s)",
							binlogPos.String(),
							masterPos.String(),
							r.health.prevBinlogPos.String(),
							r.health.prevMasterPos.String(),
						),
					)
					return healthStatusRed, messages
				}
			}
		} else {
			status.Update(healthStatusGreen)
		}
	} else {
		if masterPos.Name == binlogPos.Name && masterPos.Pos == r.health.prevMasterPos.Pos {
			// To avoid false alarms, need to sleep for a period of time, then take the result again and compare it again
			time.Sleep(defaultHealthGracePeriod)
			currentMasterPos, _ := r.master.PositionAndGTIDSet()
			if currentMasterPos.Compare(masterPos) == 1 {
				status.Update(healthStatusGreen)
			} else {
				messages = append(messages,
					fmt.Sprintf(
						"binlog-pos has changed, but master-pos has not changed, sync stopped working,"+
							"binlogPos(%s), masterPos(%s), prevBinlogPos(%s), prevMasterPos(%s)",
						binlogPos.String(),
						masterPos.String(),
						r.health.prevBinlogPos.String(),
						r.health.prevMasterPos.String(),
					),
				)
				return healthStatusRed, messages
			}
		} else {
			status.Update(healthStatusGreen)
		}
	}

	return status, messages
}
