package es

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"
)

const (
	fileName        = "master.info"
	saveMinDuration = time.Second
)

var (
	emptyDirErr  = fmt.Errorf("data dir is empty")
	emptyPathErr = fmt.Errorf("file path is empty")
)

type MasterInfo struct {
	sync.RWMutex        // protect below
	Name         string `json:"bin_name"`
	Pos          uint32 `json:"bin_pos"`
	filePath     string
	lastSaveTime time.Time
}

func LoadMasterInfo(dataDir string) (*MasterInfo, error) {
	if len(dataDir) == 0 {
		return nil, emptyDirErr
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	filePath := path.Join(dataDir, fileName)
	infoBytes, err := ReadFile(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m := &MasterInfo{
		filePath:     filePath,
		lastSaveTime: time.Now(),
	}
	if err = json.Unmarshal(infoBytes, &m); err != nil {
		return nil, errors.Trace(err)
	}

	return m, nil
}

func (m *MasterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()
	return mysql.Position{Name: m.Name, Pos: m.Pos}
}

func (m *MasterInfo) Close() error {
	pos := m.Position()
	err := m.Save(pos)
	return errors.Trace(err)
}

func (m *MasterInfo) Save(pos mysql.Position) error {
	m.Lock()
	defer m.Unlock()

	if m.filePath == "" {
		return emptyPathErr
	}

	n := time.Now()
	// 保存的最小时间间隔
	if n.Sub(m.lastSaveTime) < saveMinDuration {
		return nil
	}
	m.lastSaveTime = n
	m.Name = pos.Name
	m.Pos = pos.Pos

	content, err := json.Marshal(m)
	if err != nil {
		return errors.Trace(err)
	}
	if err := WriteFileAtomic(m.filePath, content, 0644); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func ReadFile(path string) (content []byte, err error) {
	f, err := os.Open(path)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	}
	defer f.Close()
	content, err = ioutil.ReadAll(f)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return content, nil
}

func WriteFileAtomic(filename string, data []byte, perm os.FileMode) error {
	dir, name := path.Dir(filename), path.Base(filename)
	f, err := ioutil.TempFile(dir, name)
	if err != nil {
		return errors.Trace(err)
	}
	n, err := f.Write(data)
	f.Close()
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	} else {
		err = os.Chmod(f.Name(), perm)
	}
	if err != nil {
		os.Remove(f.Name())
		return errors.Trace(err)
	}
	return os.Rename(f.Name(), filename)
}
