package river

import (
	"bytes"
	"fmt"
	"github.com/BurntSushi/toml"
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
	fileName = "master.info"

	saveMinDuration = time.Second
)

var (
	emptyDirErr  = fmt.Errorf("data dir is empty")
	emptyPathErr = fmt.Errorf("file path is empty")
)

type masterInfo struct {
	sync.RWMutex        // protect below
	Name         string `toml:"bin_name"`
	Pos          uint32 `toml:"bin_pos"`
	filePath     string
	lastSaveTime time.Time
}

func loadMasterInfo(dataDir string) (*masterInfo, error) {
	var m masterInfo
	if len(dataDir) == 0 {
		return &m, emptyDirErr
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	m.filePath = path.Join(dataDir, fileName)
	m.lastSaveTime = time.Now()

	f, err := os.Open(m.filePath)
	defer f.Close()
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return &m, nil
	}

	_, err = toml.NewDecoder(f).Decode(&m)
	return &m, errors.Trace(err)
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	pos := mysql.Position{Name: m.Name, Pos: m.Pos}
	m.RUnlock()
	return pos
}

func (m *masterInfo) Save(name string, pos uint32) error {
	m.Lock()
	defer m.Unlock()

	if m.filePath == "" {
		return emptyPathErr
	}
	if m.Name == name && m.Pos == pos {
		return nil
	}

	n := time.Now()
	// 保存的最小时间间隔
	if n.Sub(m.lastSaveTime) < saveMinDuration {
		return nil
	}
	m.lastSaveTime = n
	m.Name = name
	m.Pos = pos

	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(m); err != nil {
		return errors.Trace(err)
	}
	if err := WriteFileAtomic(m.filePath, buf.Bytes(), 0644); err != nil {
		return errors.Trace(err)
	}
	return nil
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
