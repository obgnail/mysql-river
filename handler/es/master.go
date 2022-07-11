package es

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
)

type masterInfo struct {
	sync.RWMutex

	Name    string `toml:"bin_name"`
	Pos     uint32 `toml:"bin_pos"`
	GTIDSet string `toml:"gtid_set"`

	filePath     string
	lastSaveTime time.Time
}

func loadMasterInfo(dataDir string) (*masterInfo, error) {
	var m masterInfo

	if len(dataDir) == 0 {
		return &m, nil
	}

	m.filePath = path.Join(dataDir, "master.info")
	m.lastSaveTime = time.Now()

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	f, err := os.Open(m.filePath)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return &m, nil
	}
	defer f.Close()

	_, err = toml.DecodeReader(f, &m)
	return &m, errors.Trace(err)
}

func (m *masterInfo) Update(pos mysql.Position, gset string, forceSave bool) error {
	m.Lock()
	defer m.Unlock()

	m.Name = pos.Name
	m.Pos = pos.Pos
	m.GTIDSet = gset

	if len(m.filePath) == 0 {
		return nil
	}

	if forceSave {
		m.lastSaveTime = time.Now()
	} else {
		now := time.Now()
		if now.Sub(m.lastSaveTime) < time.Second {
			return nil
		}
		m.lastSaveTime = now
	}

	var buf bytes.Buffer
	enc := toml.NewEncoder(&buf)

	if err := enc.Encode(m); err != nil {
		return errors.Trace(err)
	}

	var err error
	if err = WriteFileAtomic(m.filePath, buf.Bytes(), 0644); err != nil {
		log.Printf("[err] canal save master info to file %s err %v", m.filePath, err)
	}

	return errors.Trace(err)
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	pos := mysql.Position{
		Name: m.Name,
		Pos:  m.Pos,
	}
	m.RUnlock()
	return pos
}

func (m *masterInfo) PositionAndGTIDSet() (mysql.Position, string) {
	m.RLock()
	pos := mysql.Position{
		Name: m.Name,
		Pos:  m.Pos,
	}
	gset := m.GTIDSet
	m.RUnlock()
	return pos, gset
}

func (m *masterInfo) Close() error {
	pos, gset := m.PositionAndGTIDSet()
	return m.Update(pos, gset, true)
}

func WriteFileAtomic(filename string, data []byte, perm os.FileMode) error {
	dir, name := path.Split(filename)
	f, err := ioutil.TempFile(dir, name)
	if err != nil {
		return err
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
		return err
	}
	return os.Rename(f.Name(), filename)
}
