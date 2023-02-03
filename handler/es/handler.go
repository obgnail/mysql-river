package es

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/juju/errors"
	"log"
	"reflect"
	"strings"
	"time"
)

const (
	defaultFlushBulkInterval = 200 * time.Millisecond
	defaultSaveInterval      = 3 * time.Second
)

const mysqlDateFormat = "2006-01-02"

var (
	resultNotExistErr = fmt.Errorf("result not exist")
)

type posSaver struct {
	pos   mysql.Position
	force bool
}

type SourceConfig struct {
	Schema string   `json:"schema"`
	Tables []string `json:"tables"`
}

type SyncESConfig struct {
	ESHost        string         `json:"es_host"`
	ESPort        int64          `json:"es_port"`
	ESUser        string         `json:"es_user"`
	ESPassword    string         `json:"es_pass"`
	DataDir       string         `json:"es_data_dir"`
	BulkSize      int            `json:"es_bulk_size"`
	Sources       []SourceConfig `json:"es_source"`
	Rules         []*Rule        `json:"es_rule"`
	FlushBulkTime time.Duration  `json:"es_flush_bulk_time"`
	SkipNoPkTable bool           `json:"es_skip_no_pk_table"`
}

type SyncESHandler struct {
	cfg *SyncESConfig

	canal *canal.Canal

	rules map[string]*Rule // map[Schema.Table]*Rule

	esClient *Client

	masterInfo *MasterInfo // 记录解析到哪里了

	syncChan chan interface{} // posSaver
	exitChan chan struct{}
}

func NewSyncESHandler(cfg *SyncESConfig, canal *canal.Canal) (*SyncESHandler, error) {
	masterInfo, err := LoadMasterInfo(cfg.DataDir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	h := &SyncESHandler{
		cfg:        cfg,
		canal:      canal,
		esClient:   prepareClient(cfg),
		masterInfo: masterInfo,
		syncChan:   make(chan interface{}, 2<<1),
		exitChan:   make(chan struct{}, 1),
	}
	m, err := h.prepareRule(cfg.Rules)
	if err != nil {
		return nil, errors.Trace(err)
	}
	h.rules = m
	return h, nil
}

func (s *SyncESHandler) prepareRule(rules []*Rule) (map[string]*Rule, error) {
	res := make(map[string]*Rule)
	for _, rule := range rules {
		newRule, err := NewRule(rule.Schema, rule.Table, s.canal, s.cfg.SkipNoPkTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		res[RuleKey(rule.Schema, rule.Table)] = newRule
	}
	return res, nil
}

func (s *SyncESHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}
	s.syncChan <- posSaver{pos, true}
	return nil
}

func (s *SyncESHandler) OnTableChanged(schema string, table string) error {
	err := s.updateRule(schema, table)
	if err != nil && err != resultNotExistErr {
		return errors.Trace(err)
	}
	return nil
}

func (s *SyncESHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	s.syncChan <- posSaver{nextPos, true}
	return nil
}

func (s *SyncESHandler) OnXID(nextPos mysql.Position) error {
	s.syncChan <- posSaver{nextPos, false}
	return nil
}

func (s *SyncESHandler) OnGTID(mysql.GTIDSet) error {
	return nil
}

func (s *SyncESHandler) OnPosSynced(mysql.Position, mysql.GTIDSet, bool) error {
	return nil
}

func (s *SyncESHandler) OnRow(e *canal.RowsEvent) error {
	rule, ok := s.rules[RuleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}

	var reqs []*BulkRequest
	var err error
	switch e.Action {
	case canal.InsertAction:
		reqs, err = s.makeInsertRequest(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = s.makeDeleteRequest(rule, e.Rows)
	case canal.UpdateAction:
		reqs, err = s.makeUpdateRequest(rule, e.Rows)
	default:
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		s.exitChan <- struct{}{}
		return errors.Errorf("make %s ES request err %v, close sync", e.Action, err)
	}

	s.syncChan <- reqs
	return nil
}

func (s *SyncESHandler) String() string {
	return "sync es_new"
}

func (s *SyncESHandler) Position() mysql.Position {
	return s.masterInfo.Position()
}

func (s *SyncESHandler) Run() {
	bulkSize := s.cfg.BulkSize
	if bulkSize == 0 {
		bulkSize = 128
	}

	interval := s.cfg.FlushBulkTime
	if interval == 0 {
		interval = defaultFlushBulkInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastSavedTime := time.Now()
	var savePos mysql.Position
	reqs := make([]*BulkRequest, 0, 1024)

	for {
		needFlush := false
		needSavePos := false

		select {
		case <-ticker.C:
			needFlush = true
		case <-s.exitChan:
			return
		case ele := <-s.syncChan:
			switch ele := ele.(type) {
			case posSaver:
				now := time.Now()
				if ele.force || now.Sub(lastSavedTime) > defaultSaveInterval {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					savePos = ele.pos
				}
			case []*BulkRequest:
				reqs = append(reqs, ele...)
				needFlush = len(reqs) >= bulkSize
			case error:
				return
			}
		}

		if needFlush {
			if err := s.doBulk(reqs); err != nil {
				s.exitChan <- struct{}{}
				return
			}
			reqs = reqs[0:0]
		}

		if needSavePos {
			if err := s.masterInfo.Save(savePos); err != nil {
				s.exitChan <- struct{}{}
				return
			}
		}
	}

}

func (s *SyncESHandler) doBulk(reqs []*BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	resp, err := s.esClient.Bulk(reqs)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Code/100 == 2 || resp.Errors {
		for i := 0; i < len(resp.Items); i++ {
			for action, item := range resp.Items[i] {
				if len(item.Error) > 0 {
					log.Printf("%s index: %s, type: %s, id: %s, status: %d, error: %s\n",
						action, item.Index, item.Type, item.ID, item.Status, item.Error)
				}
			}
		}
	}

	return nil
}

func (s *SyncESHandler) updateRule(schema, table string) error {
	rule, ok := s.rules[RuleKey(schema, table)]
	if !ok {
		return resultNotExistErr
	}

	tableInfo, err := s.canal.GetTable(schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	rule.TableInfo = tableInfo

	return nil
}

// for insert and delete
func (s *SyncESHandler) makeRequest(rule *Rule, action string, rows [][]interface{}) ([]*BulkRequest, error) {
	reqs := make([]*BulkRequest, 0, len(rows))

	for _, values := range rows {
		id, err := s.getDocID(rule, values)
		if err != nil {
			return nil, errors.Trace(err)
		}

		parentID := ""
		if len(rule.Parent) > 0 {
			if parentID, err = s.getParentID(rule, values, rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		req := &BulkRequest{Index: rule.Index, ID: id, Parent: parentID, Pipeline: rule.Pipeline}

		if action == canal.DeleteAction {
			req.Action = ActionDelete
		} else {
			s.makeInsertReqData(req, rule, values)
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}
func (s *SyncESHandler) makeInsertReqData(req *BulkRequest, rule *Rule, values []interface{}) {
	req.Data = make(map[string]interface{}, len(values))
	req.Action = ActionIndex

	for i, c := range rule.TableInfo.Columns {
		if !rule.CheckFilter(c.Name) {
			continue
		}
		mapped := false
		for k, v := range rule.FieldMapping {
			mysql, elastic, fieldType := s.getFieldParts(k, v)
			if mysql == c.Name {
				mapped = true
				req.Data[elastic] = s.getFieldValue(&c, fieldType, values[i])
			}
		}
		if mapped == false {
			req.Data[c.Name] = s.makeReqColumnData(&c, values[i])
		}
	}
}
func (s *SyncESHandler) makeInsertRequest(rule *Rule, rows [][]interface{}) ([]*BulkRequest, error) {
	return s.makeRequest(rule, canal.InsertAction, rows)
}

func (s *SyncESHandler) makeDeleteRequest(rule *Rule, rows [][]interface{}) ([]*BulkRequest, error) {
	return s.makeRequest(rule, canal.DeleteAction, rows)
}

func (s *SyncESHandler) makeUpdateRequest(rule *Rule, rows [][]interface{}) ([]*BulkRequest, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	reqs := make([]*BulkRequest, 0, len(rows))

	for i := 0; i < len(rows); i += 2 {
		beforeID, err := s.getDocID(rule, rows[i])
		if err != nil {
			return nil, errors.Trace(err)
		}

		afterID, err := s.getDocID(rule, rows[i+1])

		if err != nil {
			return nil, errors.Trace(err)
		}

		beforeParentID, afterParentID := "", ""
		if len(rule.Parent) > 0 {
			if beforeParentID, err = s.getParentID(rule, rows[i], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
			if afterParentID, err = s.getParentID(rule, rows[i+1], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		req := &BulkRequest{Index: rule.Index, ID: beforeID, Parent: beforeParentID}

		if beforeID != afterID || beforeParentID != afterParentID {
			req.Action = ActionDelete
			reqs = append(reqs, req)

			req = &BulkRequest{Index: rule.Index, ID: afterID, Parent: afterParentID, Pipeline: rule.Pipeline}
			s.makeInsertReqData(req, rule, rows[i+1])
		} else {
			if len(rule.Pipeline) > 0 {
				// Pipelines can only be specified on index action
				s.makeInsertReqData(req, rule, rows[i+1])
				// Make sure action is index, not create
				req.Action = ActionIndex
				req.Pipeline = rule.Pipeline
			}
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (s *SyncESHandler) getParentID(rule *Rule, row []interface{}, columnName string) (string, error) {
	index := rule.TableInfo.FindColumn(columnName)
	if index < 0 {
		return "", errors.Errorf("parent id not found %s(%s)", rule.TableInfo.Name, columnName)
	}

	return fmt.Sprint(row[index]), nil
}

func (s *SyncESHandler) getFieldParts(k string, v string) (string, string, string) {
	composedField := strings.Split(v, ",")

	mysql := k
	elastic := composedField[0]
	fieldType := ""

	if 0 == len(elastic) {
		elastic = mysql
	}
	if 2 == len(composedField) {
		fieldType = composedField[1]
	}

	return mysql, elastic, fieldType
}

func (s *SyncESHandler) makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Printf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, err := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(time.RFC3339)
		}
	case schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			vt, err := time.Parse(mysqlDateFormat, string(v))
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(mysqlDateFormat)
		}
	}

	return value
}

// If id in toml file is none, get primary keys in one row and format them into a string, and PK must not be nil
// Else get the ID's column in one row and format them into a string
func (s *SyncESHandler) getDocID(rule *Rule, row []interface{}) (string, error) {
	var (
		ids []interface{}
		err error
	)
	if rule.ID == nil {
		ids, err = rule.TableInfo.GetPKValues(row)
		if err != nil {
			return "", err
		}
	} else {
		ids = make([]interface{}, 0, len(rule.ID))
		for _, column := range rule.ID {
			value, err := rule.TableInfo.GetColumnValue(column, row)
			if err != nil {
				return "", err
			}
			ids = append(ids, value)
		}
	}

	var buf bytes.Buffer

	sep := ""
	for i, value := range ids {
		if value == nil {
			return "", errors.Errorf("The %ds id or PK value is nil", i)
		}

		buf.WriteString(fmt.Sprintf("%s%v", sep, value))
		sep = ":"
	}

	return buf.String(), nil
}

// get mysql field value and convert it to specific value to es_new
func (s *SyncESHandler) getFieldValue(col *schema.TableColumn, fieldType string, value interface{}) interface{} {
	var fieldValue interface{}
	switch fieldType {
	case "list":
		v := s.makeReqColumnData(col, value)
		if str, ok := v.(string); ok {
			fieldValue = strings.Split(str, ",")
		} else {
			fieldValue = v
		}

	case "date":
		if col.Type == schema.TYPE_NUMBER {
			col.Type = schema.TYPE_DATETIME

			v := reflect.ValueOf(value)
			switch v.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fieldValue = s.makeReqColumnData(col, time.Unix(v.Int(), 0).Format(mysql.TimeFormat))
			}
		}
	}

	if fieldValue == nil {
		fieldValue = s.makeReqColumnData(col, value)
	}
	return fieldValue
}

func prepareClient(cfg *SyncESConfig) *Client {
	return NewClient(&ClientConfig{
		Addr:     fmt.Sprintf("%s:%d", cfg.ESHost, cfg.ESPort),
		User:     cfg.ESUser,
		Password: cfg.ESPassword,
	})
}
