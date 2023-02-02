package es_new

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/handler/es_new/elastic"
	"log"
	"reflect"
	"strings"
	"time"
)

const (
	fieldTypeList            = "list"
	defaultBulkFlushInterval = time.Millisecond * 200
	defaultBulkSize          = 4000
)

type posSaver struct {
	pos     mysql.Position
	gtidset mysql.GTIDSet
	force   bool
}

type eventHandler struct {
	r *River
}

var _ canal.EventHandler = (*eventHandler)(nil)

func newEventHandler(r *River) *eventHandler {
	return &eventHandler{
		r: r,
	}
}

func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	return h.r.ctx.Err()
}

func (h *eventHandler) OnTableChanged(schema string, table string) error {
	err := h.r.updateRule(schema, table)
	if err != nil && err != ErrRuleNotExist {
		return errors.Trace(err)
	}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	return h.r.ctx.Err()
}

func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	return h.r.ctx.Err()
}

func (h *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	return h.r.ctx.Err()
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	h.r.syncCh <- posSaver{pos, set, force}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	rules, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}

	var allreqs []*elastic.BulkRequest
	for _, rule := range rules {
		var reqs []*elastic.BulkRequest
		var err error
		switch e.Action {
		case canal.InsertAction:
			reqs, err = h.r.makeInsertRequest(rule, e.Rows)
		case canal.DeleteAction:
			reqs, err = h.r.makeDeleteRequest(rule, e.Rows)
		case canal.UpdateAction:
			reqs, err = h.r.makeUpdateRequest(rule, e.Rows)
		default:
			log.Printf("[err] invalid rows action %s", e.Action)
			continue
		}
		if err != nil {
			log.Printf("[err] make %s ES request err %v, close sync", e.Action, err)
			continue
		}
		allreqs = append(allreqs, reqs...)
	}

	h.r.syncCh <- allreqs

	return h.r.ctx.Err()
}

func (h *eventHandler) String() string {
	return "ESRiverEventHandler"
}

func (r *River) syncLoop() {
	bulkSize := defaultBulkSize
	interval := defaultBulkFlushInterval

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer r.wg.Done()

	lastSavedTime := time.Now()
	reqs := make([]*elastic.BulkRequest, 0, 1024)

	var pos mysql.Position
	var gtidset string

	for {
		needFlush := false
		needUpdatePos := false
		needSavePos := false

		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()
				pos = v.pos
				if v.gtidset == nil {
					gtidset = ""
				} else {
					gtidset = v.gtidset.String()
				}
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
				}
				needUpdatePos = true
			case []*elastic.BulkRequest:
				reqs = append(reqs, v...)
				needFlush = len(reqs) >= bulkSize
			}
		case <-ticker.C:
			needFlush = true
		case <-r.ctx.Done():
			return
		}

		if needFlush {
			var bulkErr error
			// retry 10 times
			for i := 0; i < 10; i++ {
				if err := r.doBulk(reqs); err != nil {
					log.Printf("[err] do ES bulk err %v, close sync", err)
					bulkErr = err
					time.Sleep(1 * time.Minute)
					continue
				}
				bulkErr = nil
				break
			}
			if bulkErr != nil {
				r.cancel()
				return
			}
			reqs = reqs[0:0]
		}

		if needUpdatePos {
			var updatePosErr error
			for i := 0; i < 10; i++ {
				if err := r.master.Update(pos, gtidset, needSavePos); err != nil {
					log.Printf("[err] save sync position %s err %v, close sync", pos, err)
					updatePosErr = err
					time.Sleep(1 * time.Minute)
					continue
				}
				updatePosErr = nil
				break
			}
			if updatePosErr != nil {
				r.cancel()
				return
			}
		}
	}
}

// for insert and delete
func (r *River) makeRequest(rule *Rule, action string, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for _, values := range rows {
		id, err := r.getDocID(rule, values)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(id) == 0 {
			continue
		}

		parentID := ""
		if len(rule.Parent) > 0 {
			if parentID, err = r.getParentID(rule, values, rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		mapping := rule.ActionMapping[action]
		req := &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: id, Parent: parentID}
		req.Action = mapping.ESAction

		switch req.Action {
		case elastic.ActionIndex:
			err = r.makeInsertReqData(req, rule, values)
		case elastic.ActionUpdate:
			err = r.makeUpdateReqData(req, rule, mapping, nil, values)
		case elastic.ActionDelete:
			// do nothing
		default:
			err = fmt.Errorf("Elastic bulk action '%s' is not supported", req.Action)
		}
		if err != nil {
			return nil, err
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *River) makeInsertRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.InsertAction, rows)
}

func (r *River) makeDeleteRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	return r.makeRequest(rule, canal.DeleteAction, rows)
}

func (r *River) makeUpdateRequest(rule *Rule, rows [][]interface{}) ([]*elastic.BulkRequest, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	reqs := make([]*elastic.BulkRequest, 0, len(rows))

	for i := 0; i < len(rows); i += 2 {
		afterID, err := r.getDocID(rule, rows[i+1])

		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(afterID) == 0 {
			continue
		}

		afterParentID := ""
		if len(rule.Parent) > 0 {
			if afterParentID, err = r.getParentID(rule, rows[i+1], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		req := &elastic.BulkRequest{Index: rule.Index, Type: rule.Type, ID: afterID, Parent: afterParentID}
		mapping := rule.ActionMapping[canal.UpdateAction]
		req.Action = mapping.ESAction
		switch req.Action {
		case elastic.ActionIndex:
			r.makeInsertReqData(req, rule, rows[i+1])
		case elastic.ActionUpdate:
			r.makeUpdateReqData(req, rule, mapping, rows[i], rows[i+1])
		case elastic.ActionDelete:
			// do nothing
		default:
			return nil, errors.New(fmt.Sprintf("Elastic bulk action '%s' is not supported", req.Action))
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *River) makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Printf("[warn] invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
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
	}

	return value
}

// 支持 MySQL 中的一个字段映射 ES 中多个字段，用 ｜ 分割，eg: title="my_title|es_title,list"
func (r *River) getFieldParts(k string, v string) (string, []string, []string) {
	values := strings.Split(v, "|")
	elastics := make([]string, 0, len(values))
	fieldTypes := make([]string, 0, len(values))
	mysql := k
	mapElastic := make(map[string]struct{}, len(values))
	for _, val := range values {
		composedField := strings.Split(val, ",")

		elastic := composedField[0]
		fieldType := ""

		if 0 == len(elastic) {
			elastic = mysql
		}
		if _, found := mapElastic[elastic]; found {
			continue
		} else {
			mapElastic[elastic] = struct{}{}
		}
		if 2 == len(composedField) {
			fieldType = composedField[1]
		}
		elastics = append(elastics, elastic)
		fieldTypes = append(fieldTypes, fieldType)
	}

	return mysql, elastics, fieldTypes
}

func (r *River) makeInsertReqData(req *elastic.BulkRequest, rule *Rule, values []interface{}) error {
	req.Data = make(map[string]interface{}, len(values))
	req.Action = elastic.ActionIndex

	var jsonColumns []string
	if len(rule.JSONColumns) > 0 {
		jsonColumns = strings.Split(rule.JSONColumns, ",")
	}

	for i, c := range rule.TableInfo.Columns {
		mapped := false
		keys := make([]string, 0)
		dataValues := make([]interface{}, 0)
		value := r.processFilterColumns(rule, c.Name, values[i])

		for k, v := range rule.FieldMapping {
			mysql, elastics, fieldTypes := r.getFieldParts(k, v)

			if mysql == c.Name {
				mapped = true
				for j, e := range elastics {
					keys = append(keys, e)
					fieldType := fieldTypes[j]
					colValue := r.makeReqColumnData(&c, value)
					if fieldType == fieldTypeList {
						if str, ok := colValue.(string); ok {
							colValue = strings.Split(str, ",")
						}
					}
					dataValues = append(dataValues, colValue)
				}
			}
		}
		if !mapped {
			keys = []string{c.Name}
			value = r.makeReqColumnData(&c, value)
			dataValues = []interface{}{value}
		}
		for _, jc := range jsonColumns {
			if jc == c.Name {
				for j, val := range dataValues {
					if str, ok := val.(string); ok && len(str) > 0 {
						if err := json.Unmarshal([]byte(str), &val); err != nil {
							return errors.Trace(err)
						}
					}
					dataValues[j] = val
				}
			}
		}
		for j, key := range keys {
			req.Data[key] = dataValues[j]
		}
	}
	return nil
}

func (r *River) makeUpdateReqData(req *elastic.BulkRequest, rule *Rule,
	mapping *ActionMapping, beforeValues []interface{}, afterValues []interface{}) error {

	// beforeValues could be nil, use afterValues instead
	values := make(map[string]interface{}, len(afterValues))

	// maybe dangerous if something wrong delete before?
	req.Action = elastic.ActionUpdate

	useScript := len(mapping.Script) > 0 || len(mapping.ScriptFile) > 0
	partialUpdate := len(beforeValues) > 0 && !useScript

	var jsonColumns []string
	if len(rule.JSONColumns) > 0 {
		jsonColumns = strings.Split(rule.JSONColumns, ",")
	}

	for i, c := range rule.TableInfo.Columns {
		afterValue := r.processFilterColumns(rule, c.Name, afterValues[i])
		if partialUpdate && reflect.DeepEqual(beforeValues[i], afterValue) {
			// nothing changed
			continue
		}
		mapped := false
		var value interface{}

		keys := make([]string, 0)
		dataValues := make([]interface{}, 0)
		for k, v := range rule.FieldMapping {
			mysql, elastics, fieldTypes := r.getFieldParts(k, v)
			if mysql == c.Name {
				// has custom field mapping
				mapped = true
				for j, e := range elastics {
					keys = append(keys, e)
					fieldType := fieldTypes[j]
					colValue := r.makeReqColumnData(&c, afterValue)
					if fieldType == fieldTypeList {
						if str, ok := colValue.(string); ok {
							colValue = strings.Split(str, ",")
						}
					}
					dataValues = append(dataValues, colValue)
				}
			}
		}
		if !mapped {
			keys = []string{c.Name}
			value = r.makeReqColumnData(&c, afterValue)
			dataValues = []interface{}{value}
		}
		for _, jc := range jsonColumns {
			if jc == c.Name {
				for j, dataValue := range dataValues {
					if str, ok := dataValue.(string); ok && len(str) > 0 {
						if err := json.Unmarshal([]byte(str), &dataValue); err != nil {
							return errors.Trace(err)
						}
					}
					dataValues[j] = dataValue
				}
			}
		}
		for j, key := range keys {
			values[key] = dataValues[j]
		}
	}

	if useScript {
		p := map[string]interface{}{
			"params": map[string]interface{}{
				"object": values,
			},
		}
		if len(mapping.Script) > 0 {
			p["inline"] = mapping.Script
		} else if len(mapping.ScriptFile) > 0 {
			p["file"] = mapping.ScriptFile
		}
		req.Data = map[string]interface{}{
			"script": p,
		}
		if mapping.ScriptedUpsert {
			req.Data["scripted_upsert"] = true
			req.Data["upsert"] = map[string]interface{}{}
		}
	} else {
		req.Data = map[string]interface{}{
			"doc": values,
		}
	}
	return nil
}

// Get primary keys in one row and format them into a string
// PK must not be nil
func (r *River) getDocID(rule *Rule, row []interface{}) (string, error) {
	var keys []interface{}
	if len(rule.IDColumns) > 0 {
		columns := strings.Split(rule.IDColumns, ",")
		keys = make([]interface{}, len(columns))
		for i, column := range columns {
			if pos := rule.TableInfo.FindColumn(column); pos >= 0 {
				keys[i] = row[pos]
			} else {
				return "", errors.Errorf("Could not find id column '%s' in table '%s'", column, rule.Table)
			}
		}
	} else {
		var err error
		keys, err = GetPKValues(rule.TableInfo, row)
		if err != nil {
			return "", err
		}
	}

	var buf bytes.Buffer

	sep := ""
	for i, value := range keys {
		if value == nil {
			return "", errors.Errorf("The %ds PK value is nil", i)
		}

		buf.WriteString(fmt.Sprintf("%s%v", sep, value))
		sep = ":"
	}

	return buf.String(), nil
}

// Get primary keys in one row for a table, a table may use multi fields as the PK
func GetPKValues(table *schema.Table, row []interface{}) ([]interface{}, error) {
	indexes := table.PKColumns
	if len(indexes) == 0 {
		return nil, errors.Errorf("table %s has no PK", table)
	} else if len(table.Columns) != len(row) {
		return nil, errors.Errorf("table %s has %d columns, but row data %v len is %d", table,
			len(table.Columns), row, len(row))
	}

	values := make([]interface{}, 0, len(indexes))

	for _, index := range indexes {
		values = append(values, row[index])
	}

	return values, nil
}

func (r *River) getParentID(rule *Rule, row []interface{}, columnName string) (string, error) {
	index := rule.TableInfo.FindColumn(columnName)
	if index < 0 {
		return "", errors.Errorf("parent id not found %s(%s)", rule.TableInfo.Name, columnName)
	}

	return fmt.Sprint(row[index]), nil
}

func (r *River) doBulk(reqs []*elastic.BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}

	if resp, err := r.es.Bulk(reqs); err != nil {
		log.Printf("[err] sync docs err %v after binlog %s", err, r.canal.SyncedPosition())
		return errors.Trace(err)
	} else if resp.Errors {
		for i := 0; i < len(resp.Items); i++ {
			for action, item := range resp.Items[i] {
				if len(item.Error) > 0 {
					log.Printf("[err] %s index: %s, type: %s, id: %s, status: %d, error: %s",
						action, item.Index, item.Type, item.ID, item.Status, item.Error)
				}
			}
		}
	}

	return nil
}

func (r *River) processFilterColumns(rule *Rule, columnName string, value interface{}) interface{} {
	for _, column := range strings.Split(rule.HtmlStripColumns, ",") {
		if column == columnName {
			newValue, err := HtmlStrip(value)
			if err != nil {
				log.Printf("[err] filter fail, column: %s, error: %v", column, err)
			}
			return newValue
		}
	}
	return value
}
