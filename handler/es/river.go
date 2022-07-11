package es

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/juju/errors"

	"github.com/obgnail/mysql-river/handler/es/elastic"
	"github.com/go-mysql-org/go-mysql/mysql"
)

var ErrRuleNotExist = errors.New("rule is not exist")

// In Elasticsearch, river is a pluggable service within Elasticsearch pulling data then indexing it into Elasticsearch.
// We use this definition here too, although it may not run within Elasticsearch.
// Maybe later I can implement a acutal river in Elasticsearch, but I must learn java. :-)
type River struct {
	c *Config

	canal *canal.Canal

	rules map[string][]*Rule

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	es *elastic.Client

	master *masterInfo

	health *healthInfo

	syncCh chan interface{}
}

func NewRiver(c *Config) (*River, error) {
	r := new(River)

	r.c = c
	r.rules = make(map[string][]*Rule)
	r.syncCh = make(chan interface{}, 4096)
	r.ctx, r.cancel = context.WithCancel(context.Background())

	var err error
	if r.master, err = loadMasterInfo(c.DataDir); err != nil {
		return nil, errors.Trace(err)
	}

	if r.health, err = newHealthInfo(
		c.DataDir,
		c.HealthCheckOuputDir,
		c.HealthCheckInterval,
		c.HealthCheckPosThreshold,
	); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareRule(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// We must use binlog full row image
	if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}

	r.es = elastic.NewClient(r.c.ESAddr)

	return r, nil
}

func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = r.c.MyAddr
	cfg.User = r.c.MyUser
	cfg.Password = r.c.MyPassword
	cfg.Charset = r.c.MyCharset
	cfg.Flavor = r.c.Flavor

	cfg.ServerID = r.c.ServerID
	cfg.Dump.ExecutionPath = r.c.DumpExec
	cfg.Dump.DiscardErr = false

	var err error
	r.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

func (r *River) prepareCanal() error {
	var db string
	dbs := map[string]struct{}{}

	// TODO: Considering multiple databases and wildcard tables
	source := r.c.Sources[0]
	db = source.Schema
	dbs[source.Schema] = struct{}{}
	tables := source.Tables
	// tables := make([]string, 0, len(r.rules))
	// for _, array := range r.rules {
	// 	rule := array[0]
	// 	db = rule.Schema
	// 	dbs[rule.Schema] = struct{}{}
	// 	tables = append(tables, rule.Table)
	// }

	if len(dbs) == 1 {
		// one db, we can shrink using table
		r.canal.AddDumpTables(db, tables...)
	} else {
		// many dbs, can only assign databases to dump
		keys := make([]string, 0, len(dbs))
		for key, _ := range dbs {
			keys = append(keys, key)
		}

		r.canal.AddDumpDatabases(keys...)
	}
	h := newEventHandler(r)
	r.canal.SetEventHandler(h)

	return nil
}

func (r *River) newRule(schema, table string) error {
	key := ruleKey(schema, table)
	rule := newDefaultRule(schema, table)
	r.rules[key] = append(r.rules[key], rule)
	return nil
}

func (r *River) updateRule(schema, table string) error {
	rules, ok := r.rules[ruleKey(schema, table)]
	if !ok {
		return ErrRuleNotExist
	}

	tableInfo, err := r.canal.GetTable(schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	for _, rule := range rules {
		rule.TableInfo = tableInfo
	}

	return nil
}

func (r *River) parseSource() (map[string][]string, error) {
	wildTables := make(map[string][]string, len(r.c.Sources))

	// first, check sources
	for _, s := range r.c.Sources {
		for _, table := range s.Tables {
			if len(s.Schema) == 0 {
				return nil, errors.Errorf("empty schema not allowed for source")
			}

			if regexp.QuoteMeta(table) != table {
				if _, ok := wildTables[ruleKey(s.Schema, table)]; ok {
					return nil, errors.Errorf("duplicate wildcard table defined for %s.%s", s.Schema, table)
				}

				tables := []string{}

				sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
                    table_name RLIKE "%s" AND table_schema = "%s";`, table, s.Schema)

				res, err := r.canal.Execute(sql)
				if err != nil {
					return nil, errors.Trace(err)
				}

				for i := 0; i < res.Resultset.RowNumber(); i++ {
					f, _ := res.GetString(i, 0)
					err := r.newRule(s.Schema, f)
					if err != nil {
						return nil, errors.Trace(err)
					}

					tables = append(tables, f)
				}

				wildTables[ruleKey(s.Schema, table)] = tables
			} else {
				err := r.newRule(s.Schema, table)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
	}

	if len(r.rules) == 0 {
		return nil, errors.Errorf("no source data defined")
	}

	return wildTables, nil
}

func (r *River) prepareRule() error {
	wildtables, err := r.parseSource()
	if err != nil {
		return errors.Trace(err)
	}

	if r.c.Rules != nil {
		// then, set custom mapping rule
		for _, rule := range r.c.Rules {
			if len(rule.Schema) == 0 {
				return errors.Errorf("empty schema not allowed for rule")
			}

			if regexp.QuoteMeta(rule.Table) != rule.Table {
				//wildcard table
				tables, ok := wildtables[ruleKey(rule.Schema, rule.Table)]
				if !ok {
					return errors.Errorf("wildcard table for %s.%s is not defined in source", rule.Schema, rule.Table)
				}

				if len(rule.Index) == 0 {
					return errors.Errorf("wildcard table rule %s.%s must have a index, can not empty", rule.Schema, rule.Table)
				}

				rule.prepare()

				for _, table := range tables {
					for _, rr := range r.rules[ruleKey(rule.Schema, table)] {
						rr.Index = rule.Index
						rr.Type = rule.Type
						rr.Parent = rule.Parent
						rr.FieldMapping = rule.FieldMapping
					}
				}
			} else {
				key := ruleKey(rule.Schema, rule.Table)
				if _, ok := r.rules[key]; !ok {
					return errors.Errorf("rule %s, %s not defined in source", rule.Schema, rule.Table)
				}
				rule.prepare()
				r.rules[key] = append(r.rules[key], rule)
			}
		}
	}

	for key, array := range r.rules {
		if len(array) > 1 {
			// custom mapping rule defined, remove default rule appended in parseSource()
			array = array[1:]
			r.rules[key] = array
		}
		for _, rule := range array {
			if rule.TableInfo, err = r.canal.GetTable(rule.Schema, rule.Table); err != nil {
				return errors.Trace(err)
			}

			// table must have a PK for one column, multi columns may be supported later.
			if len(rule.IDColumns) == 0 && len(rule.TableInfo.PKColumns) != 1 {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}
		}
	}

	return nil
}

func ruleKey(schema string, table string) string {
	return fmt.Sprintf("%s:%s", schema, table)
}

func (r *River) Run() error {
	r.wg.Add(1)
	go r.syncLoop()
	if r.c.HealthCheckEnable {
		go r.healthLoop()
	}

	pos, gtidset := r.master.PositionAndGTIDSet()
	if len(gtidset) == 0 {
		if err := r.canal.RunFrom(pos); err != nil {
			log.Printf("[err] start canal from position '%v' err: %v", pos, err)
			return errors.Trace(err)
		}
	} else {
		gset, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, gtidset)
		if err != nil {
			log.Printf("[err] invalid GTID set '%s': %v", gtidset, err)
			return errors.Trace(err)
		}
		if err = r.canal.StartFromGTID(gset); err != nil {
			log.Printf("[err] start canal from GTID set '%s' err: %v", gtidset, err)
			return errors.Trace(err)
		}
	}

	return nil
}

// Ctx returns the internal context for outside use.
func (r *River) Ctx() context.Context {
	return r.ctx
}

// Close closes the River
func (r *River) Close() {
	log.Println("closing river")

	r.cancel()

	r.canal.Close()

	r.master.Close()

	r.wg.Wait()
}
