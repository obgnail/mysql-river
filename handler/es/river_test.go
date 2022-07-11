package es

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/obgnail/mysql-river/handler/es/elastic"
	. "gopkg.in/check.v1"
)

var myAddr = flag.String("my_addr", "127.0.0.1:3306", "MySQL addr for normal test")
var esAddr = flag.String("es_addr", "127.0.0.1:9200", "Elasticsearch addr")

var myUser = "root"
var myPassword = "root"
var myDatabaseTest = "test"

var myAddrHealthTest = flag.String("my_addr_health_test", "127.0.0.1:3307", "MySQL addr for health test")

func Test(t *testing.T) {
	TestingT(t)
}

type riverTestSuite struct {
	cNormalTest *client.Conn
	cHealthTest *client.Conn
	r           *River
}

var _ = Suite(&riverTestSuite{})

func (s *riverTestSuite) SetUpSuite(c *C) {
	cmd := exec.Command("docker", "start", "mysql-health-test")
	err := cmd.Run()
	c.Assert(err, IsNil)

	s.cNormalTest, err = client.Connect(*myAddr, myUser, myPassword, myDatabaseTest)
	c.Assert(err, IsNil)
	s.cHealthTest, err = client.Connect(*myAddrHealthTest, myUser, myPassword, myDatabaseTest)
	c.Assert(err, IsNil)

	s.testExecute(s.cNormalTest, c, "SET SESSION binlog_format = 'ROW'")

	schema := `
        CREATE TABLE IF NOT EXISTS %s (
            id INT,
            title VARCHAR(256),
            content VARCHAR(256),
            mylist VARCHAR(256),
            tenum ENUM("e1", "e2", "e3"),
            tset SET("a", "b", "c"),
            tbit INT(1) default 1,
            PRIMARY KEY(id)) ENGINE=INNODB;
    `

	s.testExecute(s.cNormalTest, c, "DROP TABLE IF EXISTS test_river")
	s.testExecute(s.cNormalTest, c, fmt.Sprintf(schema, "test_river"))

	for i := 0; i < 3; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(s.cNormalTest, c, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		s.testExecute(s.cNormalTest, c, fmt.Sprintf(schema, table))
	}

	cfg := new(Config)
	cfg.MyAddr = *myAddr
	cfg.MyUser = myUser
	cfg.MyPassword = myPassword
	cfg.ESAddr = *esAddr

	cfg.ServerID = 1002
	cfg.Flavor = "mysql"

	cfg.DataDir = "/tmp/test_river"
	cfg.DumpExec = "mysqldump"

	cfg.StatAddr = "127.0.0.1:12600"

	os.RemoveAll(cfg.DataDir)

	cfg.Sources = []SourceConfig{SourceConfig{Schema: myDatabaseTest, Tables: []string{"test_river",
		"test_river_0000", "test_river_0001", "test_river_0002",
	}}}

	cfg.Rules = []*Rule{
		&Rule{Schema: myDatabaseTest,
			Table:        "test_river",
			Index:        "river",
			Type:         "river",
			FieldMapping: map[string]string{"title": "es_title|,list|es_title2|es_title3,list", "mylist": "es_mylist,list"},
		},
	}

	for i := 0; i < 3; i++ {
		cfg.Rules = append(cfg.Rules, &Rule{Schema: myDatabaseTest,
			Table:        fmt.Sprintf("test_river_%04d", i),
			Index:        "river",
			Type:         "river",
			FieldMapping: map[string]string{"title": "es_title", "mylist": "es_mylist,list"},
		})
	}

	s.r, err = NewRiver(cfg)
	c.Assert(err, IsNil)

	err = s.r.es.DeleteIndex("river")
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) TearDownSuite(c *C) {
	if s.cNormalTest != nil {
		s.cNormalTest.Close()
	}

	if s.cHealthTest != nil {
		s.cHealthTest.Close()
	}

	if s.r != nil {
		s.r.Close()
	}

	if s.r != nil && s.r.es != nil {
		err := s.r.es.DeleteIndex("river")
		c.Assert(err, IsNil)
	}
}

func (s *riverTestSuite) TestConfig(c *C) {
	str := `
my_addr = "%s"
my_user = "%s"
my_pass = "%s"

es_addr = "%s"

data_dir = "/tmp/test_river"

[[source]]
schema = "test"

# tables = ["test_river", "test_river_[0-9]{4}"]
tables = ["test_river", "test_river_0000", "test_river_0001", "test_river_0002"]

[[rule]]
schema = "test"
table = "test_river"
index = "river"
type = "river"
id_columns = "id"

    [rule.field]
    title = "es_title"
    mylist = "es_mylist,list"

[[rule]]
schema = "test"
table = "test_river_0000"
index = "river"
type = "river"

    [rule.field]
    title = "es_title"
    mylist = "es_mylist,list"

[[rule]]
schema = "test"
table = "test_river_0001"
index = "river"
type = "river"

	[rule.field]
	title = "es_title"
	mylist = "es_mylist,list"

[[rule]]
schema = "test"
table = "test_river_0002"
index = "river"
type = "river"

	[rule.field]
	title = "es_title"
	mylist = "es_mylist,list"
`

	str = fmt.Sprintf(str, myAddr, myUser, myPassword, esAddr)

	cfg, err := NewConfig(str)
	c.Assert(err, IsNil)
	c.Assert(cfg.Sources, HasLen, 1)
	c.Assert(cfg.Sources[0].Tables, HasLen, 4)
	c.Assert(cfg.Rules, HasLen, 4)
}

func (s *riverTestSuite) testExecute(client *client.Conn, c *C, query string, args ...interface{}) {
	_, err := client.Execute(query, args...)
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) testPrepareData(c *C) {
	s.testExecute(s.cNormalTest, c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 1, "first", "hello go 1", "e1", "a,b")
	s.testExecute(s.cNormalTest, c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 2, "second", "hello mysql 2", "e2", "b,c")
	s.testExecute(s.cNormalTest, c, "INSERT INTO test_river (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", 3, "third", "hello elaticsearch 3", "e3", "c")
	s.testExecute(s.cNormalTest, c, "INSERT INTO test_river (id, title, content, tenum, tset, tbit) VALUES (?, ?, ?, ?, ?, ?)", 4, "fouth", "hello go-mysql-elasticserach 4", "e1", "a,b,c", 0)

	for i := 0; i < 3; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(s.cNormalTest, c, fmt.Sprintf("INSERT INTO %s (id, title, content, tenum, tset) VALUES (?, ?, ?, ?, ?)", table), 5+i, "abc", "hello", "e1", "a,b,c")
	}
}

func (s *riverTestSuite) testElasticGet(c *C, id string) *elastic.Response {
	index := "river"
	docType := "river"

	r, err := s.r.es.Get(index, docType, id)
	c.Assert(err, IsNil)

	return r
}

func (s *riverTestSuite) testWaitSyncDone(c *C) {
	err := s.r.canal.CatchMasterPos(10)
	c.Assert(err, IsNil)
}

func (s *riverTestSuite) TestRiver(c *C) {
	s.testPrepareData(c)

	go s.r.Run()

	<-s.r.canal.WaitDumpDone()

	time.Sleep(time.Second * 1) // wait for river sync

	var r *elastic.Response
	r = s.testElasticGet(c, "1")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["tenum"], Equals, "e1")
	c.Assert(r.Source["tset"], Equals, "a,b")
	c.Assert(r.Source["title"], NotNil)
	c.Assert(r.Source["es_title2"], NotNil)
	c.Assert(r.Source["es_title3"], NotNil)
	c.Assert(r.Source["title"], DeepEquals, []interface{}{r.Source["es_title"]})
	c.Assert(r.Source["es_title"], Equals, r.Source["es_title2"])
	c.Assert(r.Source["es_title3"], DeepEquals, []interface{}{r.Source["es_title"]})

	r = s.testElasticGet(c, "100")
	c.Assert(r.Found, Equals, false)

	for i := 0; i < 3; i++ {
		r = s.testElasticGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(r.Source["es_title"], Equals, "abc")
	}

	s.testExecute(s.cNormalTest, c, "UPDATE test_river SET title = ?, tenum = ?, tset = ?, mylist = ? WHERE id = ?", "second 2", "e3", "a,b,c", "a,b,c", 2)
	s.testExecute(s.cNormalTest, c, "DELETE FROM test_river WHERE id = ?", 1)
	s.testExecute(s.cNormalTest, c, "UPDATE test_river SET title = ?, id = ? WHERE id = ?", "second 30", 3, 3)

	// so we can insert invalid data
	s.testExecute(s.cNormalTest, c, `SET SESSION sql_mode="NO_ENGINE_SUBSTITUTION";`)

	// bad insert
	s.testExecute(s.cNormalTest, c, "UPDATE test_river SET title = ?, tenum = ?, tset = ? WHERE id = ?", "second 2", "e5", "a,b,c,d", 4)

	for i := 0; i < 3; i++ {
		table := fmt.Sprintf("test_river_%04d", i)
		s.testExecute(s.cNormalTest, c, fmt.Sprintf("UPDATE %s SET title = ? WHERE id = ?", table), "hello", 5+i)
	}

	s.testWaitSyncDone(c)

	time.Sleep(time.Second * 1) // wait for river sync

	r = s.testElasticGet(c, "1")
	c.Assert(r.Found, Equals, false)

	r = s.testElasticGet(c, "2")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["es_title"], Equals, "second 2")
	c.Assert(r.Source["es_title2"], Equals, "second 2")
	c.Assert(r.Source["title"], DeepEquals, []interface{}{"second 2"})
	c.Assert(r.Source["es_title3"], DeepEquals, []interface{}{"second 2"})
	c.Assert(r.Source["tenum"], Equals, "e3")
	c.Assert(r.Source["tset"], Equals, "a,b,c")
	c.Assert(r.Source["es_mylist"], DeepEquals, []interface{}{"a", "b", "c"})
	c.Assert(r.Source["tbit"], Equals, float64(1))

	r = s.testElasticGet(c, "4")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["tenum"], Equals, "")
	c.Assert(r.Source["tset"], Equals, "a,b,c")
	c.Assert(r.Source["tbit"], Equals, float64(0))

	r = s.testElasticGet(c, "3")
	c.Assert(r.Found, Equals, true)
	c.Assert(r.Source["es_title"], Equals, "second 30")

	r = s.testElasticGet(c, "30")
	c.Assert(r.Found, Equals, false)

	for i := 0; i < 3; i++ {
		r = s.testElasticGet(c, fmt.Sprintf("%d", 5+i))
		c.Assert(r.Found, Equals, true)
		c.Assert(r.Source["es_title"], Equals, "hello")
	}
}
