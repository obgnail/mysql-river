package es_new

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
)

func (s *riverTestSuite) setupHealth(c *C) (r *River) {
	schema := `
        CREATE TABLE IF NOT EXISTS %s (
            id INT,
            title VARCHAR(256),
            pid INT,
            PRIMARY KEY(id)) ENGINE=INNODB;
    `

	s.testExecute(s.cHealthTest, c, "DROP TABLE IF EXISTS test_river_health")
	s.testExecute(s.cHealthTest, c, fmt.Sprintf(schema, "test_river_health"))

	cfg := new(Config)
	cfg.MyAddr = *myAddrHealthTest
	cfg.MyUser = myUser
	cfg.MyPassword = myPassword
	cfg.ESAddr = *esAddr

	cfg.ServerID = 1002
	cfg.Flavor = "mysql"

	cfg.DataDir = "/tmp/test_river_health"
	cfg.DumpExec = "mysqldump"

	cfg.StatAddr = "127.0.0.1:12701"

	os.RemoveAll(cfg.DataDir)

	cfg.HealthCheckInterval = 1

	cfg.Sources = []SourceConfig{SourceConfig{Schema: myDatabaseTest, Tables: []string{"test_river_health"}}}

	cfg.Rules = []*Rule{
		&Rule{Schema: myDatabaseTest,
			Table:  "test_river_health",
			Index:  "river",
			Type:   "river_health",
			Parent: "pid"}}

	cfg.HealthCheckEnable = true

	r, err := NewRiver(cfg)
	c.Assert(err, IsNil)

	mapping := map[string]interface{}{
		"river_health": map[string]interface{}{
			"_parent": map[string]string{"type": "river_health_parent"},
		},
	}

	r.es.CreateMapping("river", "river_health", mapping)

	return r
}

func (s *riverTestSuite) testPrepareHealth(c *C) {
	for i := 1; i < 4; i++ {
		s.testExecute(s.cHealthTest, c, "INSERT INTO test_river_health (id, title, pid) VALUES (?, ?, ?)", i, fmt.Sprintf("num-%d", i), i)
	}
}

func (s *riverTestSuite) testElasticHealthExists(c *C, id string, parent string, exist bool) {
	index := "river"
	docType := "river_health"

	reqUrl := fmt.Sprintf("http://%s/%s/%s/%s?parent=%s", s.r.es.Addr,
		url.QueryEscape(index),
		url.QueryEscape(docType),
		url.QueryEscape(id),
		url.QueryEscape(parent))

	r, err := s.r.es.Do("HEAD", reqUrl, nil)
	c.Assert(err, IsNil)

	if exist {
		c.Assert(r.Code, Equals, http.StatusOK)
	} else {
		c.Assert(r.Code, Equals, http.StatusNotFound)
	}
}

func (s *riverTestSuite) TestRiverWithHealth(c *C) {
	river := s.setupHealth(c)

	defer river.Close()

	s.testPrepareHealth(c)

	go river.Run()

	<-river.canal.WaitDumpDone()

	time.Sleep(6) // wait for health loop

	time.Sleep(time.Second * 1)

	c.Assert(river.health.prevHealthStatus, Equals, healthStatusGreen)

	s.testElasticHealthExists(c, "1", "1", true)

	s.testExecute(s.cHealthTest, c, "DELETE FROM test_river_health WHERE id = ?", 1)
	err := river.canal.CatchMasterPos(1)
	c.Assert(err, IsNil)

	time.Sleep(time.Second * 1)

	s.testElasticHealthExists(c, "1", "1", false)

	cmd := exec.Command("docker", "stop", "mysql-health-test")
	err = cmd.Run()
	c.Assert(err, IsNil)

	time.Sleep(time.Second)
	c.Assert(river.health.prevHealthStatus, Equals, healthStatusRed)

	cmd = exec.Command("docker", "start", "mysql-health-test")
	err = cmd.Run()
	c.Assert(err, IsNil)
	time.Sleep(time.Second * 1)

	conn, err := client.Connect(*myAddrHealthTest, myUser, myPassword, myDatabaseTest)
	c.Assert(err, IsNil)
	defer conn.Close()

	s.testExecute(conn, c, "INSERT INTO test_river_health (id, title, pid) VALUES (?, ?, ?)", 1000, fmt.Sprintf("num-%d", 1000), 1000)

	time.Sleep(time.Second * 2)

	c.Assert(river.health.prevHealthStatus, Equals, healthStatusGreen)
}
