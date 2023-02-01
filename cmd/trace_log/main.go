package main

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/config"
	"github.com/obgnail/mysql-river/handler/trace_log"
	"log"
	"strings"
)

func main() {
	host := config.String("mysql_host", "127.0.0.1")
	port, _ := config.Int64("mysql_port", 3306)
	user := config.String("mysql_user", "root")
	password := config.String("mysql_pass", "root")
	addr := fmt.Sprintf("%s:%d", host, port)

	showAllField := config.Bool("trace_log_show_all_field", false)
	showQuery := config.Bool("trace_log_show_query_message", false)
	dbs := config.StringSlice("trace_log_dbs")

	for idx, v := range dbs {
		dbs[idx] = strings.ToLower(strings.TrimSpace(v))
	}
	err := trace_log.RunTraceLogRiver(addr, user, password, dbs, showAllField, showQuery)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
}
