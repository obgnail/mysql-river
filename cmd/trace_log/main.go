package main

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/config"
	"github.com/obgnail/mysql-river/handler/trace_log"
	"github.com/obgnail/mysql-river/river"
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
	databasesList := config.StringSlice("trace_log_dbs")
	onlyTablesList := config.StringSlice("trace_log_tables")

	for idx, v := range databasesList {
		databasesList[idx] = strings.ToLower(strings.TrimSpace(v))
	}
	for idx, v := range onlyTablesList {
		onlyTablesList[idx] = strings.ToLower(strings.TrimSpace(v))
	}

	handler := trace_log.NewTraceLogHandler(databasesList, onlyTablesList, showAllField, showQuery)
	err := river.NewDefaultRiver(addr, user, password, handler)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
}
